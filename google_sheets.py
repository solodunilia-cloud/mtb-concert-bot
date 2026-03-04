#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Sheets Manager — MTB Concerts Bot
Исправления v2:
  - Credentials из переменной окружения GOOGLE_CREDENTIALS_JSON (JSON-строка)
  - Убран циклический импорт from bot import ...
  - get_all_concerts_fn: callback для получения всех концертов (фикс бага с календарём)
  - Добавлена колонка Яндекс Музыка (L)
  - Slug вычисляется на лету из artist + date
"""

import os
import re
import json
import logging
import calendar
from datetime import datetime
from typing import Optional, Dict, List, Callable

logger = logging.getLogger(__name__)

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False
    logger.warning("gspread не установлен. Google Sheets отключены.")

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

# ─── ЦВЕТА (RGB 0-1) ─────────────────────────────────────────────────────────

C_BLACK      = {'red': 0.00, 'green': 0.00, 'blue': 0.00}
C_DARKGRAY   = {'red': 0.26, 'green': 0.26, 'blue': 0.26}
C_HEADER     = {'red': 0.26, 'green': 0.26, 'blue': 0.26}
C_TITLE_BG   = {'red': 0.00, 'green': 0.00, 'blue': 0.00}
C_WHITE      = {'red': 1.00, 'green': 1.00, 'blue': 1.00}
C_LIGHT      = {'red': 0.95, 'green': 0.95, 'blue': 0.95}
C_YELLOW     = {'red': 0.96, 'green': 0.80, 'blue': 0.60}
C_GREEN_TEXT = {'red': 0.40, 'green': 0.86, 'blue': 0.50}
C_RED_TEXT   = {'red': 0.96, 'green': 0.33, 'blue': 0.33}

C_CAL_GREEN  = {'red': 0.20, 'green': 0.66, 'blue': 0.33}
C_CAL_ORANGE = {'red': 0.98, 'green': 0.74, 'blue': 0.02}
C_CAL_RED    = {'red': 0.96, 'green': 0.33, 'blue': 0.33}
C_CAL_DATE   = {'red': 0.95, 'green': 0.95, 'blue': 0.95}

WEEKDAYS_RU = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
MONTHS_RU   = [
    '', 'Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
    'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'
]

# Заголовки листа "Данные" — 12 колонок (A–L)
DATA_HEADERS = [
    'Сайт', 'Дата', 'Время', 'Страничка', 'Артист',
    'Покупка билета', 'Картинка', 'Текст', 'Яндекс Музыка',
    'Афиша', 'Статус', 'ID'
]

# ─── ВСПОМОГАТЕЛЬНОЕ ─────────────────────────────────────────────────────────

def _status_color_cal(c: Dict) -> Dict:
    filled = sum([
        c.get('poster_status') == 'approved',
        bool(c.get('tickets_url')),
        bool(c.get('description_text')),
        bool(c.get('date')),
    ])
    if filled == 4: return C_CAL_GREEN
    if filled >= 2: return C_CAL_ORANGE
    return C_CAL_RED

def _status_text(c: Dict) -> str:
    missing = []
    if c.get('poster_status') != 'approved': missing.append('афиша')
    if not c.get('tickets_url'):             missing.append('билеты')
    if not c.get('description_text'):        missing.append('текст')
    return '✅ Готово' if not missing else '❌ ' + ', '.join(missing)

def _col_letter(n: int) -> str:
    result = ''
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result

def _make_page_slug(artist: str) -> str:
    """
    Маша и Медведи → mashaimedvedi
    Depeche Mode   → depechemode
    Slug для URL страницы (без даты, без дефисов).
    """
    table = {
        'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e',
        'ж':'zh','з':'z','и':'i','й':'y','к':'k','л':'l','м':'m',
        'н':'n','о':'o','п':'p','р':'r','с':'s','т':'t','у':'u',
        'ф':'f','х':'kh','ц':'ts','ч':'ch','ш':'sh','щ':'sch',
        'ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya',
    }
    result = ''
    for ch in artist.lower():
        if ch in table:
            result += table[ch]
        elif ch.isascii() and ch.isalpha():
            result += ch
        # пробелы и прочие символы — просто пропускаем
    return result


# ─── МЕНЕДЖЕР ────────────────────────────────────────────────────────────────

class GoogleSheetsManager:
    def __init__(
        self,
        spreadsheet_id: Optional[str] = None,
        get_all_concerts_fn: Optional[Callable[[], List[Dict]]] = None,
    ):
        """
        get_all_concerts_fn — callback для получения всех концертов из БД.
        Передаётся из bot.py чтобы избежать циклического импорта.
        Нужен для корректной пересборки календаря.
        """
        self.spreadsheet_id       = spreadsheet_id
        self.client               = None
        self.spreadsheet          = None
        self._get_all_concerts_fn = get_all_concerts_fn  # ✅ фикс бага с календарём

        if not GSPREAD_AVAILABLE:
            return
        if not spreadsheet_id:
            logger.info("GOOGLE_SHEETS_ID не задан — Sheets отключены")
            return

        try:
            creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
            if creds_json:
                creds_info = json.loads(creds_json)
                creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
            else:
                creds_file = os.getenv('GOOGLE_CREDENTIALS_FILE', 'credentials.json')
                creds = Credentials.from_service_account_file(creds_file, scopes=SCOPES)

            self.client      = gspread.authorize(creds)
            self.spreadsheet = self.client.open_by_key(spreadsheet_id)
            logger.info("✅ Google Sheets подключён")
        except Exception as e:
            logger.error(f"Google Sheets init error: {e}")

    def _is_connected(self) -> bool:
        return self.client is not None and self.spreadsheet is not None

    # ── ЛИСТ "ДАННЫЕ" ────────────────────────────────────────────────────────

    def _get_or_create_data_sheet(self):
        try:
            return self.spreadsheet.worksheet('Данные')
        except Exception:
            ws = self.spreadsheet.add_worksheet('Данные', rows=500, cols=12)
            ws.update('A1:L1', [DATA_HEADERS])
            ws.format('A1:L1', {
                'backgroundColor': C_HEADER,
                'textFormat': {
                    'bold': True,
                    'foregroundColor': C_WHITE,
                    'fontSize': 10,
                },
                'horizontalAlignment': 'CENTER',
            })
            try:
                self.spreadsheet.batch_update({'requests': [{
                    'updateSheetProperties': {
                        'properties': {
                            'sheetId': ws.id,
                            'gridProperties': {'frozenRowCount': 1}
                        },
                        'fields': 'gridProperties.frozenRowCount'
                    }
                }]})
            except Exception:
                pass
            return ws

    def sync_concert(self, concert: Dict):
        """Обновляет строку в листе Данные + пересобирает календарь месяца."""
        if not self._is_connected():
            return
        try:
            self._sync_data_row(concert)
            if concert.get('date'):
                self._rebuild_calendar_for_concert(concert)
        except Exception as e:
            logger.error(f"sync_concert error: {e}")

    def _sync_data_row(self, concert: Dict):
        ws = self._get_or_create_data_sheet()
        all_values = ws.get_all_values()
        cid = str(concert.get('id', ''))

        # Ищем строку по ID (последняя колонка — L = 12)
        row_idx = None
        for i, row in enumerate(all_values[1:], start=2):
            if len(row) >= 12 and row[11] == cid:
                row_idx = i
                break

        date_str   = concert.get('date', '') or ''
        time_str   = concert.get('time', '') or ''
        # ✅ Slug вычисляем из артиста (без даты — короткий URL для Tilda)
        page_slug  = _make_page_slug(concert.get('artist', ''))
        yandex_url = concert.get('yandex_music_url', '') or ''

        row_data = [[
            '✅' if concert.get('status') != 'cancelled' else '🚫',  # A Сайт
            date_str,                                                  # B Дата
            time_str,                                                  # C Время
            f"https://mtbarmoscow.com/{page_slug}" if page_slug else '',  # D Страничка
            concert.get('artist', ''),                                 # E Артист
            concert.get('tickets_url', '') or '',                      # F Покупка билета
            concert.get('poster_file_id', '') or '',                   # G Картинка
            (concert.get('description_text', '') or '')[:200],         # H Текст
            yandex_url,                                                # I Яндекс Музыка ✅ новое
            '✅' if concert.get('poster_status') == 'approved' else '❌',  # J Афиша
            _status_text(concert),                                     # K Статус
            cid,                                                       # L ID
        ]]

        if row_idx:
            ws.update(f'A{row_idx}:L{row_idx}', row_data)
            bg = C_BLACK if row_idx % 2 == 0 else C_DARKGRAY
        else:
            ws.append_row(row_data[0])
            row_idx = len(ws.get_all_values())
            bg = C_BLACK if row_idx % 2 == 0 else C_DARKGRAY

        # Базовый стиль строки
        ws.format(f'A{row_idx}:L{row_idx}', {
            'backgroundColor': bg,
            'textFormat': {'foregroundColor': C_LIGHT, 'fontSize': 9},
            'verticalAlignment': 'MIDDLE',
        })

        # Ссылки жёлтым (D Страничка, F Билеты, G Картинка, I Яндекс Музыка)
        for col in ['D', 'F', 'G', 'I']:
            ws.format(f'{col}{row_idx}', {
                'backgroundColor': bg,
                'textFormat': {'foregroundColor': C_YELLOW, 'fontSize': 9},
            })

        # Статус — цветной текст
        status_ok = (concert.get('poster_status') == 'approved' and
                     concert.get('tickets_url') and concert.get('description_text'))
        ws.format(f'K{row_idx}', {
            'backgroundColor': bg,
            'textFormat': {
                'foregroundColor': C_GREEN_TEXT if status_ok else C_RED_TEXT,
                'fontSize': 9,
            },
        })

    # ── CALENDAR ─────────────────────────────────────────────────────────────

    def _get_or_create_calendar_sheet(self, month: int, year: int):
        sheet_name = f"{MONTHS_RU[month]} {year}"
        try:
            return self.spreadsheet.worksheet(sheet_name)
        except Exception:
            return self.spreadsheet.add_worksheet(sheet_name, rows=50, cols=7)

    def _rebuild_calendar_for_concert(self, concert: Dict):
        try:
            dt = datetime.strptime(concert['date'], '%d.%m.%Y')
        except Exception:
            return

        # ✅ ФИКС: берём все концерты через callback вместо пустого списка
        all_concerts: List[Dict] = []
        if self._get_all_concerts_fn:
            try:
                all_concerts = self._get_all_concerts_fn()
            except Exception as e:
                logger.error(f"get_all_concerts_fn error: {e}")

        self.rebuild_month_calendar(dt.month, dt.year, all_concerts)

    def rebuild_month_calendar(self, month: int, year: int, all_concerts: List[Dict] = None):
        """
        Перестраивает лист-календарь.
        all_concerts передаётся снаружи чтобы избежать циклического импорта.
        Если не передан, используется callback _get_all_concerts_fn.
        """
        if not self._is_connected():
            return
        try:
            if all_concerts is None and self._get_all_concerts_fn:
                all_concerts = self._get_all_concerts_fn()
            ws = self._get_or_create_calendar_sheet(month, year)
            ws.clear()
            self._draw_calendar(ws, month, year, all_concerts or [])
        except Exception as e:
            logger.error(f"rebuild_month_calendar error: {e}")

    def _draw_calendar(self, ws, month: int, year: int, all_concerts: List[Dict]):
        sheet_name = f"{MONTHS_RU[month]} {year}"

        concerts_by_day: Dict[int, List[Dict]] = {}
        for c in all_concerts:
            if not c.get('date'):
                continue
            try:
                dt = datetime.strptime(c['date'], '%d.%m.%Y')
                if dt.month == month and dt.year == year:
                    concerts_by_day.setdefault(dt.day, []).append(c)
            except Exception:
                pass

        # Строка 1 — заголовок месяца
        ws.merge_cells('A1:G1')
        ws.update('A1', [[f"АФИША МЕРОПРИЯТИЙ — {MONTHS_RU[month].upper()} {year}"]])
        ws.format('A1:G1', {
            'backgroundColor': C_TITLE_BG,
            'textFormat': {
                'bold': True,
                'fontSize': 16,
                'foregroundColor': C_YELLOW,
            },
            'horizontalAlignment': 'CENTER',
            'verticalAlignment': 'MIDDLE',
        })

        # Строка 2 — дни недели
        ws.update('A2:G2', [WEEKDAYS_RU])
        ws.format('A2:G2', {
            'backgroundColor': C_HEADER,
            'textFormat': {'bold': True, 'foregroundColor': C_WHITE, 'fontSize': 11},
            'horizontalAlignment': 'CENTER',
        })

        cal        = calendar.monthcalendar(year, month)
        batch      = []
        current_row = 3

        for week in cal:
            row_days  = []
            row_info1 = []
            row_info2 = []
            row_info3 = []

            for day in week:
                if day == 0:
                    row_days.append('')
                    row_info1.append('')
                    row_info2.append('')
                    row_info3.append('')
                else:
                    row_days.append(str(day))
                    day_cs = concerts_by_day.get(day, [])
                    info   = ['', '', '']
                    for i, c in enumerate(day_cs[:3]):
                        t = f" {c['time']}" if c.get('time') else ''
                        info[i] = f"{c.get('artist','')}{t}\n{_status_text(c)}"
                    row_info1.append(info[0])
                    row_info2.append(info[1])
                    row_info3.append(info[2])

            r = current_row
            batch += [
                {'range': f'A{r}:G{r}',     'values': [row_days]},
                {'range': f'A{r+1}:G{r+1}', 'values': [row_info1]},
                {'range': f'A{r+2}:G{r+2}', 'values': [row_info2]},
                {'range': f'A{r+3}:G{r+3}', 'values': [row_info3]},
            ]
            current_row += 4

        ws.batch_update(batch)

        sheet_id  = ws.id
        requests  = []
        current_row = 3

        for week in cal:
            for col_idx, day in enumerate(week):
                if day != 0:
                    day_cs = concerts_by_day.get(day, [])

                    requests.append({'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': current_row - 1,
                            'endRowIndex': current_row,
                            'startColumnIndex': col_idx,
                            'endColumnIndex': col_idx + 1,
                        },
                        'cell': {'userEnteredFormat': {
                            'backgroundColor': C_CAL_DATE,
                            'textFormat': {'bold': True, 'fontSize': 11},
                            'horizontalAlignment': 'LEFT',
                            'verticalAlignment': 'MIDDLE',
                        }},
                        'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)',
                    }})

                    for i, c in enumerate(day_cs[:3]):
                        requests.append({'repeatCell': {
                            'range': {
                                'sheetId': sheet_id,
                                'startRowIndex': current_row + i,
                                'endRowIndex': current_row + i + 1,
                                'startColumnIndex': col_idx,
                                'endColumnIndex': col_idx + 1,
                            },
                            'cell': {'userEnteredFormat': {
                                'backgroundColor': _status_color_cal(c),
                                'textFormat': {'fontSize': 9, 'foregroundColor': C_BLACK},
                                'wrapStrategy': 'WRAP',
                                'verticalAlignment': 'TOP',
                            }},
                            'fields': 'userEnteredFormat(backgroundColor,textFormat,wrapStrategy,verticalAlignment)',
                        }})

            current_row += 4

        # Высоты строк
        r = 3
        for _ in cal:
            requests += [
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': r-1, 'endIndex': r},   'properties': {'pixelSize': 22}, 'fields': 'pixelSize'}},
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': r,   'endIndex': r+1}, 'properties': {'pixelSize': 55}, 'fields': 'pixelSize'}},
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': r+1, 'endIndex': r+2}, 'properties': {'pixelSize': 55}, 'fields': 'pixelSize'}},
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': r+2, 'endIndex': r+3}, 'properties': {'pixelSize': 55}, 'fields': 'pixelSize'}},
            ]
            r += 4

        # Ширина колонок
        requests.append({'updateDimensionProperties': {
            'range': {'sheetId': sheet_id, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': 7},
            'properties': {'pixelSize': 170},
            'fields': 'pixelSize',
        }})

        # Строка 1 — высота заголовка
        requests.append({'updateDimensionProperties': {
            'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': 1},
            'properties': {'pixelSize': 45},
            'fields': 'pixelSize',
        }})

        if requests:
            self.spreadsheet.batch_update({'requests': requests})

        logger.info(f"✅ Календарь '{sheet_name}' обновлён ({len(all_concerts)} концертов)")

    def rebuild_all_calendars(self, all_concerts: List[Dict]):
        """Пересобирает все календари. Концерты передаются снаружи."""
        if not self._is_connected():
            return
        try:
            months = set()
            for c in all_concerts:
                if c.get('date'):
                    try:
                        dt = datetime.strptime(c['date'], '%d.%m.%Y')
                        months.add((dt.month, dt.year))
                    except Exception:
                        pass
            for month, year in months:
                month_concerts = [
                    c for c in all_concerts
                    if c.get('date') and datetime.strptime(c['date'], '%d.%m.%Y').month == month
                ]
                self.rebuild_month_calendar(month, year, month_concerts)
        except Exception as e:
            logger.error(f"rebuild_all_calendars error: {e}")
