#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Sheets Manager — MTB Concerts Bot
Исправления:
  - Credentials из переменной окружения GOOGLE_CREDENTIALS_JSON (JSON-строка)
  - Убран циклический импорт from bot import ...
  - Стиль таблицы как в оригинальном xlsx (тёмный фон, жёлтые ссылки)
"""

import os
import json
import logging
import calendar
from datetime import datetime
from typing import Optional, Dict, List

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

# ─── ЦВЕТА (точные из xlsx MT_BAR_EVENTS) ───────────────────────────────────
# Данные: чётные строки #424242, нечётные #000000
C_ROW_ODD    = {'red': 0.259, 'green': 0.259, 'blue': 0.259}  # #424242 — нечётные
C_ROW_EVEN   = {'red': 0.027, 'green': 0.027, 'blue': 0.027}  # #070707 — чётные
C_HEADER     = {'red': 0.259, 'green': 0.259, 'blue': 0.259}  # #424242 — заголовок
C_TITLE_BG   = {'red': 0.027, 'green': 0.027, 'blue': 0.027}  # #070707 — заголовок месяца
C_WHITE      = {'red': 1.000, 'green': 1.000, 'blue': 1.000}  # #FFFFFF
C_LIGHT      = {'red': 0.949, 'green': 0.949, 'blue': 0.949}  # #F2F2F2 — обычный текст
C_YELLOW     = {'red': 0.957, 'green': 0.800, 'blue': 0.600}  # #F4CC99 — ссылки
C_GREEN_TEXT = {'red': 0.400, 'green': 0.859, 'blue': 0.498}  # #66DB7F — статус готово
C_RED_TEXT   = {'red': 0.957, 'green': 0.329, 'blue': 0.329}  # #F45454 — статус не готово

# Календарь: числа дня
C_CAL_DATE   = {'red': 0.949, 'green': 0.949, 'blue': 0.949}  # #F2F2F2
C_CAL_TEXT   = {'red': 0.027, 'green': 0.027, 'blue': 0.027}  # текст на дате #070707
C_CAL_CELL   = {'red': 0.027, 'green': 0.027, 'blue': 0.027}  # #070707 — фон ячеек

# Статус концерта (цвет текста в ячейке календаря)
C_CAL_GREEN  = {'red': 0.400, 'green': 0.859, 'blue': 0.498}  # готово
C_CAL_ORANGE = {'red': 0.957, 'green': 0.800, 'blue': 0.600}  # частично
C_CAL_RED    = {'red': 0.957, 'green': 0.329, 'blue': 0.329}  # мало

# Обратная совместимость
C_BLACK    = C_ROW_EVEN
C_DARKGRAY = C_ROW_ODD

WEEKDAYS_RU = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
MONTHS_RU   = [
    '', 'Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
    'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'
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

def _rgb(hex_str: str) -> Dict:
    """'FF434343' → {red, green, blue}"""
    h = hex_str.lstrip('#')
    if len(h) == 8: h = h[2:]  # убрать alpha
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return {'red': r/255, 'green': g/255, 'blue': b/255}

# ─── МЕНЕДЖЕР ────────────────────────────────────────────────────────────────

class GoogleSheetsManager:
    def __init__(self, spreadsheet_id: Optional[str] = None):
        self.spreadsheet_id = spreadsheet_id
        self.client         = None
        self.spreadsheet    = None

        if not GSPREAD_AVAILABLE:
            return
        if not spreadsheet_id:
            logger.info("GOOGLE_SHEETS_ID не задан — Sheets отключены")
            return

        try:
            # ✅ Берём credentials из переменной окружения, не из файла
            creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
            if creds_json:
                creds_info = json.loads(creds_json)
                creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
            else:
                # Фолбэк на файл если есть
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
            ws = self.spreadsheet.add_worksheet('Данные', rows=500, cols=11)
            headers = [['Сайт', 'Дата', 'Время', 'Страничка', 'Артист',
                        'Покупка билета', 'Картинка', 'Текст', 'Афиша', 'Статус', 'ID']]
            ws.update('A1:K1', headers)
            # Стиль заголовка — как в оригинале: тёмно-серый фон, белый жирный
            ws.format('A1:K1', {
                'backgroundColor': C_HEADER,
                'textFormat': {
                    'bold': True,
                    'foregroundColor': C_WHITE,
                    'fontSize': 10,
                },
                'horizontalAlignment': 'CENTER',
            })
            # Заморозить первую строку
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

        # Ищем строку по ID (последняя колонка)
        row_idx = None
        for i, row in enumerate(all_values[1:], start=2):
            if len(row) >= 11 and row[10] == cid:
                row_idx = i
                break

        date_str = concert.get('date', '') or ''
        time_str = concert.get('time', '') or ''
        slug     = concert.get('slug', '')

        row_data = [[
            '✅' if concert.get('status') != 'cancelled' else '🚫',
            date_str,
            time_str,
            f"https://mtbarmoscow.com/{slug}" if slug else '',
            concert.get('artist', ''),
            concert.get('tickets_url', '') or '',
            concert.get('poster_file_id', '') or '',
            (concert.get('description_text', '') or '')[:200],
            '✅' if concert.get('poster_status') == 'approved' else '❌',
            _status_text(concert),
            cid,
        ]]

        # Чередование цветов строк
        if row_idx:
            ws.update(f'A{row_idx}:K{row_idx}', row_data)
            bg = C_BLACK if row_idx % 2 == 0 else C_DARKGRAY
        else:
            ws.append_row(row_data[0])
            row_idx = len(ws.get_all_values())
            bg = C_BLACK if row_idx % 2 == 0 else C_DARKGRAY

        # Чередование: нечётные строки = #424242, чётные = #000000
        # (строка 1 — заголовок, строка 2 = первая данных = нечётная)
        bg = C_ROW_ODD if row_idx % 2 == 0 else C_ROW_EVEN

        # Базовый стиль строки — через batchUpdate для точного контроля
        sheet_id = ws.id
        requests = []

        # Весь ряд: фон + обычный текст
        requests.append({'repeatCell': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': row_idx - 1, 'endRowIndex': row_idx,
                'startColumnIndex': 0, 'endColumnIndex': 11,
            },
            'cell': {'userEnteredFormat': {
                'backgroundColor': bg,
                'textFormat': {'foregroundColor': C_LIGHT, 'fontSize': 9},
                'verticalAlignment': 'TOP',
                'wrapStrategy': 'WRAP',
            }},
            'fields': 'userEnteredFormat(backgroundColor,textFormat,verticalAlignment,wrapStrategy)',
        }})

        # Ссылки жёлтым (D=3, F=5, G=6 — 0-based)
        for col_idx in [3, 5, 6]:
            requests.append({'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': row_idx - 1, 'endRowIndex': row_idx,
                    'startColumnIndex': col_idx, 'endColumnIndex': col_idx + 1,
                },
                'cell': {'userEnteredFormat': {
                    'backgroundColor': bg,
                    'textFormat': {'foregroundColor': C_YELLOW, 'fontSize': 9},
                }},
                'fields': 'userEnteredFormat(backgroundColor,textFormat)',
            }})

        # Статус (J=9) — зелёный или красный
        status_ok = (concert.get('poster_status') == 'approved' and
                     concert.get('tickets_url') and concert.get('description_text'))
        requests.append({'repeatCell': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': row_idx - 1, 'endRowIndex': row_idx,
                'startColumnIndex': 9, 'endColumnIndex': 10,
            },
            'cell': {'userEnteredFormat': {
                'backgroundColor': bg,
                'textFormat': {
                    'foregroundColor': C_GREEN_TEXT if status_ok else C_RED_TEXT,
                    'fontSize': 9,
                },
            }},
            'fields': 'userEnteredFormat(backgroundColor,textFormat)',
        }})

        # Высота строки — 49.5px как в оригинале (≈66 пикселей API = 49.5pt)
        requests.append({'updateDimensionProperties': {
            'range': {
                'sheetId': sheet_id,
                'dimension': 'ROWS',
                'startIndex': row_idx - 1,
                'endIndex': row_idx,
            },
            'properties': {'pixelSize': 66},
            'fields': 'pixelSize',
        }})

        if requests:
            self.spreadsheet.batch_update({'requests': requests})

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
        self.rebuild_month_calendar(dt.month, dt.year)

    def rebuild_month_calendar(self, month: int, year: int, all_concerts: List[Dict] = None):
        """
        Перестраивает лист-календарь.
        all_concerts передаётся снаружи чтобы избежать циклического импорта.
        """
        if not self._is_connected():
            return
        try:
            ws = self._get_or_create_calendar_sheet(month, year)
            ws.clear()
            self._draw_calendar(ws, month, year, all_concerts or [])
        except Exception as e:
            logger.error(f"rebuild_month_calendar error: {e}")

    def _draw_calendar(self, ws, month: int, year: int, all_concerts: List[Dict]):
        sheet_name = f"{MONTHS_RU[month]} {year}"

        # Концерты по дням этого месяца
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

        # Строка 1 — заголовок месяца (bg=#000000, fg=#F4CC99, bold, 16px)
        ws.merge_cells('A1:G1')
        ws.update('A1', [[f"АФИША МЕРОПРИЯТИЙ — {MONTHS_RU[month].upper()} {year}"]])
        ws.format('A1:G1', {
            'backgroundColor': C_TITLE_BG,
            'textFormat': {'bold': True, 'fontSize': 16, 'foregroundColor': C_YELLOW},
            'horizontalAlignment': 'CENTER',
            'verticalAlignment': 'MIDDLE',
        })

        # Строка 2 — дни недели (bg=#424242, fg=#FFFFFF, bold, 11px)
        ws.update('A2:G2', [WEEKDAYS_RU])
        ws.format('A2:G2', {
            'backgroundColor': C_HEADER,
            'textFormat': {'bold': True, 'foregroundColor': C_WHITE, 'fontSize': 11},
            'horizontalAlignment': 'CENTER',
        })

        # Сетка дней
        cal        = calendar.monthcalendar(year, month)
        batch      = []
        current_row = 3

        for week in cal:
            row_days  = [''] * 7
            row_info1 = [''] * 7
            row_info2 = [''] * 7
            row_info3 = [''] * 7

            for day_idx, day in enumerate(week):
                if day == 0:
                    pass  # оставляем пустые строки
                else:
                    row_days[day_idx] = str(day)
                    day_cs = concerts_by_day.get(day, [])
                    info   = ['', '', '']
                    for i, c in enumerate(day_cs[:3]):
                        t = f" {c['time']}" if c.get('time') else ''
                        info[i] = f"{c.get('artist','')}{t}\n{_status_text(c)}"
                    row_info1[day_idx] = info[0]
                    row_info2[day_idx] = info[1]
                    row_info3[day_idx] = info[2]

            r = current_row
            batch += [
                {'range': f'A{r}:G{r}',     'values': [row_days]},
                {'range': f'A{r+1}:G{r+1}', 'values': [row_info1]},
                {'range': f'A{r+2}:G{r+2}', 'values': [row_info2]},
                {'range': f'A{r+3}:G{r+3}', 'values': [row_info3]},
            ]
            current_row += 4

        ws.batch_update(batch)

        # Форматирование через Sheets API
        sheet_id  = ws.id
        requests  = []
        current_row = 3

        for week in cal:
            for col_idx, day in enumerate(week):
                if day == 0:
                    current_row_unused = None
                else:
                    day_cs = concerts_by_day.get(day, [])

                    # Номер дня
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

                    # Ячейки концертов
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

        # Высоты строк (точно как в xlsx)
        # row 1 заголовок = 33.75pt ≈ 45px
        # число дня = 16.5pt ≈ 22px
        # строки концертов = 41.25pt ≈ 55px
        requests.append({'updateDimensionProperties': {
            'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': 1},
            'properties': {'pixelSize': 45}, 'fields': 'pixelSize',
        }})
        r = 3
        for _ in cal:
            requests += [
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': r-1, 'endIndex': r},   'properties': {'pixelSize': 22}, 'fields': 'pixelSize'}},
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': r,   'endIndex': r+1}, 'properties': {'pixelSize': 55}, 'fields': 'pixelSize'}},
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': r+1, 'endIndex': r+2}, 'properties': {'pixelSize': 55}, 'fields': 'pixelSize'}},
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': r+2, 'endIndex': r+3}, 'properties': {'pixelSize': 55}, 'fields': 'pixelSize'}},
            ]
            r += 4

        # Ширина колонок — 21.38 символов ≈ 160px как в xlsx
        requests.append({'updateDimensionProperties': {
            'range': {'sheetId': sheet_id, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': 7},
            'properties': {'pixelSize': 160}, 'fields': 'pixelSize',
        }})

        # Фон всех пустых ячеек = #000000
        requests.append({'repeatCell': {
            'range': {'sheetId': sheet_id, 'startRowIndex': 2, 'endRowIndex': 2 + len(cal) * 4,
                      'startColumnIndex': 0, 'endColumnIndex': 7},
            'cell': {'userEnteredFormat': {
                'backgroundColor': C_CAL_CELL,
                'textFormat': {'foregroundColor': C_LIGHT, 'fontSize': 9},
            }},
            'fields': 'userEnteredFormat(backgroundColor,textFormat)',
        }})

        if requests:
            self.spreadsheet.batch_update({'requests': requests})

        logger.info(f"✅ Календарь '{sheet_name}' обновлён")

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

    # ── МЕТОДЫ КОТОРЫЕ НУЖНЫ bot.py (in-memory режим) ───────────────────────

    def is_connected(self) -> bool:
        return self._is_connected()

    def next_id(self, concerts: list) -> int:
        """Следующий ID — максимальный существующий + 1."""
        if not concerts:
            return 1
        return max((c.get('id', 0) for c in concerts), default=0) + 1

    def load_all_concerts(self) -> list:
        """
        Загружает все концерты из листа 'Данные'.
        Возвращает список dict совместимых с bot.py.
        """
        if not self._is_connected():
            logger.warning("Sheets не подключён — стартуем с пустым списком")
            return []
        try:
            ws   = self._get_or_create_data_sheet()
            rows = ws.get_all_values()
            if len(rows) <= 1:
                return []

            concerts = []
            for i, row in enumerate(rows[1:], start=2):
                # Колонки: Сайт, Дата, Время, Страничка, Артист, Билеты, Картинка, Текст, Афиша, Статус, ID
                while len(row) < 11:
                    row.append('')
                cid = row[10].strip()
                if not cid.isdigit():
                    continue
                concerts.append({
                    'id':               int(cid),
                    'artist':           row[4].strip().rstrip(' —').strip(),
                    'date':             row[1].strip() or None,
                    'time':             row[2].strip() or None,
                    'poster_status':    'approved' if row[8].strip() == '✅' else 'none',
                    'poster_file_id':   row[6].strip() or None,
                    'tickets_url':      row[5].strip() or None,
                    'description_text': row[7].strip() or None,
                    'status':           row[9].strip() if row[9].strip() in ('draft','published','cancelled','archived') else 'draft',
                    '_row':             i,
                })
            logger.info(f"✅ Загружено концертов из Sheets: {len(concerts)}")
            return concerts
        except Exception as e:
            logger.error(f"load_all_concerts error: {e}")
            return []

    def load_chats(self) -> list:
        """Загружает зарегистрированные chat_id из листа 'Чаты'."""
        if not self._is_connected():
            return []
        try:
            try:
                ws = self.spreadsheet.worksheet('Чаты')
            except Exception:
                ws = self.spreadsheet.add_worksheet('Чаты', rows=100, cols=1)
                ws.update('A1', [['chat_id']])
                return []
            rows = ws.get_all_values()
            return [int(r[0]) for r in rows[1:] if r and r[0].lstrip('-').isdigit()]
        except Exception as e:
            logger.error(f"load_chats error: {e}")
            return []

    def save_chat(self, chat_id: int, all_chats: list):
        """Добавляет chat_id в лист 'Чаты' если его там нет."""
        if not self._is_connected():
            return
        try:
            try:
                ws = self.spreadsheet.worksheet('Чаты')
            except Exception:
                ws = self.spreadsheet.add_worksheet('Чаты', rows=100, cols=1)
                ws.update('A1', [['chat_id']])
            existing = [r[0] for r in ws.get_all_values()[1:] if r]
            if str(chat_id) not in existing:
                ws.append_row([chat_id])
        except Exception as e:
            logger.error(f"save_chat error: {e}")

    def sync_concert(self, concert: dict, all_concerts: list = None):
        """
        Обновляет строку концерта в листе 'Данные' + пересобирает календарь.
        all_concerts используется для календаря (чтобы показать все события).
        """
        if not self._is_connected():
            return
        try:
            self._sync_data_row(concert)
            if concert.get('date') and all_concerts is not None:
                self._rebuild_calendar_for_concert_with_list(concert, all_concerts)
            elif concert.get('date'):
                self._rebuild_calendar_for_concert(concert)
        except Exception as e:
            logger.error(f"sync_concert error: {e}")

    def _rebuild_calendar_for_concert_with_list(self, concert: dict, all_concerts: list):
        try:
            dt = datetime.strptime(concert['date'], '%d.%m.%Y')
        except Exception:
            return
        self.rebuild_month_calendar(dt.month, dt.year, all_concerts)

    def delete_concert(self, concert: dict, all_concerts: list):
        """
        Помечает концерт как 'archived' в листе 'Данные' (не удаляет строку).
        """
        if not self._is_connected():
            return
        try:
            ws      = self._get_or_create_data_sheet()
            cid     = str(concert.get('id', ''))
            all_val = ws.get_all_values()
            for i, row in enumerate(all_val[1:], start=2):
                if len(row) >= 11 and row[10] == cid:
                    # Статус в колонку J (индекс 9)
                    ws.update(f'J{i}', [['archived']])
                    ws.format(f'A{i}:K{i}', {
                        'textFormat': {'strikethrough': True, 'foregroundColor': C_DARKGRAY},
                    })
                    break
        except Exception as e:
            logger.error(f"delete_concert error: {e}")
