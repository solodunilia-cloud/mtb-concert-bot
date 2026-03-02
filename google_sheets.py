#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Sheets Manager — MTB Concerts Bot
Collector Mode v2

Структура таблицы:
  Лист "Данные"    — строки с концертами (для Dataview/фильтрации)
  Лист "Март 2026" — визуальный календарь месяца (автосоздаётся)

Календарь:
  - Пн-Вс по колонкам
  - Каждая ячейка даты: число + имя артиста + статус
  - Цвет ячейки: зелёный / оранжевый / красный
"""

import os
import logging
import calendar
from datetime import datetime
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

# Попытка импорта gspread
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

CREDENTIALS_FILE = os.getenv('GOOGLE_CREDENTIALS_FILE', 'credentials.json')

# Цвета для статусов (RGB 0-1)
COLOR_GREEN  = {'red': 0.20, 'green': 0.66, 'blue': 0.33}   # 🟢 всё есть
COLOR_ORANGE = {'red': 0.98, 'green': 0.74, 'blue': 0.02}   # 🟠 частично
COLOR_RED    = {'red': 0.96, 'green': 0.33, 'blue': 0.33}   # 🔴 мало
COLOR_WHITE  = {'red': 1.0,  'green': 1.0,  'blue': 1.0}
COLOR_HEADER = {'red': 0.13, 'green': 0.13, 'blue': 0.13}   # почти чёрный
COLOR_DATE_BG= {'red': 0.95, 'green': 0.95, 'blue': 0.95}   # светло-серый фон числа

WEEKDAYS_RU = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
MONTHS_RU   = [
    '', 'Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
    'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'
]


def _concert_status_color(concert: Dict) -> Dict:
    """Возвращает цвет по статусу концерта."""
    has_poster  = concert.get('poster_status') == 'approved'
    has_tickets = bool(concert.get('tickets_url'))
    has_desc    = bool(concert.get('description_text'))
    has_date    = bool(concert.get('date'))

    filled = sum([has_poster, has_tickets, has_desc, has_date])

    if filled == 4:
        return COLOR_GREEN
    elif filled >= 2:
        return COLOR_ORANGE
    else:
        return COLOR_RED


def _concert_status_text(concert: Dict) -> str:
    """Короткая строка статуса для ячейки."""
    parts = []
    if concert.get('poster_status') != 'approved': parts.append('афиша')
    if not concert.get('tickets_url'):              parts.append('билеты')
    if not concert.get('description_text'):         parts.append('текст')

    if not parts:
        return '✅ Готово'
    return '❌ Нет: ' + ', '.join(parts)


def _col_letter(n: int) -> str:
    """Номер колонки → буква (1=A, 2=B, ...)"""
    result = ''
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


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
            creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
            self.client      = gspread.authorize(creds)
            self.spreadsheet = self.client.open_by_key(spreadsheet_id)
            logger.info("Google Sheets подключён")
        except Exception as e:
            logger.error(f"Google Sheets init error: {e}")

    def _is_connected(self) -> bool:
        return self.client is not None and self.spreadsheet is not None

    # ==================== ЛИСТ "ДАННЫЕ" ====================

    def _get_or_create_data_sheet(self):
        """Получает или создаёт лист 'Данные'."""
        try:
            return self.spreadsheet.worksheet('Данные')
        except gspread.WorksheetNotFound:
            ws = self.spreadsheet.add_worksheet('Данные', rows=500, cols=10)
            # Заголовки
            headers = ['Дата', 'Артист', 'Город', 'Афиша', 'Билеты', 'Текст', 'Статус', 'ID']
            ws.update('A1:H1', [headers])
            ws.format('A1:H1', {
                'backgroundColor': COLOR_HEADER,
                'textFormat': {'bold': True, 'foregroundColor': COLOR_WHITE},
                'horizontalAlignment': 'CENTER',
            })
            return ws

    def sync_concert(self, concert: Dict):
        """Обновляет строку концерта в листе 'Данные' и пересоздаёт календарь."""
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

        # Ищем существующую строку по ID
        row_idx = None
        for i, row in enumerate(all_values[1:], start=2):
            if len(row) >= 8 and row[7] == cid:
                row_idx = i
                break

        poster_val  = '✅' if concert.get('poster_status') == 'approved' else '❌'
        tickets_val = '✅' if concert.get('tickets_url') else '❌'
        desc_val    = '✅' if concert.get('description_text') else '❌'
        status_val  = _concert_status_text(concert)

        row_data = [
            concert.get('date', '—'),
            concert.get('artist', ''),
            concert.get('city', ''),
            poster_val,
            tickets_val,
            desc_val,
            status_val,
            cid,
        ]

        color = _concert_status_color(concert)

        if row_idx:
            ws.update(f'A{row_idx}:H{row_idx}', [row_data])
        else:
            ws.append_row(row_data)
            row_idx = len(ws.get_all_values())

        # Красим строку
        ws.format(f'A{row_idx}:H{row_idx}', {
            'backgroundColor': color,
            'horizontalAlignment': 'CENTER',
        })

    # ==================== КАЛЕНДАРЬ ====================

    def _get_or_create_calendar_sheet(self, month: int, year: int):
        """Получает или создаёт лист-календарь для месяца."""
        sheet_name = f"{MONTHS_RU[month]} {year}"
        try:
            ws = self.spreadsheet.worksheet(sheet_name)
            return ws
        except gspread.WorksheetNotFound:
            # Создаём новый лист — 40 строк, 7 колонок
            ws = self.spreadsheet.add_worksheet(sheet_name, rows=40, cols=7)
            return ws

    def _rebuild_calendar_for_concert(self, concert: Dict):
        """Перестраивает календарный лист для месяца концерта."""
        try:
            dt = datetime.strptime(concert['date'], '%d.%m.%Y')
        except:
            return
        self.rebuild_month_calendar(dt.month, dt.year)

    def rebuild_month_calendar(self, month: int, year: int):
        """
        Полностью перестраивает календарь месяца.
        Вызывается после любого изменения концерта.
        """
        if not self._is_connected():
            return
        try:
            ws = self._get_or_create_calendar_sheet(month, year)
            ws.clear()
            self._draw_calendar(ws, month, year)
        except Exception as e:
            logger.error(f"rebuild_month_calendar error: {e}")

    def _draw_calendar(self, ws, month: int, year: int):
        """Рисует календарную сетку и вставляет концерты."""
        from gspread.utils import rowcol_to_a1

        sheet_name = f"{MONTHS_RU[month]} {year}"

        # --- Заголовок месяца ---
        ws.merge_cells('A1:G1')
        ws.update('A1', [[f"{MONTHS_RU[month].upper()} {year}"]])
        ws.format('A1:G1', {
            'backgroundColor': COLOR_HEADER,
            'textFormat': {
                'bold': True,
                'fontSize': 14,
                'foregroundColor': COLOR_WHITE,
            },
            'horizontalAlignment': 'CENTER',
            'verticalAlignment': 'MIDDLE',
        })

        # --- Дни недели ---
        ws.update('A2:G2', [WEEKDAYS_RU])
        ws.format('A2:G2', {
            'backgroundColor': {'red': 0.25, 'green': 0.25, 'blue': 0.25},
            'textFormat': {'bold': True, 'foregroundColor': COLOR_WHITE},
            'horizontalAlignment': 'CENTER',
        })

        # --- Получаем все концерты этого месяца ---
        # Импортируем здесь чтобы избежать циклического импорта
        from bot import get_all_concerts
        all_concerts = get_all_concerts()

        concerts_by_day: Dict[int, List[Dict]] = {}
        for c in all_concerts:
            if not c.get('date'):
                continue
            try:
                dt = datetime.strptime(c['date'], '%d.%m.%Y')
                if dt.month == month and dt.year == year:
                    day = dt.day
                    if day not in concerts_by_day:
                        concerts_by_day[day] = []
                    concerts_by_day[day].append(c)
            except:
                pass

        # --- Рисуем сетку дней ---
        cal = calendar.monthcalendar(year, month)
        # cal — список недель, каждая неделя = [пн, вт, ср, чт, пт, сб, вс]
        # 0 = день не принадлежит этому месяцу

        batch_updates = []   # для массового обновления ячеек
        format_requests = [] # для форматирования через batchUpdate API

        current_row = 3  # строки 1=заголовок, 2=дни недели, с 3й начинаем недели

        for week in cal:
            # Каждая неделя занимает 4 строки:
            # строка 1 — номер дня
            # строки 2-4 — содержимое (концерты)
            day_row   = current_row
            info_rows = [current_row + 1, current_row + 2, current_row + 3]

            row_day_values   = []
            row_info1_values = []
            row_info2_values = []
            row_info3_values = []

            for col_idx, day in enumerate(week):
                col_letter = _col_letter(col_idx + 1)

                if day == 0:
                    row_day_values.append('')
                    row_info1_values.append('')
                    row_info2_values.append('')
                    row_info3_values.append('')
                else:
                    row_day_values.append(str(day))
                    day_concerts = concerts_by_day.get(day, [])

                    if not day_concerts:
                        row_info1_values.append('')
                        row_info2_values.append('')
                        row_info3_values.append('')
                    else:
                        # Вставляем до 3 концертов в день
                        info_lines = ['', '', '']
                        for i, c in enumerate(day_concerts[:3]):
                            time_str = f" {c['time']}" if c.get('time') else ''
                            artist   = c.get('artist', '')
                            status   = _concert_status_text(c)
                            info_lines[i] = f"{artist}{time_str}\n{status}"

                        row_info1_values.append(info_lines[0])
                        row_info2_values.append(info_lines[1])
                        row_info3_values.append(info_lines[2])

            # Записываем строки
            a1_day   = f'A{day_row}:G{day_row}'
            a1_info1 = f'A{info_rows[0]}:G{info_rows[0]}'
            a1_info2 = f'A{info_rows[1]}:G{info_rows[1]}'
            a1_info3 = f'A{info_rows[2]}:G{info_rows[2]}'

            batch_updates.extend([
                {'range': a1_day,   'values': [row_day_values]},
                {'range': a1_info1, 'values': [row_info1_values]},
                {'range': a1_info2, 'values': [row_info2_values]},
                {'range': a1_info3, 'values': [row_info3_values]},
            ])

            current_row += 4

        # Массово записываем все данные
        ws.batch_update(batch_updates)

        # --- Форматирование через Sheets API ---
        spreadsheet_id = self.spreadsheet.id
        requests = []

        sheet_id = ws.id
        current_row = 3

        for week in cal:
            for col_idx, day in enumerate(week):
                if day == 0:
                    current_row_in_loop = None
                else:
                    day_concerts = concerts_by_day.get(day, [])

                    # Номер дня — серый фон
                    requests.append({
                        'repeatCell': {
                            'range': {
                                'sheetId': sheet_id,
                                'startRowIndex': current_row - 1,
                                'endRowIndex': current_row,
                                'startColumnIndex': col_idx,
                                'endColumnIndex': col_idx + 1,
                            },
                            'cell': {
                                'userEnteredFormat': {
                                    'backgroundColor': COLOR_DATE_BG,
                                    'textFormat': {'bold': True, 'fontSize': 11},
                                    'horizontalAlignment': 'LEFT',
                                    'verticalAlignment': 'MIDDLE',
                                }
                            },
                            'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)',
                        }
                    })

                    # Ячейки с концертами — цвет статуса
                    if day_concerts:
                        for i, c in enumerate(day_concerts[:3]):
                            color = _concert_status_color(c)
                            requests.append({
                                'repeatCell': {
                                    'range': {
                                        'sheetId': sheet_id,
                                        'startRowIndex': current_row + i,
                                        'endRowIndex': current_row + i + 1,
                                        'startColumnIndex': col_idx,
                                        'endColumnIndex': col_idx + 1,
                                    },
                                    'cell': {
                                        'userEnteredFormat': {
                                            'backgroundColor': color,
                                            'textFormat': {'fontSize': 9},
                                            'wrapStrategy': 'WRAP',
                                            'verticalAlignment': 'TOP',
                                        }
                                    },
                                    'fields': 'userEnteredFormat(backgroundColor,textFormat,wrapStrategy,verticalAlignment)',
                                }
                            })

            current_row += 4

        # Высота строк — большие для ячеек с концертами
        row_heights = []
        r = 3
        for _ in cal:
            row_heights.append({'index': r - 1,     'pixelSize': 22})   # число дня
            row_heights.append({'index': r,         'pixelSize': 52})   # концерт 1
            row_heights.append({'index': r + 1,     'pixelSize': 52})   # концерт 2
            row_heights.append({'index': r + 2,     'pixelSize': 52})   # концерт 3
            r += 4

        for rh in row_heights:
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'ROWS',
                        'startIndex': rh['index'],
                        'endIndex': rh['index'] + 1,
                    },
                    'properties': {'pixelSize': rh['pixelSize']},
                    'fields': 'pixelSize',
                }
            })

        # Ширина колонок — одинаковая
        requests.append({
            'updateDimensionProperties': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'startIndex': 0,
                    'endIndex': 7,
                },
                'properties': {'pixelSize': 160},
                'fields': 'pixelSize',
            }
        })

        # Отправляем все форматирования одним запросом
        if requests:
            self.spreadsheet.batch_update({'requests': requests})

        logger.info(f"Календарь '{sheet_name}' обновлён")

    def rebuild_all_calendars(self):
        """Пересобирает все календари по текущим данным из БД."""
        if not self._is_connected():
            return
        try:
            from bot import get_all_concerts
            concerts = get_all_concerts()
            months_to_rebuild = set()
            for c in concerts:
                if c.get('date'):
                    try:
                        dt = datetime.strptime(c['date'], '%d.%m.%Y')
                        months_to_rebuild.add((dt.month, dt.year))
                    except:
                        pass
            for month, year in months_to_rebuild:
                self.rebuild_month_calendar(month, year)
        except Exception as e:
            logger.error(f"rebuild_all_calendars error: {e}")
