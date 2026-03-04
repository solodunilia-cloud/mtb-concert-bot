#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Sheets Manager — MTB Concerts Bot
Sheets = единственная база данных. SQLite убран полностью.
При старте бот читает все концерты из листа "Данные".
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

# ─── ЦВЕТА ───────────────────────────────────────────────────────────────────

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

# Колонки листа "Данные": A-K
# A=Сайт(статус), B=Дата, C=Время, D=Страничка, E=Артист,
# F=Покупка билета, G=Картинка, H=Текст, I=Афиша, J=Статус, K=ID
COL_COUNT = 11

# ─── ВСПОМОГАТЕЛЬНЫЕ ─────────────────────────────────────────────────────────

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

def _row_to_concert(row: List[str], row_idx: int) -> Optional[Dict]:
    """Превращает строку Sheets в dict концерта."""
    if len(row) < COL_COUNT:
        row = row + [''] * (COL_COUNT - len(row))
    cid_str = row[10].strip()
    if not cid_str or not cid_str.isdigit():
        return None
    status_col = row[9].strip()
    if row[0].strip() == '🚫':
        status = 'cancelled'
    elif status_col == 'published':
        status = 'published'
    else:
        status = 'draft'
    return {
        'id':               int(cid_str),
        'status':           status,
        'date':             row[1].strip() or None,
        'time':             row[2].strip() or None,
        'artist':           row[4].strip(),
        'tickets_url':      row[5].strip() or None,
        'poster_file_id':   row[6].strip() or None,
        'description_text': row[7].strip() or None,
        'poster_status':    'approved' if row[8].strip() == '✅' else 'none',
        'created_at':       '',
        'updated_at':       '',
        '_row':             row_idx,
    }

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
            import traceback
            logger.error(f"Google Sheets init error: {e}")
            logger.error(traceback.format_exc())

    def is_connected(self) -> bool:
        return self.client is not None and self.spreadsheet is not None

    # ── ЛИСТ "ДАННЫЕ" ────────────────────────────────────────────────────────

    def _get_or_create_data_sheet(self):
        try:
            return self.spreadsheet.worksheet('Данные')
        except Exception:
            ws = self.spreadsheet.add_worksheet('Данные', rows=500, cols=COL_COUNT)
            ws.update('A1:K1', [['Сайт', 'Дата', 'Время', 'Страничка', 'Артист',
                                  'Покупка билета', 'Картинка', 'Текст', 'Афиша', 'Статус', 'ID']])
            ws.format('A1:K1', {
                'backgroundColor': C_HEADER,
                'textFormat': {'bold': True, 'foregroundColor': C_WHITE, 'fontSize': 10},
                'horizontalAlignment': 'CENTER',
            })
            try:
                self.spreadsheet.batch_update({'requests': [{
                    'updateSheetProperties': {
                        'properties': {'sheetId': ws.id, 'gridProperties': {'frozenRowCount': 1}},
                        'fields': 'gridProperties.frozenRowCount'
                    }
                }]})
            except Exception:
                pass
            return ws

    def load_all_concerts(self) -> List[Dict]:
        """
        Читает все концерты из листа "Данные" при старте бота.
        Возвращает список dict — это и есть "база данных" в памяти.
        """
        if not self.is_connected():
            logger.warning("Sheets не подключён — начинаем с пустым списком")
            return []
        try:
            ws      = self._get_or_create_data_sheet()
            rows    = ws.get_all_values()
            result  = []
            for i, row in enumerate(rows[1:], start=2):
                c = _row_to_concert(row, i)
                if c and c['artist']:
                    result.append(c)
            logger.info(f"✅ Загружено концертов из Sheets: {len(result)}")
            return result
        except Exception as e:
            logger.error(f"load_all_concerts error: {e}")
            return []

    def next_id(self, concerts: List[Dict]) -> int:
        """Следующий ID — максимум существующих + 1."""
        if not concerts:
            return 1
        return max(c['id'] for c in concerts) + 1

    # ── ЧАТЫ ─────────────────────────────────────────────────────────────────

    def load_chats(self) -> List[int]:
        if not self.is_connected():
            return []
        try:
            try:
                ws = self.spreadsheet.worksheet('Чаты')
            except Exception:
                ws = self.spreadsheet.add_worksheet('Чаты', rows=100, cols=1)
                ws.update('A1', [['chat_id']])
                return []
            rows = ws.get_all_values()
            return [int(r[0].strip()) for r in rows[1:] if r and r[0].strip().lstrip('-').isdigit()]
        except Exception as e:
            logger.error(f"load_chats error: {e}")
            return []

    def save_chat(self, chat_id: int, known_chats: List[int]):
        if not self.is_connected() or chat_id in known_chats:
            return
        try:
            try:
                ws = self.spreadsheet.worksheet('Чаты')
            except Exception:
                ws = self.spreadsheet.add_worksheet('Чаты', rows=100, cols=1)
                ws.update('A1', [['chat_id']])
            ws.append_row([chat_id])
        except Exception as e:
            logger.error(f"save_chat error: {e}")

    # ── SYNC / DELETE ─────────────────────────────────────────────────────────

    def sync_concert(self, concert: Dict, all_concerts: List[Dict]):
        """Записывает/обновляет концерт в Sheets + обновляет календарь."""
        if not self.is_connected():
            return
        try:
            self._sync_data_row(concert)
            if concert.get('date'):
                self._rebuild_calendar_for_concert(concert, all_concerts)
        except Exception as e:
            logger.error(f"sync_concert error: {e}")

    def delete_concert(self, concert: Dict, all_concerts: List[Dict]):
        """Удаляет строку концерта из листа Данные."""
        if not self.is_connected():
            return
        try:
            ws      = self._get_or_create_data_sheet()
            row_idx = concert.get('_row')
            if row_idx:
                ws.delete_rows(row_idx)
                for c in all_concerts:
                    if c.get('_row', 0) > row_idx:
                        c['_row'] -= 1
            if concert.get('date'):
                self._rebuild_calendar_for_concert(concert, all_concerts)
        except Exception as e:
            logger.error(f"delete_concert error: {e}")

    def _sync_data_row(self, concert: Dict):
        ws         = self._get_or_create_data_sheet()
        all_values = ws.get_all_values()
        cid        = str(concert.get('id', ''))

        date_str   = concert.get('date', '') or ''
        time_str   = concert.get('time', '') or ''
        slug       = concert.get('slug', '') or ''
        status_val = concert.get('status', 'draft')

        row_data = [[
            '✅' if status_val != 'cancelled' else '🚫',
            date_str,
            time_str,
            f"https://mtbarmoscow.com/{slug}" if slug else '',
            concert.get('artist', ''),
            concert.get('tickets_url', '') or '',
            concert.get('poster_file_id', '') or '',
            (concert.get('description_text', '') or '')[:200],
            '✅' if concert.get('poster_status') == 'approved' else '❌',
            status_val,
            cid,
        ]]

        row_idx = concert.get('_row')
        if not row_idx:
            for i, row in enumerate(all_values[1:], start=2):
                if len(row) >= 11 and row[10] == cid:
                    row_idx = i
                    concert['_row'] = row_idx
                    break

        if row_idx:
            ws.update(f'A{row_idx}:K{row_idx}', row_data)
            bg = C_BLACK if row_idx % 2 == 0 else C_DARKGRAY
        else:
            ws.append_row(row_data[0])
            row_idx = len(ws.get_all_values())
            concert['_row'] = row_idx
            bg = C_BLACK if row_idx % 2 == 0 else C_DARKGRAY

        ws.format(f'A{row_idx}:K{row_idx}', {
            'backgroundColor': bg,
            'textFormat': {'foregroundColor': C_LIGHT, 'fontSize': 9},
            'verticalAlignment': 'MIDDLE',
        })
        for col in ['D', 'F', 'G']:
            ws.format(f'{col}{row_idx}', {
                'backgroundColor': bg,
                'textFormat': {'foregroundColor': C_YELLOW, 'fontSize': 9},
            })
        status_ok = (concert.get('poster_status') == 'approved' and
                     concert.get('tickets_url') and concert.get('description_text'))
        ws.format(f'J{row_idx}', {
            'backgroundColor': bg,
            'textFormat': {'foregroundColor': C_GREEN_TEXT if status_ok else C_RED_TEXT, 'fontSize': 9},
        })

    # ── CALENDAR ─────────────────────────────────────────────────────────────

    def _get_or_create_calendar_sheet(self, month: int, year: int):
        sheet_name = f"{MONTHS_RU[month]} {year}"
        try:
            return self.spreadsheet.worksheet(sheet_name)
        except Exception:
            return self.spreadsheet.add_worksheet(sheet_name, rows=50, cols=7)

    def _rebuild_calendar_for_concert(self, concert: Dict, all_concerts: List[Dict]):
        try:
            dt = datetime.strptime(concert['date'], '%d.%m.%Y')
        except Exception:
            return
        self.rebuild_month_calendar(dt.month, dt.year, all_concerts)

    def rebuild_month_calendar(self, month: int, year: int, all_concerts: List[Dict]):
        if not self.is_connected():
            return
        try:
            ws = self._get_or_create_calendar_sheet(month, year)
            ws.clear()
            self._draw_calendar(ws, month, year, all_concerts)
        except Exception as e:
            logger.error(f"rebuild_month_calendar error: {e}")

    def rebuild_all_calendars(self, all_concerts: List[Dict]):
        if not self.is_connected():
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
                self.rebuild_month_calendar(month, year, all_concerts)
        except Exception as e:
            logger.error(f"rebuild_all_calendars error: {e}")

    def _draw_calendar(self, ws, month: int, year: int, all_concerts: List[Dict]):
        concerts_by_day: Dict[int, List[Dict]] = {}
        for c in all_concerts:
            if not c.get('date') or c.get('status') == 'cancelled':
                continue
            try:
                dt = datetime.strptime(c['date'], '%d.%m.%Y')
                if dt.month == month and dt.year == year:
                    concerts_by_day.setdefault(dt.day, []).append(c)
            except Exception:
                pass

        ws.merge_cells('A1:G1')
        ws.update('A1', [[f"АФИША МЕРОПРИЯТИЙ — {MONTHS_RU[month].upper()} {year}"]])
        ws.format('A1:G1', {
            'backgroundColor': C_TITLE_BG,
            'textFormat': {'bold': True, 'fontSize': 16, 'foregroundColor': C_YELLOW},
            'horizontalAlignment': 'CENTER',
            'verticalAlignment': 'MIDDLE',
        })

        ws.update('A2:G2', [WEEKDAYS_RU])
        ws.format('A2:G2', {
            'backgroundColor': C_HEADER,
            'textFormat': {'bold': True, 'foregroundColor': C_WHITE, 'fontSize': 11},
            'horizontalAlignment': 'CENTER',
        })

        cal         = calendar.monthcalendar(year, month)
        batch       = []
        current_row = 3

        for week in cal:
            row_days, row_info1, row_info2, row_info3 = [], [], [], []
            for day in week:
                if day == 0:
                    row_days.append(''); row_info1.append(''); row_info2.append(''); row_info3.append('')
                else:
                    row_days.append(str(day))
                    day_cs = concerts_by_day.get(day, [])
                    info   = ['', '', '']
                    for i, c in enumerate(day_cs[:3]):
                        t = f" {c['time']}" if c.get('time') else ''
                        info[i] = f"{c.get('artist','')}{t}\n{_status_text(c)}"
                    row_info1.append(info[0]); row_info2.append(info[1]); row_info3.append(info[2])

            r = current_row
            batch += [
                {'range': f'A{r}:G{r}',     'values': [row_days]},
                {'range': f'A{r+1}:G{r+1}', 'values': [row_info1]},
                {'range': f'A{r+2}:G{r+2}', 'values': [row_info2]},
                {'range': f'A{r+3}:G{r+3}', 'values': [row_info3]},
            ]
            current_row += 4

        ws.batch_update(batch)

        sheet_id    = ws.id
        requests    = []
        current_row = 3

        for week in cal:
            for col_idx, day in enumerate(week):
                if day != 0:
                    day_cs = concerts_by_day.get(day, [])
                    requests.append({'repeatCell': {
                        'range': {'sheetId': sheet_id, 'startRowIndex': current_row-1, 'endRowIndex': current_row,
                                  'startColumnIndex': col_idx, 'endColumnIndex': col_idx+1},
                        'cell': {'userEnteredFormat': {
                            'backgroundColor': C_CAL_DATE,
                            'textFormat': {'bold': True, 'fontSize': 11},
                            'horizontalAlignment': 'LEFT', 'verticalAlignment': 'MIDDLE',
                        }},
                        'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)',
                    }})
                    for i, c in enumerate(day_cs[:3]):
                        requests.append({'repeatCell': {
                            'range': {'sheetId': sheet_id, 'startRowIndex': current_row+i, 'endRowIndex': current_row+i+1,
                                      'startColumnIndex': col_idx, 'endColumnIndex': col_idx+1},
                            'cell': {'userEnteredFormat': {
                                'backgroundColor': _status_color_cal(c),
                                'textFormat': {'fontSize': 9, 'foregroundColor': C_BLACK},
                                'wrapStrategy': 'WRAP', 'verticalAlignment': 'TOP',
                            }},
                            'fields': 'userEnteredFormat(backgroundColor,textFormat,wrapStrategy,verticalAlignment)',
                        }})
            current_row += 4

        r = 3
        for _ in cal:
            requests += [
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': r-1, 'endIndex': r},   'properties': {'pixelSize': 22}, 'fields': 'pixelSize'}},
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': r,   'endIndex': r+1}, 'properties': {'pixelSize': 55}, 'fields': 'pixelSize'}},
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': r+1, 'endIndex': r+2}, 'properties': {'pixelSize': 55}, 'fields': 'pixelSize'}},
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': r+2, 'endIndex': r+3}, 'properties': {'pixelSize': 55}, 'fields': 'pixelSize'}},
            ]
            r += 4

        requests.append({'updateDimensionProperties': {
            'range': {'sheetId': sheet_id, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': 7},
            'properties': {'pixelSize': 170}, 'fields': 'pixelSize',
        }})
        requests.append({'updateDimensionProperties': {
            'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': 1},
            'properties': {'pixelSize': 45}, 'fields': 'pixelSize',
        }})

        if requests:
            self.spreadsheet.batch_update({'requests': requests})

        logger.info(f"✅ Календарь '{MONTHS_RU[month]} {year}' обновлён")
