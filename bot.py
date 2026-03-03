#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MTB Concerts Bot — v5
Все правки из чеклиста:
1. /new — любой порядок слов, предлагает создать если имя+дата без команды
2. /edit [номер] — редактирует конкретное, без номера — список
3. Защита от дублей при создании
4. /list afisha/tickets/text — фильтры по отсутствующим полям
5. /list 2026-04 — по месяцам
6. После даты без времени — спрашивает время
"""

import os
import re
import logging
import sqlite3
from datetime import datetime, time as dtime
from typing import Optional, Dict, List, Tuple

from rapidfuzz import fuzz
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)
from google_sheets import GoogleSheetsManager

# ─── НАСТРОЙКИ ────────────────────────────────────────────────────────────────

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN     = os.getenv('TELEGRAM_TOKEN', '')
OWNER_ID  = int(os.getenv('OWNER_ID', '534303997'))
SHEETS_ID = os.getenv('GOOGLE_SHEETS_ID', '')
DB_PATH   = 'concerts.db'

sheets = GoogleSheetsManager(spreadsheet_id=SHEETS_ID if SHEETS_ID else None)

KW = {
    'tickets': ['билеты', 'билет', 'ticket', 'tickets'],
    'poster':  ['афиша', 'poster'],
    'text':    ['текст', 'описание', 'text'],
    'date':    ['дата', 'date', 'перенос'],
    'cancel':  ['отмена', 'отменен', 'отменён', 'отменили', 'cancel'],
}
POSTER_OK = ['одобрена', 'ок', 'ok', 'утверждена', 'готова', 'approved']

# ─── БАЗА ДАННЫХ ──────────────────────────────────────────────────────────────

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS concerts (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            artist           TEXT NOT NULL,
            date             TEXT,
            time             TEXT,
            poster_status    TEXT DEFAULT 'none',
            poster_file_id   TEXT,
            tickets_url      TEXT,
            description_text TEXT,
            status           TEXT DEFAULT 'draft',
            created_at       TEXT DEFAULT (datetime('now')),
            updated_at       TEXT DEFAULT (datetime('now'))
        )''')
        conn.execute('CREATE TABLE IF NOT EXISTS chats (chat_id INTEGER PRIMARY KEY)')

def db_save(data: dict) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        now = datetime.now().isoformat()
        if data.get('id'):
            conn.execute('''UPDATE concerts SET
                artist=?, date=?, time=?, poster_status=?, poster_file_id=?,
                tickets_url=?, description_text=?, status=?, updated_at=?
                WHERE id=?''', (
                data.get('artist'), data.get('date'), data.get('time'),
                data.get('poster_status', 'none'), data.get('poster_file_id'),
                data.get('tickets_url'), data.get('description_text'),
                data.get('status', 'draft'), now, data['id']
            ))
            return data['id']
        else:
            cur = conn.execute('''INSERT INTO concerts
                (artist, date, time, poster_status, tickets_url, description_text, status)
                VALUES (?,?,?,?,?,?,?)''', (
                data.get('artist'), data.get('date'), data.get('time'),
                data.get('poster_status', 'none'), data.get('tickets_url'),
                data.get('description_text'), data.get('status', 'draft')
            ))
            return cur.lastrowid

def db_get(cid: int) -> Optional[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute('SELECT * FROM concerts WHERE id=?', (cid,)).fetchone()
        return dict(row) if row else None

def db_all(include_cancelled=False) -> List[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        q = 'SELECT * FROM concerts ORDER BY date ASC, id DESC' if include_cancelled else \
            "SELECT * FROM concerts WHERE status != 'cancelled' ORDER BY date ASC, id DESC"
        return [dict(r) for r in conn.execute(q).fetchall()]

def db_delete(cid: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('DELETE FROM concerts WHERE id=?', (cid,))

def register_chat(chat_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('INSERT OR IGNORE INTO chats (chat_id) VALUES (?)', (chat_id,))

def get_chats() -> List[int]:
    with sqlite3.connect(DB_PATH) as conn:
        return [r[0] for r in conn.execute('SELECT chat_id FROM chats').fetchall()]

# ─── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ──────────────────────────────────────────────────

def norm(text: str) -> str:
    text = text.lower().replace('ё', 'е')
    return re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', text)).strip()

def extract_url(text: str) -> Optional[str]:
    m = re.search(r'https?://\S+', text)
    return m.group(0) if m else None

MONTHS = {
    'января':1,'февраля':2,'марта':3,'апреля':4,'мая':5,'июня':6,
    'июля':7,'августа':8,'сентября':9,'октября':10,'ноября':11,'декабря':12
}

def extract_date_time(text: str) -> Tuple[Optional[str], Optional[str]]:
    date_str = time_str = None

    # DD.MM.YYYY или DD/MM/YYYY или DD-MM-YYYY
    m = re.search(r'(\d{1,2})[./\-](\d{1,2})[./\-](\d{4})', text)
    if m:
        d, mo, y = m.groups()
        date_str = f"{int(d):02d}.{int(mo):02d}.{y}"

    # DD месяц YYYY
    if not date_str:
        pat = r'(\d{1,2})\s+(' + '|'.join(MONTHS) + r')(?:\s+(\d{4}))?'
        m = re.search(pat, text.lower())
        if m:
            d, mo_s, y = m.group(1), m.group(2), m.group(3) or str(datetime.now().year)
            date_str = f"{int(d):02d}.{MONTHS[mo_s]:02d}.{y}"

    # DD.MM (без года)
    if not date_str:
        m = re.search(r'\b(\d{1,2})[./](\d{1,2})\b', text)
        if m:
            d, mo = m.groups()
            y = str(datetime.now().year)
            date_str = f"{int(d):02d}.{int(mo):02d}.{y}"

    # Время HH:MM или HH.MM
    m = re.search(r'\b(\d{1,2})[:\.](\d{2})\b', text)
    if m:
        h, mi = m.groups()
        if 0 <= int(h) <= 23:
            time_str = f"{int(h):02d}:{int(mi):02d}"

    return date_str, time_str

def strip_date_time(text: str) -> str:
    """Убирает дату и время из текста."""
    cleaned = re.sub(r'\d{1,2}[./\-]\d{1,2}[./\-]\d{4}', '', text)
    cleaned = re.sub(r'\b\d{1,2}[./]\d{1,2}\b', '', cleaned)
    cleaned = re.sub(r'\b\d{1,2}[:.]\d{2}\b', '', cleaned)
    for mo in MONTHS:
        cleaned = re.sub(r'(?i)\b' + mo + r'\b', '', cleaned)
    return re.sub(r'\s+', ' ', cleaned).strip()

def fuzzy_find(name: str) -> List[dict]:
    all_c  = db_all()
    name_n = norm(name)
    results = []
    for c in all_c:
        score = fuzz.token_set_ratio(name_n, norm(c['artist']))
        if score >= 70:
            results.append((c, score))
    if not results:
        return []
    results.sort(key=lambda x: x[1], reverse=True)
    top = results[0][1]
    if top == 100 or (len(results) >= 2 and top - results[1][1] >= 20):
        return [results[0][0]]
    return [r[0] for r in results[:5]]

# ─── КАРТОЧКА ─────────────────────────────────────────────────────────────────

def is_ready(c: dict) -> bool:
    return all([c.get('date'), c.get('poster_status') == 'approved',
                c.get('tickets_url'), c.get('description_text')])

def missing(c: dict) -> List[str]:
    m = []
    if not c.get('date'):                       m.append('дата')
    if c.get('poster_status') != 'approved':    m.append('афиша')
    if not c.get('tickets_url'):                m.append('билеты')
    if not c.get('description_text'):           m.append('текст')
    return m

def s_icon(c: dict) -> str:
    s = c.get('status', 'draft')
    if s == 'cancelled': return '🚫'
    if s == 'published': return '⚫'
    if is_ready(c):      return '🟢'
    filled = sum([bool(c.get('date')), c.get('poster_status') == 'approved',
                  bool(c.get('tickets_url')), bool(c.get('description_text'))])
    return '🟡' if filled >= 2 else '🔴'

def card(c: dict) -> str:
    icon  = s_icon(c)
    dt    = f"{c['date']} {c.get('time') or ''}".strip() if c.get('date') else '—'
    title = f"#{c['id']} {c['artist']}" + (f" • {dt}" if c.get('date') else '')
    m     = missing(c)
    text  = (
        f"{icon} *{title}*\n\n"
        f"{'✅' if c.get('poster_status') == 'approved' else '❌'} Афиша\n"
        f"{'✅' if c.get('tickets_url') else '❌'} Билеты\n"
        f"{'✅' if c.get('description_text') else '❌'} Текст\n"
        f"{'✅' if c.get('date') else '❌'} Дата\n"
    )
    if m:
        text += f"\n❗ Не хватает: {', '.join(m)}"
    else:
        text += f"\n🟢 Готово → `/code {c['id']}`"
    return text

def edit_kb(cid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Дата",      callback_data=f"ed|date|{cid}"),
         InlineKeyboardButton("🖼 Афиша ✓",   callback_data=f"do|poster|{cid}")],
        [InlineKeyboardButton("🎟 Билеты",    callback_data=f"ed|tickets|{cid}"),
         InlineKeyboardButton("📝 Текст",     callback_data=f"ed|text|{cid}")],
        [InlineKeyboardButton("✏️ Артист",    callback_data=f"ed|artist|{cid}"),
         InlineKeyboardButton("🚫 Отменить",  callback_data=f"do|cancel|{cid}")],
        [InlineKeyboardButton("⚫ Опубликовать", callback_data=f"do|publish|{cid}"),
         InlineKeyboardButton("🗑 Удалить",   callback_data=f"do|delete|{cid}")],
    ])

async def notify_ready(ctx: ContextTypes.DEFAULT_TYPE, c: dict):
    if is_ready(c) and c.get('status') == 'draft':
        try:
            await ctx.bot.send_message(
                OWNER_ID,
                f"🎤 *{c['artist']}* — готов к публикации!\n`/code {c['id']}`",
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"notify: {e}")

# ─── ПАРСИНГ ТРИГГЕРА ─────────────────────────────────────────────────────────

def detect_kw(text: str) -> Optional[Tuple[str, str]]:
    text_n = norm(text)
    words  = text_n.split()
    for action, keywords in KW.items():
        for kw in keywords:
            kw_n = norm(kw)
            if kw_n in words:
                return action, kw
            for w in words:
                if len(w) >= 4 and fuzz.ratio(kw_n, w) >= 85:
                    return action, w
    return None

def parse_trigger(text: str) -> Optional[dict]:
    """Любой порядок: Артист билеты URL / билеты Артист URL / и т.д."""
    text = text.strip()
    if not text:
        return None
    result = detect_kw(text)
    if not result:
        return None
    action, found_kw = result

    # Убираем URL, ключевые слова, даты — остаток = артист
    cleaned = re.sub(r'https?://\S+', '', text)
    all_kw  = [w for ws in KW.values() for w in ws] + POSTER_OK
    for kw in all_kw:
        cleaned = re.sub(r'(?i)\b' + re.escape(kw) + r'\b', '', cleaned)
    cleaned = strip_date_time(cleaned)
    artist  = re.sub(r'\s+', ' ', cleaned).strip()

    if not artist or len(artist) < 2:
        return None

    url     = extract_url(text)
    payload = url or ''

    if action == 'text':
        text_n = norm(text)
        kw_n   = norm(found_kw)
        idx    = text_n.find(kw_n)
        if idx != -1:
            after = text[idx + len(found_kw):].strip()
            if after:
                payload = after

    elif action == 'date':
        d, t    = extract_date_time(text)
        payload = f"{d or ''} {t or ''}".strip()

    elif action == 'poster':
        payload = text

    return {'artist': artist, 'action': action, 'payload': payload}

def parse_free_text(text: str) -> Optional[dict]:
    """
    Распознаёт свободный ввод БЕЗ команды:
    'Иван Дорн 15.04.2026 21:00' → предложить создать
    Возвращает {artist, date, time} или None
    """
    d, t    = extract_date_time(text)
    if not d:
        return None
    artist = strip_date_time(text).strip()
    # Убираем URL
    artist = re.sub(r'https?://\S+', '', artist).strip()
    if len(artist) < 2:
        return None
    return {'artist': artist, 'date': d, 'time': t}

# ─── ПРИМЕНИТЬ ДЕЙСТВИЕ ───────────────────────────────────────────────────────

async def apply_action(upd: Update, ctx: ContextTypes.DEFAULT_TYPE,
                        c: dict, action: str, payload: str):
    msg  = upd.effective_message
    cid  = c['id']
    name = c['artist']

    if action == 'cancel':
        kb = [[InlineKeyboardButton("✅ Да",  callback_data=f"do|cancel|{cid}"),
               InlineKeyboardButton("❌ Нет", callback_data="noop")]]
        await msg.reply_text(f"Отменить *{name}*?",
                             reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    elif action == 'tickets':
        url = extract_url(payload) or extract_url(upd.effective_message.text or '')
        if not url:
            await msg.reply_text(f"Нет ссылки.\nПример: `{name} билеты https://...`",
                                 parse_mode='Markdown')
            return
        ctx.user_data[f'v_{cid}'] = url
        label = "Перезаписать билеты" if c.get('tickets_url') else "Добавить билеты"
        kb = [[InlineKeyboardButton("✅ Да",  callback_data=f"do|tickets|{cid}"),
               InlineKeyboardButton("❌ Нет", callback_data="noop")]]
        await msg.reply_text(f"{label} для *{name}*?",
                             reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    elif action == 'poster':
        if not any(norm(w) in norm(payload) for w in POSTER_OK):
            await msg.reply_text(f"Напиши: `{name} афиша одобрена`", parse_mode='Markdown')
            return
        kb = [[InlineKeyboardButton("✅ Да",  callback_data=f"do|poster|{cid}"),
               InlineKeyboardButton("❌ Нет", callback_data="noop")]]
        await msg.reply_text(f"Отметить афишу *{name}* как одобренную?",
                             reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    elif action == 'text':
        if len(payload) < 10:
            await msg.reply_text(f"Текст слишком короткий.\nПример: `{name} текст Описание...`",
                                 parse_mode='Markdown')
            return
        ctx.user_data[f'v_{cid}'] = payload
        preview = payload[:100] + ('...' if len(payload) > 100 else '')
        kb = [[InlineKeyboardButton("✅ Да",  callback_data=f"do|text|{cid}"),
               InlineKeyboardButton("❌ Нет", callback_data="noop")]]
        await msg.reply_text(f"Добавить текст для *{name}*?\n\n_{preview}_",
                             reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    elif action == 'date':
        d, t = extract_date_time(payload + ' ' + (upd.effective_message.text or ''))
        if not d:
            await msg.reply_text(f"Не распознал дату.\nПример: `{name} дата 15.04.2026 21:00`",
                                 parse_mode='Markdown')
            return
        ctx.user_data[f'v_{cid}'] = (d, t)
        dt = f"{d} {t or ''}".strip()
        if not t:
            # Спрашиваем время отдельно
            ctx.user_data[f'aw_time_{cid}'] = d
            kb = [[InlineKeyboardButton("Пропустить", callback_data=f"do|date|{cid}")]]
            await msg.reply_text(
                f"Дата *{name}*: `{d}`\nВведи время (например `21:00`) или пропусти:",
                reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown'
            )
            ctx.user_data['aw']    = 'time_for_date'
            ctx.user_data['aw_id'] = cid
            return
        kb = [[InlineKeyboardButton("✅ Да",  callback_data=f"do|date|{cid}"),
               InlineKeyboardButton("❌ Нет", callback_data="noop")]]
        await msg.reply_text(f"Установить дату *{name}*: `{dt}`?",
                             reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

# ─── ОБРАБОТКА ТРИГГЕРА ───────────────────────────────────────────────────────

async def process_trigger(upd: Update, ctx: ContextTypes.DEFAULT_TYPE, parsed: dict):
    msg    = upd.effective_message
    name   = parsed['artist']
    action = parsed['action']
    payload= parsed['payload']

    matches = fuzzy_find(name)

    if not matches:
        kb = [[InlineKeyboardButton("✅ Создать", callback_data=f"cnew|{name}|{action}|{payload[:80]}"),
               InlineKeyboardButton("❌ Отмена",  callback_data="noop")]]
        await msg.reply_text(f"*{name}* не найден.\nСоздать новое мероприятие?",
                             reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        return

    if len(matches) > 1:
        kb = [[InlineKeyboardButton(
            f"#{c['id']} {c['artist']}" + (f" • {c['date']}" if c.get('date') else ''),
            callback_data=f"tsel|{c['id']}|{action}|{payload[:80]}"
        )] for c in matches]
        kb.append([InlineKeyboardButton("❌ Отмена", callback_data="noop")])
        await msg.reply_text("Найдено несколько — уточни:",
                             reply_markup=InlineKeyboardMarkup(kb))
        return

    await apply_action(upd, ctx, matches[0], action, payload)

# ─── CALLBACKS ────────────────────────────────────────────────────────────────

async def on_callback(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = upd.callback_query
    await q.answer()
    data = q.data

    if data == 'noop':
        await q.edit_message_text("Отменено")
        return

    if data.startswith('cnew|'):
        _, name, action, payload = data.split('|', 3)
        cid = db_save({'artist': name})
        c   = db_get(cid)
        await q.edit_message_text(f"✅ Создано: *#{cid} {name}*", parse_mode='Markdown')
        await apply_action(upd, ctx, c, action, payload)
        return

    if data.startswith('tsel|'):
        _, cid_s, action, payload = data.split('|', 3)
        c = db_get(int(cid_s))
        await q.edit_message_text(f"#{c['id']} {c['artist']}")
        await apply_action(upd, ctx, c, action, payload)
        return

    if data.startswith('do|'):
        _, action, cid_s = data.split('|')
        cid = int(cid_s)
        c   = db_get(cid)
        if not c:
            await q.edit_message_text("Не найдено")
            return

        if action == 'tickets':
            url = ctx.user_data.pop(f'v_{cid}', None)
            if url:
                c['tickets_url'] = url
                db_save(c); sheets.sync_concert(c)
                await notify_ready(ctx, c)
                await q.edit_message_text(f"✅ Билеты добавлены — *{c['artist']}*", parse_mode='Markdown')

        elif action == 'poster':
            c['poster_status'] = 'approved'
            db_save(c); sheets.sync_concert(c)
            await notify_ready(ctx, c)
            await q.edit_message_text(f"✅ Афиша одобрена — *{c['artist']}*", parse_mode='Markdown')

        elif action == 'text':
            txt = ctx.user_data.pop(f'v_{cid}', None)
            if txt:
                c['description_text'] = txt
                db_save(c); sheets.sync_concert(c)
                await notify_ready(ctx, c)
                await q.edit_message_text(f"✅ Текст добавлен — *{c['artist']}*", parse_mode='Markdown')

        elif action == 'date':
            val = ctx.user_data.pop(f'v_{cid}', None)
            if val:
                d, t = val
                c['date'] = d
                if t: c['time'] = t
                db_save(c); sheets.sync_concert(c)
                await notify_ready(ctx, c)
                await q.edit_message_text(f"✅ Дата установлена — *{c['artist']}*", parse_mode='Markdown')

        elif action == 'cancel':
            c['status'] = 'cancelled'
            db_save(c); sheets.sync_concert(c)
            await q.edit_message_text(f"🚫 *{c['artist']}* — отменён", parse_mode='Markdown')

        elif action == 'publish':
            c['status'] = 'published'
            db_save(c); sheets.sync_concert(c)
            await q.edit_message_text(f"⚫ *{c['artist']}* — опубликован", parse_mode='Markdown')

        elif action == 'delete':
            name = c['artist']
            db_delete(cid)
            await q.edit_message_text(f"🗑 *{name}* — удалён", parse_mode='Markdown')
        return

    if data.startswith('edit_menu_'):
        cid = int(data.split('_')[2])
        c   = db_get(cid)
        if c:
            await q.edit_message_text(card(c), reply_markup=edit_kb(cid), parse_mode='Markdown')
        return

    if data.startswith('ed|'):
        _, field, cid_s = data.split('|')
        cid = int(cid_s)
        c   = db_get(cid)
        ctx.user_data['aw']    = field
        ctx.user_data['aw_id'] = cid
        prompts = {
            'date':    f"📅 Дата для *{c['artist']}*:\nПример: `15.04.2026 21:00`",
            'tickets': f"🎟 Ссылка для *{c['artist']}*:",
            'text':    f"📝 Описание для *{c['artist']}*:",
            'artist':  f"✏️ Новое имя (сейчас: {c['artist']}):",
        }
        await q.edit_message_text(prompts.get(field, 'Введи:'), parse_mode='Markdown')
        return

    if data.startswith('fc|'):
        _, name, d, t = data.split('|')
        cid = db_save({'artist': name, 'date': d or None, 'time': t or None})
        c   = db_get(cid)
        await q.edit_message_text(card(c), reply_markup=edit_kb(cid), parse_mode='Markdown')

    if data.startswith('new_confirm|'):
        # cnew from free text: new_confirm|artist|date|time
        parts  = data.split('|')
        artist = parts[1]
        d      = parts[2] or None
        t      = parts[3] or None
        cid    = db_save({'artist': artist, 'date': d, 'time': t})
        c      = db_get(cid)
        await q.edit_message_text(card(c), reply_markup=edit_kb(cid), parse_mode='Markdown')

# ─── ОЖИДАНИЕ ВВОДА ───────────────────────────────────────────────────────────

async def handle_awaiting(upd: Update, ctx: ContextTypes.DEFAULT_TYPE) -> bool:
    field = ctx.user_data.get('aw')
    if not field:
        return False
    cid = ctx.user_data.pop('aw_id', None)
    ctx.user_data.pop('aw')
    text = (upd.message.text or '').strip()

    # Время после даты без времени
    if field == 'time_for_date':
        if not cid:
            return False
        c = db_get(cid)
        if not c:
            return False
        d = ctx.user_data.pop(f'aw_time_{cid}', None)
        _, t = extract_date_time(text)
        c['date'] = d
        if t: c['time'] = t
        db_save(c); sheets.sync_concert(c)
        await notify_ready(ctx, c)
        kb = [[InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_menu_{cid}")]]
        dt = f"{d} {t or ''}".strip()
        await upd.message.reply_text(
            f"✅ Дата *{c['artist']}*: `{dt}`",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown'
        )
        return True

    if not cid:
        return False
    c = db_get(cid)
    if not c:
        return False

    if field == 'date':
        d, t = extract_date_time(text)
        if d:
            c['date'] = d
            if t:
                c['time'] = t
                db_save(c); sheets.sync_concert(c)
                await notify_ready(ctx, c)
                kb = [[InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_menu_{cid}")]]
                await upd.message.reply_text(
                    f"✅ *{c['artist']}* — сохранено",
                    reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown'
                )
            else:
                # Спросить время
                ctx.user_data[f'aw_time_{cid}'] = d
                ctx.user_data['aw']    = 'time_for_date'
                ctx.user_data['aw_id'] = cid
                kb = [[InlineKeyboardButton("Пропустить", callback_data=f"do|date|{cid}")]]
                ctx.user_data[f'v_{cid}'] = (d, None)
                await upd.message.reply_text(
                    f"Дата `{d}` — введи время (например `21:00`) или пропусти:",
                    reply_markup=InlineKeyboardMarkup(kb)
                )
        return True

    elif field == 'tickets':
        c['tickets_url'] = extract_url(text) or text
    elif field == 'text':
        c['description_text'] = text
    elif field == 'artist':
        c['artist'] = text
    else:
        return False

    db_save(c); sheets.sync_concert(c)
    await notify_ready(ctx, c)
    kb = [[InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_menu_{cid}")]]
    await upd.message.reply_text(
        f"✅ *{c['artist']}* — сохранено",
        reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown'
    )
    return True

# ─── КОМАНДЫ ──────────────────────────────────────────────────────────────────

async def cmd_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    register_chat(upd.effective_chat.id)
    await upd.message.reply_text(
        "🎸 *MTB Concerts Manager*\n\n"
        "*Триггеры (любой порядок слов):*\n"
        "`Иван Дорн билеты https://...`\n"
        "`Иван Дорн афиша одобрена`\n"
        "`Иван Дорн текст Описание...`\n"
        "`Иван Дорн дата 15.04.2026 21:00`\n"
        "`Иван Дорн отмена`\n\n"
        "*Или просто напиши имя + дату:*\n"
        "`Иван Дорн 15.04.2026 21:00`\n\n"
        "*Команды:*\n"
        "`/new [Артист дата время]` — новое мероприятие\n"
        "`/edit` — список для редактирования\n"
        "`/edit [номер]` — редактировать конкретное\n"
        "`/list` — все в работе\n"
        "`/list afisha` | `tickets` | `text` — фильтр\n"
        "`/list 2026-04` — по месяцу\n"
        "`/status [номер]` — карточка\n"
        "`/publish [номер]` — опубликовать\n"
        "`/cancel [номер]` — отменить\n"
        "`/digest` — сводка\n"
        "`/code [номер]` — HTML для Tilda",
        parse_mode='Markdown'
    )


async def cmd_new(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    register_chat(upd.effective_chat.id)
    args = ' '.join(ctx.args).strip() if ctx.args else ''

    if not args:
        ctx.user_data['aw']    = 'create_name'
        ctx.user_data['aw_id'] = 0
        await upd.message.reply_text(
            "Введи имя артиста (можно сразу с датой и временем):\n"
            "Например: `Иван Дорн 15.04.2026 21:00`",
            parse_mode='Markdown'
        )
        return

    # Парсим любой порядок: дата, время, имя
    d, t    = extract_date_time(args)
    artist  = strip_date_time(args).strip()

    if not artist:
        await upd.message.reply_text("Не понял имя артиста. Попробуй: `/new Иван Дорн 15.04.2026`",
                                     parse_mode='Markdown')
        return

    await _create_or_warn(upd, ctx, artist, d, t)


async def _create_or_warn(upd: Update, ctx: ContextTypes.DEFAULT_TYPE,
                           artist: str, d: Optional[str], t: Optional[str]):
    """Проверяет дубли и создаёт или предупреждает."""
    msg     = upd.effective_message
    matches = fuzzy_find(artist)
    exact   = [c for c in matches if norm(c['artist']) == norm(artist)]

    if exact:
        c  = exact[0]
        kb = [[
            InlineKeyboardButton("Создать новое", callback_data=f"fc|{artist}|{d or ''}|{t or ''}"),
            InlineKeyboardButton(f"Открыть #{c['id']}", callback_data=f"edit_menu_{c['id']}"),
        ]]
        await msg.reply_text(
            f"*{artist}* уже есть (#{c['id']}).\nЧто делаем?",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown'
        )
        return

    cid = db_save({'artist': artist, 'date': d, 'time': t})
    c   = db_get(cid)

    # Если дата есть но времени нет — спросить время
    if d and not t:
        ctx.user_data[f'aw_time_{cid}'] = d
        ctx.user_data['aw']    = 'time_for_date'
        ctx.user_data['aw_id'] = cid
        ctx.user_data[f'v_{cid}'] = (d, None)
        kb = [[InlineKeyboardButton("Пропустить", callback_data=f"do|date|{cid}")]]
        await msg.reply_text(
            card(c) + f"\n\n📅 Введи время (например `21:00`) или пропусти:",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown'
        )
        return

    kb = [[InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_menu_{cid}")]]
    await msg.reply_text(card(c), reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')


async def cmd_edit(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # /edit 5 — сразу открыть карточку
    if ctx.args:
        try:
            cid = int(ctx.args[0])
            c   = db_get(cid)
            if not c:
                await upd.message.reply_text(f"#{cid} не найдено")
                return
            kb = [[InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_menu_{cid}")]]
            await upd.message.reply_text(card(c), reply_markup=InlineKeyboardMarkup(kb),
                                         parse_mode='Markdown')
            return
        except ValueError:
            # Попробуем как имя
            name    = ' '.join(ctx.args)
            matches = fuzzy_find(name)
            if len(matches) == 1:
                c  = matches[0]
                kb = [[InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_menu_{c['id']}")]]
                await upd.message.reply_text(card(c), reply_markup=InlineKeyboardMarkup(kb),
                                             parse_mode='Markdown')
                return
            elif len(matches) > 1:
                kb = [[InlineKeyboardButton(
                    f"#{c['id']} {c['artist']}" + (f" • {c['date']}" if c.get('date') else ''),
                    callback_data=f"edit_menu_{c['id']}"
                )] for c in matches]
                await upd.message.reply_text("Уточни:", reply_markup=InlineKeyboardMarkup(kb))
                return

    # /edit без аргументов — показать список
    concerts = [c for c in db_all() if c['status'] == 'draft']
    if not concerts:
        await upd.message.reply_text("Нет активных мероприятий. Создай: `/new`", parse_mode='Markdown')
        return
    kb = [[InlineKeyboardButton(
        f"{s_icon(c)} #{c['id']} {c['artist']}" + (f" • {c['date']}" if c.get('date') else ''),
        callback_data=f"edit_menu_{c['id']}"
    )] for c in concerts[:15]]
    await upd.message.reply_text(
        "Выбери мероприятие (или `/edit [номер]`):",
        reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown'
    )


async def cmd_list(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    register_chat(upd.effective_chat.id)
    arg = ctx.args[0].lower() if ctx.args else ''

    # Фильтры по отсутствующим полям
    FILTERS = {
        'afisha': ('афиша', lambda c: c.get('poster_status') != 'approved'),
        'афиша':  ('афиша', lambda c: c.get('poster_status') != 'approved'),
        'tickets': ('билеты', lambda c: not c.get('tickets_url')),
        'билеты':  ('билеты', lambda c: not c.get('tickets_url')),
        'text':   ('текст',  lambda c: not c.get('description_text')),
        'текст':  ('текст',  lambda c: not c.get('description_text')),
    }

    if arg in FILTERS:
        label, fn = FILTERS[arg]
        concerts  = [c for c in db_all() if c['status'] == 'draft' and fn(c)]
        if not concerts:
            await upd.message.reply_text(f"✅ У всех мероприятий есть {label}!")
            return
        lines = [f"❌ *Нет {label}: {len(concerts)}*\n"]
        for c in concerts:
            d = f" — {c['date']}" if c.get('date') else ''
            lines.append(f"#{c['id']} {c['artist']}{d}")
        await upd.message.reply_text('\n'.join(lines), parse_mode='Markdown')
        return

    # Фильтр по месяцу YYYY-MM
    month_filter = arg if re.match(r'^\d{4}-\d{2}$', arg) else None
    inc_all      = arg == 'all'
    concerts     = db_all(include_cancelled=inc_all)

    if month_filter:
        def in_month(c):
            d = c.get('date', '')
            try: return f"{d.split('.')[2]}-{d.split('.')[1]}" == month_filter
            except: return False
        concerts = [c for c in concerts if in_month(c)]

    if not concerts:
        await upd.message.reply_text("Мероприятий нет. Создай: `/new`", parse_mode='Markdown')
        return

    active    = [c for c in concerts if c['status'] == 'draft']
    published = [c for c in concerts if c['status'] == 'published']
    cancelled = [c for c in concerts if c['status'] == 'cancelled']

    lines = [f"📋 *В работе: {len(active)}*\n"]
    for c in active:
        d    = f" — {c['date']}" if c.get('date') else ''
        m    = missing(c)
        miss = f" | нет: {', '.join(m)}" if m else " | ✅"
        lines.append(f"{s_icon(c)} #{c['id']} {c['artist']}{d}{miss}")

    if published:
        lines.append(f"\n⚫ Опубликовано: {len(published)}")
        for c in published[:5]: lines.append(f"  #{c['id']} {c['artist']}")

    if inc_all and cancelled:
        lines.append(f"\n🚫 Отменены: {len(cancelled)}")
        for c in cancelled: lines.append(f"  #{c['id']} {c['artist']}")

    await upd.message.reply_text('\n'.join(lines), parse_mode='Markdown')


async def cmd_status(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = None
    if ctx.args:
        try:
            cid = int(ctx.args[0])
        except ValueError:
            name = ' '.join(ctx.args)
            m    = fuzzy_find(name)
            if len(m) == 1:
                cid = m[0]['id']
            elif len(m) > 1:
                kb = [[InlineKeyboardButton(
                    f"#{c['id']} {c['artist']}" + (f" • {c['date']}" if c.get('date') else ''),
                    callback_data=f"edit_menu_{c['id']}"
                )] for c in m]
                await upd.message.reply_text("Уточни:", reply_markup=InlineKeyboardMarkup(kb))
                return
    if not cid:
        await upd.message.reply_text("Укажи номер: `/status 5`", parse_mode='Markdown')
        return
    c = db_get(cid)
    if not c:
        await upd.message.reply_text(f"#{cid} не найдено")
        return
    kb = [[InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_menu_{cid}")]]
    await upd.message.reply_text(card(c), reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')


async def cmd_publish(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await upd.message.reply_text("Укажи номер: `/publish 5`", parse_mode='Markdown')
        return
    try: cid = int(ctx.args[0])
    except: await upd.message.reply_text("Неверный номер"); return
    c = db_get(cid)
    if not c: await upd.message.reply_text(f"#{cid} не найдено"); return
    kb = [[InlineKeyboardButton("✅ Опубликовать", callback_data=f"do|publish|{cid}"),
           InlineKeyboardButton("❌ Отмена",       callback_data="noop")]]
    await upd.message.reply_text(
        f"Опубликовать *#{cid} {c['artist']}*?",
        reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown'
    )


async def cmd_cancel(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await upd.message.reply_text("Укажи номер: `/cancel 5`", parse_mode='Markdown')
        return
    try: cid = int(ctx.args[0])
    except: await upd.message.reply_text("Неверный номер"); return
    c = db_get(cid)
    if not c: await upd.message.reply_text(f"#{cid} не найдено"); return
    kb = [[InlineKeyboardButton("✅ Отменить", callback_data=f"do|cancel|{cid}"),
           InlineKeyboardButton("❌ Назад",    callback_data="noop")]]
    await upd.message.reply_text(
        f"Отменить *#{cid} {c['artist']}*?",
        reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown'
    )


async def cmd_digest(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    register_chat(upd.effective_chat.id)
    all_c = db_all()
    ready, prog, draft, pub = [], [], [], []
    for c in all_c:
        if c['status'] == 'published':   pub.append(c)
        elif is_ready(c):                ready.append(c)
        elif any([c.get('date'), c.get('tickets_url'),
                  c.get('poster_status') == 'approved',
                  c.get('description_text')]): prog.append(c)
        else:                            draft.append(c)

    if not any([ready, prog, draft]):
        await upd.message.reply_text("Активных мероприятий нет.")
        return

    now = datetime.now()
    lines = [f"📊 *Статус на {now.strftime('%d.%m.%Y')}*\n"]
    if ready:
        lines.append(f"🟢 READY ({len(ready)})")
        for c in ready: lines.append(f"— {c['artist']}" + (f" • {c['date']}" if c.get('date') else ''))
        lines.append("")
    if prog:
        lines.append(f"🟡 IN PROGRESS ({len(prog)})")
        for c in prog: lines.append(f"— {c['artist']} (нет: {', '.join(missing(c))})")
        lines.append("")
    if draft:
        lines.append(f"🔴 DRAFT ({len(draft)})")
        for c in draft: lines.append(f"— {c['artist']}")
        lines.append("")
    if pub:
        lines.append(f"⚫ PUBLISHED ({len(pub)})")
        for c in pub[:5]: lines.append(f"— {c['artist']}")

    await upd.message.reply_text('\n'.join(lines), parse_mode='Markdown')


async def cmd_code(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await upd.message.reply_text("Укажи номер: `/code 5`", parse_mode='Markdown')
        return
    try: cid = int(ctx.args[0])
    except: await upd.message.reply_text("Неверный номер"); return
    c = db_get(cid)
    if not c: await upd.message.reply_text(f"#{cid} не найдено"); return

    artist     = c.get('artist', '')
    date_str   = c.get('date', '') or ''
    time_str   = c.get('time', '') or ''
    dt         = (date_str + ' • ' + time_str).strip(' •') if date_str else ''
    url        = c.get('tickets_url', '') or ''
    poster_url = c.get('poster_file_id', '') or 'ССЫЛКА_НА_АФИШУ'
    desc       = c.get('description_text', '') or ''

    paragraphs = [p.strip() for p in desc.split('\n\n') if p.strip()]
    first_para = paragraphs[0] if paragraphs else desc
    rest_paras = '<br><br>'.join(paragraphs[1:]) if len(paragraphs) > 1 else ''

    html_lines = [
        '<div class="event-wrapper">',
        '    <button class="back-btn" onclick="goBackSafe(); return false;">',
        '        <span class="arrow-left">←</span>',
        '        <span>Назад</span>',
        '    </button>',
        '    <div class="event-image">',
        f'        <img src="{poster_url}" alt="{artist}">',
        '    </div>',
        '    <div class="event-content">',
        f'        <h1 class="event-title">{artist.upper()}</h1>',
        f'        <div class="event-datetime">{dt}</div>',
        '        <div class="buttons-row">',
        f'            <button class="buy-btn" onclick="window.open(\'{url}\', \'_blank\')">',
        '                Купить билет',
        '            </button>',
        '        </div>',
        '        <div class="text-container" id="textContainer">',
        '            <div class="text-scroll-zone" id="textZone">',
        f'                <p class="text-preview">{first_para}</p>',
        f'                <p class="full-text">{rest_paras}</p>',
        '            </div>',
        '            <div class="toggle-btn-wrapper" id="toggleWrapper" style="display: none;">',
        '                <button class="toggle-btn" onclick="toggleText()">',
        '                    <span class="btn-text">Читать далее</span>',
        '                    <span class="arrow">▼</span>',
        '                </button>',
        '            </div>',
        '        </div>',
        '    </div>',
        '</div>',
    ]
    html = '\n'.join(html_lines)

    m = missing(c)
    warnings = []
    if 'афиша'  in m: warnings.append('⚠️ Афиша не добавлена — замени `ССЫЛКА_НА_АФИШУ`')
    if 'билеты' in m: warnings.append('⚠️ Билеты не добавлены')
    if 'текст'  in m: warnings.append('⚠️ Текст не добавлен')
    if 'дата'   in m: warnings.append('⚠️ Дата не установлена')

    header = f'🎤 *{artist}* — код для Tilda Zero Block'
    if warnings:
        header += '\n\n' + '\n'.join(warnings)
    header += f'\n\nПосле публикации → `/publish {cid}`'

    await upd.message.reply_text(header, parse_mode='Markdown')
    msg = f'```html\n{html}\n```'
    if len(msg) <= 4096:
        await upd.message.reply_text(msg, parse_mode='Markdown')
    else:
        await upd.message.reply_text(f'```html\n{html[:3800]}\n```', parse_mode='Markdown')
        await upd.message.reply_text(f'```html\n{html[3800:]}\n```', parse_mode='Markdown')


# ─── ОБРАБОТЧИК ТЕКСТА ────────────────────────────────────────────────────────

async def on_text(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = (upd.message.text or '').strip()

    # Ожидание имени при /new
    if ctx.user_data.get('aw') == 'create_name':
        ctx.user_data.pop('aw'); ctx.user_data.pop('aw_id', None)
        if text:
            d, t   = extract_date_time(text)
            artist = strip_date_time(text).strip() or text
            await _create_or_warn(upd, ctx, artist, d, t)
        return

    # Ожидание ввода из редактора
    if await handle_awaiting(upd, ctx):
        return

    # Триггеры (Артист + действие)
    parsed = parse_trigger(text)
    if parsed:
        await process_trigger(upd, ctx, parsed)
        return

    # Свободный ввод: Имя + дата → предложить создать
    free = parse_free_text(text)
    if free:
        artist  = free['artist']
        d, t    = free['date'], free['time']
        matches = fuzzy_find(artist)
        if not matches:
            kb = [[
                InlineKeyboardButton("✅ Создать", callback_data=f"new_confirm|{artist}|{d or ''}|{t or ''}"),
                InlineKeyboardButton("❌ Отмена",  callback_data="noop"),
            ]]
            dt = f"{d} {t or ''}".strip()
            await upd.message.reply_text(
                f"Создать мероприятие *{artist}* на `{dt}`?",
                reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown'
            )


# ─── УТРЕННИЙ ДАЙДЖЕСТ ────────────────────────────────────────────────────────

async def morning_digest(ctx: ContextTypes.DEFAULT_TYPE):
    all_c = db_all()
    ready, prog, draft = [], [], []
    for c in all_c:
        if c['status'] != 'draft': continue
        if is_ready(c): ready.append(c)
        elif any([c.get('date'), c.get('tickets_url'),
                  c.get('poster_status') == 'approved',
                  c.get('description_text')]): prog.append(c)
        else: draft.append(c)

    if not any([ready, prog, draft]):
        return

    now = datetime.now()
    lines = [f"📊 *Статус на {now.strftime('%d.%m.%Y')}*\n"]
    if ready:
        lines.append(f"🟢 READY ({len(ready)})")
        for c in ready: lines.append(f"— {c['artist']}" + (f" • {c['date']}" if c.get('date') else ''))
        lines.append("")
    if prog:
        lines.append(f"🟡 IN PROGRESS ({len(prog)})")
        for c in prog: lines.append(f"— {c['artist']} (нет: {', '.join(missing(c))})")
        lines.append("")
    if draft:
        lines.append(f"🔴 DRAFT ({len(draft)})")
        for c in draft: lines.append(f"— {c['artist']}")

    text = '\n'.join(lines)
    for chat_id in get_chats():
        try:
            await ctx.bot.send_message(chat_id, text, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"digest {chat_id}: {e}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    init_db()
    app = Application.builder().token(TOKEN).build()

    for cmd, fn in [
        ('start',   cmd_start),
        ('new',     cmd_new),
        ('edit',    cmd_edit),
        ('list',    cmd_list),
        ('status',  cmd_status),
        ('publish', cmd_publish),
        ('cancel',  cmd_cancel),
        ('digest',  cmd_digest),
        ('code',    cmd_code),
        ('help',    cmd_start),
    ]:
        app.add_handler(CommandHandler(cmd, fn))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_handler(CallbackQueryHandler(on_callback))

    jq = app.job_queue
    if jq:
        jq.run_daily(morning_digest, time=dtime(hour=9, minute=0))

    logger.info("🎸 MTB Concerts Bot v5 запущен!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
