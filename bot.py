#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MTB Concerts Bot — v3
Логика:
- Триггеры одной строкой: "Артист билеты https://..." / "Артист афиша одобрена" / "Артист текст ..." / "Артист дата 15.03"
- Подтверждение Да/Нет после каждого действия
- Если артист не найден → предложить создать
- /menu — кнопки для всего
- /status [id или имя] — карточка как в макете
- В группе бот реагирует ТОЛЬКО на триггеры
"""

import os
import re
import logging
import sqlite3
from datetime import datetime, timedelta, time as dtime
from typing import Optional, Dict, Any, List, Tuple

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)
from rapidfuzz import fuzz
from google_sheets import GoogleSheetsManager

# ==================== НАСТРОЙКИ ====================

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '')
SHEETS_ID      = os.getenv('GOOGLE_SHEETS_ID', '')
OWNER_ID       = int(os.getenv('OWNER_ID', '534303997'))

DB_PATH = 'concerts.db'
sheets  = GoogleSheetsManager(spreadsheet_id=SHEETS_ID if SHEETS_ID else None)

FUZZY_THRESHOLD = 70

# Ключевые слова триггеров
TRIGGER_TICKETS  = ['билеты', 'ticket', 'tickets']
TRIGGER_POSTER   = ['афиша', 'poster']
POSTER_APPROVED  = ['одобрена', 'ок', 'ok', 'утверждена', 'готова', 'approved']
TRIGGER_TEXT     = ['текст', 'описание', 'text', 'description']
TRIGGER_DATE     = ['дата', 'date', 'перенос']
TRIGGER_CANCEL   = ['отмена', 'отменён', 'отменен', 'отменили', 'cancelled', 'canceled']

# ==================== HELPERS ====================

def normalize(text: str) -> str:
    text = text.lower().replace('ё', 'е')
    text = re.sub(r'[^\w\s]', '', text)
    return re.sub(r'\s+', ' ', text).strip()

def is_group(update: Update) -> bool:
    return update.effective_chat.type in ('group', 'supergroup')

def extract_urls(text: str) -> List[str]:
    return re.findall(r'https?://[^\s<>"\']+', text)

def parse_date_time(text: str) -> Tuple[Optional[str], Optional[str]]:
    months_ru = {
        'января':1,'февраля':2,'марта':3,'апреля':4,'мая':5,'июня':6,
        'июля':7,'августа':8,'сентября':9,'октября':10,'ноября':11,'декабря':12
    }
    date_str = time_str = None
    text_low = text.lower()

    m = re.search(r'(\d{1,2})[./\-](\d{1,2})[./\-](\d{4})', text)
    if m:
        d, mo, y = m.groups()
        date_str = f"{int(d):02d}.{int(mo):02d}.{y}"

    if not date_str:
        pat = r'(\d{1,2})\s+(' + '|'.join(months_ru) + r')(?:\s+(\d{4}))?'
        m = re.search(pat, text_low)
        if m:
            d  = m.group(1)
            mo = months_ru[m.group(2)]
            y  = m.group(3) or str(datetime.now().year)
            date_str = f"{int(d):02d}.{mo:02d}.{y}"

    m = re.search(r'\b(\d{1,2})[:\.](\d{2})\b', text)
    if m:
        h, mi = m.groups()
        if 0 <= int(h) <= 23:
            time_str = f"{int(h):02d}:{int(mi):02d}"

    return date_str, time_str

# ==================== БД ====================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS concerts (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        artist           TEXT NOT NULL,
        date             TEXT,
        time             TEXT,
        city             TEXT,
        poster_status    TEXT DEFAULT 'none',
        poster_file_id   TEXT,
        tickets_url      TEXT,
        description_text TEXT,
        published_status TEXT DEFAULT 'draft',
        created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS reminders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        concert_id INTEGER NOT NULL,
        remind_at  TEXT NOT NULL,
        sent       INTEGER DEFAULT 0
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS digest_chats (
        chat_id INTEGER PRIMARY KEY
    )''')
    conn.commit()
    conn.close()

def save_concert(data: Dict[str, Any]) -> int:
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    now  = datetime.now().isoformat()
    if data.get('id'):
        c.execute('''UPDATE concerts SET
            artist=?,date=?,time=?,city=?,
            poster_status=?,poster_file_id=?,
            tickets_url=?,description_text=?,
            published_status=?,updated_at=?
            WHERE id=?''', (
            data.get('artist'), data.get('date'), data.get('time'), data.get('city'),
            data.get('poster_status','none'), data.get('poster_file_id'),
            data.get('tickets_url'), data.get('description_text'),
            data.get('published_status','draft'), now, data['id']
        ))
        cid = data['id']
    else:
        c.execute('''INSERT INTO concerts
            (artist,date,time,city,poster_status,poster_file_id,
             tickets_url,description_text,published_status)
            VALUES (?,?,?,?,?,?,?,?,?)''', (
            data.get('artist'), data.get('date'), data.get('time'), data.get('city'),
            data.get('poster_status','none'), data.get('poster_file_id'),
            data.get('tickets_url'), data.get('description_text'),
            data.get('published_status','draft')
        ))
        cid = c.lastrowid
    conn.commit()
    conn.close()
    return cid

def get_concert(cid: int) -> Optional[Dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('SELECT * FROM concerts WHERE id=?', (cid,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None

def get_all_concerts(include_cancelled=False) -> List[Dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    if include_cancelled:
        c.execute('SELECT * FROM concerts ORDER BY date ASC, created_at DESC')
    else:
        c.execute("SELECT * FROM concerts WHERE published_status != 'cancelled' ORDER BY date ASC, created_at DESC")
    rows = c.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def delete_concert(cid: int):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('DELETE FROM concerts WHERE id=?', (cid,))
    c.execute('DELETE FROM reminders WHERE concert_id=?', (cid,))
    conn.commit()
    conn.close()

def register_chat(chat_id: int):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('INSERT OR IGNORE INTO digest_chats (chat_id) VALUES (?)', (chat_id,))
    conn.commit()
    conn.close()

def get_digest_chats() -> List[int]:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT chat_id FROM digest_chats')
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

# ==================== СТАТУСЫ ====================

def is_ready(concert: Dict) -> bool:
    return all([
        concert.get('date'),
        concert.get('poster_status') == 'approved',
        concert.get('tickets_url'),
        concert.get('description_text'),
    ])

def missing_fields(concert: Dict) -> List[str]:
    m = []
    if not concert.get('date'):                        m.append('дата')
    if concert.get('poster_status') != 'approved':     m.append('афиша')
    if not concert.get('tickets_url'):                 m.append('билеты')
    if not concert.get('description_text'):            m.append('текст')
    return m

def status_icon(concert: Dict) -> str:
    ps = concert.get('published_status', 'draft')
    if ps == 'cancelled':  return '🚫'
    if ps == 'published':  return '⚫'
    if is_ready(concert):  return '🟢'
    filled = sum([
        bool(concert.get('date')),
        concert.get('poster_status') == 'approved',
        bool(concert.get('tickets_url')),
        bool(concert.get('description_text')),
    ])
    return '🟡' if filled >= 2 else '🔴'

def concert_card(concert: Dict) -> str:
    icon      = status_icon(concert)
    date_line = f"{concert['date']} {concert.get('time','') or ''}".strip() if concert.get('date') else '—'
    city      = concert.get('city') or '—'
    m         = missing_fields(concert)

    poster_icon  = '✅' if concert.get('poster_status') == 'approved' else '❌'
    tickets_icon = '✅' if concert.get('tickets_url') else '❌'
    text_icon    = '✅' if concert.get('description_text') else '❌'
    date_icon    = '✅' if concert.get('date') else '❌'

    text = (
        f"{icon} *#{concert['id']} {concert['artist']}*\n"
        f"📅 {date_line} | 🏙 {city}\n\n"
        f"{poster_icon} Афиша\n"
        f"{tickets_icon} Билеты\n"
        f"{text_icon} Текст\n"
        f"{date_icon} Дата\n"
    )
    if m:
        text += f"\n❗ Не хватает: {', '.join(m)}"
    else:
        text += f"\n🟢 Готов к публикации"
    return text

async def notify_owner_if_ready(context: ContextTypes.DEFAULT_TYPE, concert: Dict):
    if is_ready(concert) and concert.get('published_status') == 'draft':
        try:
            await context.bot.send_message(
                chat_id=OWNER_ID,
                text=(
                    f"🎤 *{concert['artist']}* — полностью готов!\n"
                    f"ID: {concert['id']}\n\n"
                    f"`/code {concert['id']}`"
                ),
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"notify_owner: {e}")

# ==================== FUZZY ПОИСК ====================

def fuzzy_find(name: str, include_cancelled=False) -> List[Dict]:
    concerts  = get_all_concerts(include_cancelled=include_cancelled)
    name_norm = normalize(name)
    results   = []
    for c in concerts:
        score = fuzz.token_set_ratio(name_norm, normalize(c['artist']))
        if score >= FUZZY_THRESHOLD:
            results.append((c, score))
    if not results:
        return []
    results.sort(key=lambda x: x[1], reverse=True)
    top = results[0][1]
    if top == 100 or (len(results) >= 2 and top - results[1][1] >= 20):
        return [results[0][0]]
    return [r[0] for r in results[:5]]

# ==================== ПАРСИНГ ТРИГГЕРОВ ====================

def parse_trigger(text: str) -> Optional[Dict]:
    """
    Разбирает сообщение на: artist_name, trigger_type, payload
    Форматы:
      Артист билеты https://...
      Артист афиша одобрена
      Артист текст <описание>
      Артист дата 15.03.2026 23:00
      Артист отмена
    Возвращает dict или None если не распознано.
    """
    text = text.strip()
    words = text.split()
    if len(words) < 2:
        return None

    text_low = normalize(text)

    # Ищем позицию триггерного слова
    all_triggers = TRIGGER_TICKETS + TRIGGER_POSTER + TRIGGER_TEXT + TRIGGER_DATE + TRIGGER_CANCEL
    trigger_pos  = None
    trigger_word = None
    trigger_type = None

    for i, word in enumerate(words):
        w = normalize(word)
        if w in [normalize(t) for t in TRIGGER_TICKETS]:
            trigger_pos, trigger_word, trigger_type = i, word, 'tickets'
            break
        if w in [normalize(t) for t in TRIGGER_POSTER]:
            trigger_pos, trigger_word, trigger_type = i, word, 'poster'
            break
        if w in [normalize(t) for t in TRIGGER_TEXT]:
            trigger_pos, trigger_word, trigger_type = i, word, 'text'
            break
        if w in [normalize(t) for t in TRIGGER_DATE]:
            trigger_pos, trigger_word, trigger_type = i, word, 'date'
            break
        if w in [normalize(t) for t in TRIGGER_CANCEL]:
            trigger_pos, trigger_word, trigger_type = i, word, 'cancel'
            break

    if trigger_pos is None or trigger_pos == 0:
        return None

    artist_name = ' '.join(words[:trigger_pos]).strip()
    payload     = ' '.join(words[trigger_pos+1:]).strip()

    if not artist_name:
        return None

    return {
        'artist_name':  artist_name,
        'trigger_type': trigger_type,
        'payload':      payload,
    }

# ==================== ОБРАБОТКА ТРИГГЕРА ====================

async def process_trigger(update: Update, context: ContextTypes.DEFAULT_TYPE, parsed: Dict):
    message     = update.effective_message
    artist_name = parsed['artist_name']
    ttype       = parsed['trigger_type']
    payload     = parsed['payload']

    # Ищем артиста
    matches = fuzzy_find(artist_name)

    if len(matches) == 0:
        # Артист не найден — предлагаем создать
        kb = [[
            InlineKeyboardButton("✅ Создать", callback_data=f"create_new_{artist_name}_{ttype}_{payload[:50]}"),
            InlineKeyboardButton("❌ Отмена",  callback_data="noop"),
        ]]
        await message.reply_text(
            f"Мероприятие для *{artist_name}* не найдено.\nСоздать новое?",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )
        return

    if len(matches) > 1:
        # Несколько совпадений — уточнить
        kb = [[InlineKeyboardButton(
            f"#{c['id']} {c['artist']}",
            callback_data=f"trigger_select_{c['id']}_{ttype}_{payload[:50]}"
        )] for c in matches[:5]]
        kb.append([InlineKeyboardButton("❌ Отмена", callback_data='noop')])
        await message.reply_text("Уточни:", reply_markup=InlineKeyboardMarkup(kb))
        return

    concert = matches[0]
    await apply_trigger(update, context, concert, ttype, payload)


async def apply_trigger(update: Update, context: ContextTypes.DEFAULT_TYPE,
                         concert: Dict, ttype: str, payload: str):
    message = update.effective_message

    if ttype == 'cancel':
        kb = [[
            InlineKeyboardButton("✅ Да", callback_data=f"confirm_cancel_{concert['id']}"),
            InlineKeyboardButton("❌ Нет", callback_data="noop"),
        ]]
        await message.reply_text(
            f"Отменить *{concert['artist']}*?",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )
        return

    if ttype == 'tickets':
        urls = extract_urls(payload)
        if not urls:
            await message.reply_text("Не нашёл ссылку. Пример: `Иван Дорн билеты https://...`", parse_mode='Markdown')
            return
        url = urls[0]
        # Проверяем перезапись
        if concert.get('tickets_url'):
            kb = [[
                InlineKeyboardButton("✅ Перезаписать", callback_data=f"confirm_tickets_{concert['id']}_{url[:80]}"),
                InlineKeyboardButton("❌ Отмена", callback_data="noop"),
            ]]
            await message.reply_text(
                f"Билеты уже есть для *{concert['artist']}*.\nПерезаписать?",
                reply_markup=InlineKeyboardMarkup(kb),
                parse_mode='Markdown'
            )
        else:
            kb = [[
                InlineKeyboardButton("✅ Да", callback_data=f"confirm_tickets_{concert['id']}_{url[:80]}"),
                InlineKeyboardButton("❌ Нет", callback_data="noop"),
            ]]
            await message.reply_text(
                f"Добавить билеты для *{concert['artist']}*?",
                reply_markup=InlineKeyboardMarkup(kb),
                parse_mode='Markdown'
            )
        return

    if ttype == 'poster':
        payload_low = normalize(payload)
        approved = any(normalize(w) in payload_low for w in POSTER_APPROVED)
        if not approved:
            await message.reply_text(
                f"Напиши: `{concert['artist']} афиша одобрена`",
                parse_mode='Markdown'
            )
            return
        kb = [[
            InlineKeyboardButton("✅ Да", callback_data=f"confirm_poster_{concert['id']}"),
            InlineKeyboardButton("❌ Нет", callback_data="noop"),
        ]]
        await message.reply_text(
            f"Отметить афишу *{concert['artist']}* как одобренную?",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )
        return

    if ttype == 'text':
        if not payload or len(payload) < 10:
            await message.reply_text(
                f"Укажи текст: `{concert['artist']} текст <описание>`",
                parse_mode='Markdown'
            )
            return
        # Сохраняем payload в user_data для подтверждения
        context.user_data[f'pending_text_{concert["id"]}'] = payload
        kb = [[
            InlineKeyboardButton("✅ Да", callback_data=f"confirm_text_{concert['id']}"),
            InlineKeyboardButton("❌ Нет", callback_data="noop"),
        ]]
        preview = payload[:80] + ('...' if len(payload) > 80 else '')
        await message.reply_text(
            f"Добавить описание для *{concert['artist']}*?\n\n_{preview}_",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )
        return

    if ttype == 'date':
        date_str, time_str = parse_date_time(payload)
        if not date_str:
            await message.reply_text(
                f"Не распознал дату. Пример: `{concert['artist']} дата 15.03.2026 23:00`",
                parse_mode='Markdown'
            )
            return
        date_full = f"{date_str} {time_str or ''}".strip()
        context.user_data[f'pending_date_{concert["id"]}']     = date_str
        context.user_data[f'pending_time_{concert["id"]}']     = time_str
        kb = [[
            InlineKeyboardButton("✅ Да", callback_data=f"confirm_date_{concert['id']}"),
            InlineKeyboardButton("❌ Нет", callback_data="noop"),
        ]]
        await message.reply_text(
            f"Установить дату *{concert['artist']}*: `{date_full}`?",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )
        return


# ==================== CALLBACK КНОПКИ ====================

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data  = query.data

    # --- Подтверждение билетов ---
    if data.startswith('confirm_tickets_'):
        parts   = data.split('_', 3)
        cid     = int(parts[2])
        url     = parts[3] if len(parts) > 3 else ''
        concert = get_concert(cid)
        if concert:
            concert['tickets_url'] = url
            save_concert(concert)
            sheets.sync_concert(concert)
            await notify_owner_if_ready(context, concert)
            await query.edit_message_text(f"✅ Билеты добавлены — *{concert['artist']}*", parse_mode='Markdown')

    # --- Подтверждение афиши ---
    elif data.startswith('confirm_poster_'):
        cid     = int(data.split('_')[2])
        concert = get_concert(cid)
        if concert:
            concert['poster_status'] = 'approved'
            save_concert(concert)
            sheets.sync_concert(concert)
            await notify_owner_if_ready(context, concert)
            await query.edit_message_text(f"✅ Афиша одобрена — *{concert['artist']}*", parse_mode='Markdown')

    # --- Подтверждение текста ---
    elif data.startswith('confirm_text_'):
        cid     = int(data.split('_')[2])
        concert = get_concert(cid)
        text    = context.user_data.pop(f'pending_text_{cid}', None)
        if concert and text:
            concert['description_text'] = text
            save_concert(concert)
            sheets.sync_concert(concert)
            await notify_owner_if_ready(context, concert)
            await query.edit_message_text(f"✅ Текст добавлен — *{concert['artist']}*", parse_mode='Markdown')

    # --- Подтверждение даты ---
    elif data.startswith('confirm_date_'):
        cid     = int(data.split('_')[2])
        concert = get_concert(cid)
        d       = context.user_data.pop(f'pending_date_{cid}', None)
        t       = context.user_data.pop(f'pending_time_{cid}', None)
        if concert and d:
            concert['date'] = d
            if t: concert['time'] = t
            save_concert(concert)
            sheets.sync_concert(concert)
            await notify_owner_if_ready(context, concert)
            await query.edit_message_text(f"✅ Дата установлена — *{concert['artist']}*", parse_mode='Markdown')

    # --- Подтверждение отмены ---
    elif data.startswith('confirm_cancel_'):
        cid     = int(data.split('_')[2])
        concert = get_concert(cid)
        if concert:
            concert['published_status'] = 'cancelled'
            save_concert(concert)
            sheets.sync_concert(concert)
            await query.edit_message_text(f"🚫 *{concert['artist']}* — отменён", parse_mode='Markdown')

    # --- Подтверждение удаления ---
    elif data.startswith('confirm_delete_'):
        cid = int(data.split('_')[2])
        c   = get_concert(cid)
        if c:
            delete_concert(cid)
            await query.edit_message_text(f"🗑 *{c['artist']}* — удалён", parse_mode='Markdown')

    # --- Создать нового артиста ---
    elif data.startswith('create_new_'):
        parts       = data.split('_', 4)
        artist_name = parts[2]
        ttype       = parts[3] if len(parts) > 3 else None
        payload     = parts[4] if len(parts) > 4 else ''
        cid         = save_concert({'artist': artist_name})
        concert     = get_concert(cid)
        await query.edit_message_text(f"✅ Создано: *#{cid} {artist_name}*", parse_mode='Markdown')
        if ttype and ttype != 'None':
            await apply_trigger(update, context, concert, ttype, payload)

    # --- Выбор из нескольких совпадений ---
    elif data.startswith('trigger_select_'):
        parts   = data.split('_', 4)
        cid     = int(parts[2])
        ttype   = parts[3]
        payload = parts[4] if len(parts) > 4 else ''
        concert = get_concert(cid)
        await query.edit_message_text(f"#{cid} {concert['artist']}")
        await apply_trigger(update, context, concert, ttype, payload)

    # --- Меню редактирования ---
    elif data.startswith('edit_menu_'):
        cid = int(data.split('_')[2])
        c   = get_concert(cid)
        kb  = [
            [InlineKeyboardButton("📅 Дата/время", callback_data=f"menu_edit_date_{cid}"),
             InlineKeyboardButton("🏙 Город",       callback_data=f"menu_edit_city_{cid}")],
            [InlineKeyboardButton("🎟 Билеты",      callback_data=f"menu_edit_tickets_{cid}"),
             InlineKeyboardButton("🖼 Афиша ок",    callback_data=f"confirm_poster_{cid}")],
            [InlineKeyboardButton("📝 Текст",       callback_data=f"menu_edit_text_{cid}"),
             InlineKeyboardButton("✏️ Артист",      callback_data=f"menu_edit_artist_{cid}")],
            [InlineKeyboardButton("🚫 Отменить",    callback_data=f"confirm_cancel_{cid}"),
             InlineKeyboardButton("🗑 Удалить",     callback_data=f"confirm_delete_{cid}")],
        ]
        await query.edit_message_text(
            concert_card(c) + "\n\n_Что изменить?_",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )

    # --- Редактирование через меню (ввод текста) ---
    elif data.startswith('menu_edit_'):
        parts = data.split('_', 3)
        field = parts[2]
        cid   = int(parts[3])
        c     = get_concert(cid)
        context.user_data['awaiting']        = field
        context.user_data['awaiting_for_id'] = cid
        prompts = {
            'date':    f"📅 Введи дату для *{c['artist']}*:\nПример: `15.03.2026 23:00`",
            'city':    f"🏙 Введи город для *{c['artist']}*:",
            'tickets': f"🎟 Введи ссылку на билеты для *{c['artist']}*:",
            'text':    f"📝 Введи описание для *{c['artist']}*:",
            'artist':  f"✏️ Введи новое имя (сейчас: {c['artist']}):",
        }
        await query.edit_message_text(prompts.get(field, 'Введи значение:'), parse_mode='Markdown')

    # --- Опубликовать ---
    elif data.startswith('confirm_publish_'):
        cid     = int(data.split('_')[2])
        concert = get_concert(cid)
        if concert:
            concert['published_status'] = 'published'
            save_concert(concert)
            sheets.sync_concert(concert)
            await query.edit_message_text(f"⚫ *{concert['artist']}* — опубликован", parse_mode='Markdown')

    elif data == 'noop':
        await query.edit_message_text("Отменено")


# ==================== ОЖИДАНИЕ ВВОДА (меню) ====================

async def handle_awaiting(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    awaiting = context.user_data.get('awaiting')
    if not awaiting:
        return False
    cid     = context.user_data.pop('awaiting_for_id', None)
    context.user_data.pop('awaiting')
    if not cid:
        return False
    concert = get_concert(cid)
    if not concert:
        return False
    text = update.message.text or ''

    if awaiting == 'date':
        d, t = parse_date_time(text)
        if d: concert['date'] = d
        if t: concert['time'] = t
    elif awaiting == 'city':    concert['city'] = text
    elif awaiting == 'tickets':
        urls = extract_urls(text)
        concert['tickets_url'] = urls[0] if urls else text
    elif awaiting == 'text':    concert['description_text'] = text
    elif awaiting == 'artist':  concert['artist'] = text
    else:
        return False

    save_concert(concert)
    sheets.sync_concert(concert)
    await notify_owner_if_ready(context, concert)
    await update.message.reply_text(f"✅ *{concert['artist']}* — сохранено", parse_mode='Markdown')
    return True


# ==================== КОМАНДЫ ====================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_chat(update.effective_chat.id)
    await update.message.reply_text(
        "🎸 *MTB Concerts Manager*\n\n"
        "*Триггеры (пиши в чат):*\n"
        "`Артист билеты https://...`\n"
        "`Артист афиша одобрена`\n"
        "`Артист текст <описание>`\n"
        "`Артист дата 15.03.2026 23:00`\n"
        "`Артист отмена`\n\n"
        "*Команды:*\n"
        "/createtask — новое мероприятие\n"
        "/list — все в работе\n"
        "/status [номер] — карточка\n"
        "/publish [номер] — опубликовать\n"
        "/cancel [номер] — отменить\n"
        "/menu — управление\n"
        "/digest — сводка\n"
        "/help — помощь",
        parse_mode='Markdown'
    )


async def cmd_createtask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_chat(update.effective_chat.id)
    # Если передан аргумент — создать сразу
    args_text = ' '.join(context.args).strip() if context.args else ''
    if args_text:
        date_str, time_str = parse_date_time(args_text)
        cleaned = re.sub(r'\d{1,2}[./\-]\d{1,2}[./\-]\d{4}', '', args_text)
        cleaned = re.sub(r'\b\d{1,2}:\d{2}\b', '', cleaned).strip()
        parts   = cleaned.split()
        if len(parts) >= 2:
            city, artist = parts[-1], ' '.join(parts[:-1])
        elif len(parts) == 1:
            artist, city = parts[0], None
        else:
            artist, city = args_text, None
        cid = save_concert({'artist': artist, 'date': date_str, 'time': time_str, 'city': city})
        concert = get_concert(cid)
        kb = [[InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_menu_{cid}")]]
        await update.message.reply_text(
            concert_card(concert),
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )
    else:
        # Спрашиваем имя
        context.user_data['awaiting']        = 'create_artist'
        context.user_data['awaiting_for_id'] = 0
        await update.message.reply_text(
            "Введи имя артиста:",
            parse_mode='Markdown'
        )


async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_chat(update.effective_chat.id)
    # Фильтр по месяцу если передан
    month_filter = None
    if context.args:
        arg = context.args[0]
        if re.match(r'^\d{4}-\d{2}$', arg):
            month_filter = arg  # "2026-03"

    include_all = bool(context.args and context.args[0].lower() == 'all')
    concerts    = get_all_concerts(include_cancelled=include_all)

    if month_filter:
        # Фильтруем по месяцу (дата в формате DD.MM.YYYY)
        def in_month(c):
            d = c.get('date', '')
            if not d: return False
            try:
                parts = d.split('.')
                return f"{parts[2]}-{parts[1]}" == month_filter
            except: return False
        concerts = [c for c in concerts if in_month(c)]

    if not concerts:
        await update.message.reply_text("Мероприятий нет.")
        return

    active    = [c for c in concerts if c['published_status'] == 'draft']
    published = [c for c in concerts if c['published_status'] == 'published']
    cancelled = [c for c in concerts if c['published_status'] == 'cancelled']

    lines = [f"📋 *В работе: {len(active)}*\n"]
    for c in active:
        icon = status_icon(c)
        d    = f" — {c['date']}" if c.get('date') else ''
        city = f" ({c['city']})" if c.get('city') else ''
        m    = missing_fields(c)
        miss = f" | нет: {', '.join(m)}" if m else " | ✅"
        lines.append(f"{icon} #{c['id']} {c['artist']}{d}{city}{miss}")

    if published:
        lines.append(f"\n⚫ Опубликовано: {len(published)}")
        for c in published[:5]:
            lines.append(f"  #{c['id']} {c['artist']}")

    if include_all and cancelled:
        lines.append(f"\n🚫 Отменены: {len(cancelled)}")
        for c in cancelled:
            lines.append(f"  #{c['id']} {c['artist']}")

    await update.message.reply_text('\n'.join(lines), parse_mode='Markdown')


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = None
    if context.args:
        try:
            cid = int(context.args[0])
        except ValueError:
            # Попробуем как имя
            name    = ' '.join(context.args)
            matches = fuzzy_find(name)
            if matches:
                cid = matches[0]['id']

    if not cid:
        await update.message.reply_text("Укажи номер: `/status 5`", parse_mode='Markdown')
        return

    concert = get_concert(cid)
    if not concert:
        await update.message.reply_text(f"#{cid} не найдено")
        return

    kb = [[InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_menu_{cid}")]]
    await update.message.reply_text(
        concert_card(concert),
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode='Markdown'
    )


async def cmd_publish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Укажи номер: `/publish 5`", parse_mode='Markdown')
        return
    try: cid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Неверный номер")
        return

    concert = get_concert(cid)
    if not concert:
        await update.message.reply_text(f"#{cid} не найдено")
        return

    kb = [[
        InlineKeyboardButton("✅ Опубликовать", callback_data=f"confirm_publish_{cid}"),
        InlineKeyboardButton("❌ Отмена",       callback_data="noop"),
    ]]
    await update.message.reply_text(
        f"Опубликовать *#{cid} {concert['artist']}*?",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode='Markdown'
    )


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Укажи номер: `/cancel 5`", parse_mode='Markdown')
        return
    try: cid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Неверный номер")
        return

    concert = get_concert(cid)
    if not concert:
        await update.message.reply_text(f"#{cid} не найдено")
        return

    kb = [[
        InlineKeyboardButton("✅ Отменить", callback_data=f"confirm_cancel_{cid}"),
        InlineKeyboardButton("❌ Назад",    callback_data="noop"),
    ]]
    await update.message.reply_text(
        f"Отменить *#{cid} {concert['artist']}*?",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode='Markdown'
    )


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    concerts = get_all_concerts()
    active   = [c for c in concerts if c['published_status'] == 'draft']

    if not active:
        await update.message.reply_text("Нет активных мероприятий. Создай: `/createtask`", parse_mode='Markdown')
        return

    kb = [[InlineKeyboardButton(
        f"{status_icon(c)} #{c['id']} {c['artist']}",
        callback_data=f"edit_menu_{c['id']}"
    )] for c in active[:10]]

    await update.message.reply_text(
        "📋 Выбери мероприятие:",
        reply_markup=InlineKeyboardMarkup(kb)
    )


async def cmd_digest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_chat(update.effective_chat.id)
    all_c = get_all_concerts()

    ready, in_progress, draft_only, published = [], [], [], []
    for c in all_c:
        if c['published_status'] == 'published':    published.append(c)
        elif is_ready(c):                           ready.append(c)
        elif any([c.get('date'), c.get('tickets_url'),
                  c.get('poster_status') == 'approved',
                  c.get('description_text')]):      in_progress.append(c)
        else:                                       draft_only.append(c)

    if not ready and not in_progress and not draft_only:
        await update.message.reply_text("📊 Активных мероприятий нет.")
        return

    now   = datetime.now()
    lines = [f"📊 *Статус на {now.strftime('%d.%m.%Y')}*\n"]

    if ready:
        lines.append(f"🟢 READY ({len(ready)})")
        for c in ready:
            lines.append(f"— {c['artist']}" + (f" — {c['date']}" if c.get('date') else ''))
        lines.append("")

    if in_progress:
        lines.append(f"🟡 IN PROGRESS ({len(in_progress)})")
        for c in in_progress:
            lines.append(f"— {c['artist']} (нет: {', '.join(missing_fields(c))})")
        lines.append("")

    if draft_only:
        lines.append(f"🔴 DRAFT ({len(draft_only)})")
        for c in draft_only:
            lines.append(f"— {c['artist']}")
        lines.append("")

    if published:
        lines.append(f"⚫ PUBLISHED ({len(published)})")
        for c in published[:5]:
            lines.append(f"— {c['artist']}")

    await update.message.reply_text('\n'.join(lines), parse_mode='Markdown')


async def cmd_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Укажи номер: `/code 5`", parse_mode='Markdown')
        return
    try: cid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Неверный номер")
        return

    concert = get_concert(cid)
    if not concert:
        await update.message.reply_text(f"#{cid} не найдено")
        return

    artist      = concert.get('artist', '')
    date        = concert.get('date', '')
    time_str    = concert.get('time', '')
    city        = concert.get('city', '')
    tickets_url = concert.get('tickets_url', '')
    description = concert.get('description_text', '')
    date_line   = f"{date} • {time_str}" if time_str else date

    html = f"""<div class="event-wrapper">
    <div class="event-image">
        <!-- Замени POSTER_URL на URL афиши -->
        <img src="POSTER_URL" alt="{artist}">
    </div>
    <div class="event-content">
        <h1 class="event-title">{artist.upper()}</h1>
        <div class="event-datetime">{date_line}</div>
        <div class="event-city">{city}</div>
        <div class="buttons-row">
            <button class="buy-btn" onclick="window.open('{tickets_url}','_blank')">
                Купить билет
            </button>
        </div>
        <div class="event-description"><p>{description}</p></div>
    </div>
</div>"""

    await update.message.reply_text(
        f"🎤 *{artist}* — HTML для Tilda:\n\n"
        f"1. Загрузи афишу → скопируй URL\n"
        f"2. Замени `POSTER_URL`\n"
        f"3. Вставь в Zero Block\n"
        f"4. После публикации → `/publish {cid}`",
        parse_mode='Markdown'
    )
    await update.message.reply_text(f"```html\n{html}\n```", parse_mode='Markdown')


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎸 *MTB Concerts Manager*\n\n"
        "*Триггеры одной строкой:*\n"
        "`Артист билеты https://...`\n"
        "`Артист афиша одобрена`\n"
        "`Артист текст <описание>`\n"
        "`Артист дата 15.03.2026 23:00`\n"
        "`Артист отмена`\n\n"
        "*Команды:*\n"
        "`/createtask [дата Артист Город]` — новое мероприятие\n"
        "`/list` | `/list 2026-03` | `/list all`\n"
        "`/status [номер]` — карточка\n"
        "`/publish [номер]` — опубликовать\n"
        "`/cancel [номер]` — отменить\n"
        "`/menu` — управление кнопками\n"
        "`/code [номер]` — HTML для Tilda\n"
        "`/digest` — сводка по всем",
        parse_mode='Markdown'
    )


# ==================== ОБРАБОТЧИКИ СООБЩЕНИЙ ====================

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    text    = message.text or ''

    # Ожидание ввода из меню
    if context.user_data.get('awaiting') == 'create_artist':
        context.user_data.pop('awaiting')
        context.user_data.pop('awaiting_for_id', None)
        artist = text.strip()
        if artist:
            cid     = save_concert({'artist': artist})
            concert = get_concert(cid)
            kb = [[InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_menu_{cid}")]]
            await message.reply_text(
                concert_card(concert),
                reply_markup=InlineKeyboardMarkup(kb),
                parse_mode='Markdown'
            )
        return

    if await handle_awaiting(update, context):
        return

    # Парсим триггер
    parsed = parse_trigger(text)
    if parsed:
        await process_trigger(update, context, parsed)


# ==================== ПЛАНИРОВЩИК ====================

async def send_morning_digest(context: ContextTypes.DEFAULT_TYPE):
    all_c = get_all_concerts()
    ready, in_progress, draft_only = [], [], []
    for c in all_c:
        if c['published_status'] != 'draft': continue
        if is_ready(c):                      ready.append(c)
        elif any([c.get('date'), c.get('tickets_url'),
                  c.get('poster_status') == 'approved',
                  c.get('description_text')]): in_progress.append(c)
        else:                                draft_only.append(c)

    if not ready and not in_progress and not draft_only:
        return

    now   = datetime.now()
    lines = [f"📊 *Статус на {now.strftime('%d.%m.%Y')}*\n"]
    if ready:
        lines.append(f"🟢 READY ({len(ready)})")
        for c in ready: lines.append(f"— {c['artist']}")
        lines.append("")
    if in_progress:
        lines.append(f"🟡 IN PROGRESS ({len(in_progress)})")
        for c in in_progress: lines.append(f"— {c['artist']} (нет: {', '.join(missing_fields(c))})")
        lines.append("")
    if draft_only:
        lines.append(f"🔴 DRAFT ({len(draft_only)})")
        for c in draft_only: lines.append(f"— {c['artist']}")

    text  = '\n'.join(lines)
    chats = get_digest_chats()
    for chat_id in chats:
        try:
            await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Digest error {chat_id}: {e}")


# ==================== MAIN ====================

def main():
    init_db()
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    for cmd, handler in [
        ("start",       cmd_start),
        ("createtask",  cmd_createtask),
        ("new",         cmd_createtask),
        ("list",        cmd_list),
        ("status",      cmd_status),
        ("publish",     cmd_publish),
        ("cancel",      cmd_cancel),
        ("menu",        cmd_menu),
        ("digest",      cmd_digest),
        ("code",        cmd_code),
        ("help",        cmd_help),
    ]:
        app.add_handler(CommandHandler(cmd, handler))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(handle_callback))

    jq = app.job_queue
    if jq:
        jq.run_daily(send_morning_digest, time=dtime(hour=9, minute=0), name='digest')

    logger.info("🎸 MTB Concerts Bot v3 запущен!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
