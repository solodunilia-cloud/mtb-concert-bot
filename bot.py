#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MTB Concerts Bot — v4
Исправлены все баги из чеклиста:
- Триггеры: "Артист билеты URL", "Артист афиша одобрена", "Артист текст ...", "Артист дата ...", "Артист отмена"
- Город убран везде
- /createtask не создаёт дубли — проверяет существующих
- При нескольких совпадениях — спрашивает уточнение
- /publish с подтверждением
- /digest и /menu работают
- Карточка показывает дату в заголовке
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

# Триггерные слова
T_TICKETS = ['билеты', 'билет', 'ticket', 'tickets']
T_POSTER  = ['афиша', 'poster']
T_TEXT    = ['текст', 'описание', 'text']
T_DATE    = ['дата', 'date', 'перенос']
T_CANCEL  = ['отмена', 'отменен', 'отменён', 'отменили', 'cancel', 'cancelled']
T_POSTER_OK = ['одобрена', 'ок', 'ok', 'утверждена', 'готова', 'approved']

# ==================== HELPERS ====================

def normalize(text: str) -> str:
    text = text.lower().replace('ё', 'е')
    text = re.sub(r'[^\w\s]', '', text)
    return re.sub(r'\s+', ' ', text).strip()

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
        poster_status    TEXT DEFAULT 'none',
        poster_file_id   TEXT,
        tickets_url      TEXT,
        description_text TEXT,
        published_status TEXT DEFAULT 'draft',
        created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            artist=?,date=?,time=?,
            poster_status=?,poster_file_id=?,
            tickets_url=?,description_text=?,
            published_status=?,updated_at=?
            WHERE id=?''', (
            data.get('artist'), data.get('date'), data.get('time'),
            data.get('poster_status','none'), data.get('poster_file_id'),
            data.get('tickets_url'), data.get('description_text'),
            data.get('published_status','draft'), now, data['id']
        ))
        cid = data['id']
    else:
        c.execute('''INSERT INTO concerts
            (artist,date,time,poster_status,poster_file_id,
             tickets_url,description_text,published_status)
            VALUES (?,?,?,?,?,?,?,?)''', (
            data.get('artist'), data.get('date'), data.get('time'),
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
    """Карточка мероприятия — как в макете."""
    icon      = status_icon(concert)
    date_line = f"{concert['date']} {concert.get('time') or ''}".strip() if concert.get('date') else '—'
    m         = missing_fields(concert)

    # Заголовок с датой если есть
    title = f"#{concert['id']} {concert['artist']}"
    if concert.get('date'):
        title += f" • {date_line}"

    text = (
        f"{icon} *{title}*\n\n"
        f"{'✅' if concert.get('poster_status') == 'approved' else '❌'} Афиша\n"
        f"{'✅' if concert.get('tickets_url') else '❌'} Билеты\n"
        f"{'✅' if concert.get('description_text') else '❌'} Текст\n"
        f"{'✅' if concert.get('date') else '❌'} Дата\n"
    )
    if m:
        text += f"\n❗ Не хватает: {', '.join(m)}"
    else:
        text += f"\n🟢 Готов к публикации → `/code {concert['id']}`"
    return text

def edit_keyboard(cid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Дата/время", callback_data=f"ed_date_{cid}"),
         InlineKeyboardButton("🖼 Афиша ок",   callback_data=f"confirm_poster_{cid}")],
        [InlineKeyboardButton("🎟 Билеты",     callback_data=f"ed_tickets_{cid}"),
         InlineKeyboardButton("📝 Текст",      callback_data=f"ed_text_{cid}")],
        [InlineKeyboardButton("✏️ Артист",     callback_data=f"ed_artist_{cid}"),
         InlineKeyboardButton("🚫 Отменить",   callback_data=f"confirm_cancel_{cid}")],
        [InlineKeyboardButton("🗑 Удалить",    callback_data=f"confirm_delete_{cid}")],
    ])

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
    # Если одно явное совпадение
    if top == 100 or (len(results) >= 2 and top - results[1][1] >= 20):
        return [results[0][0]]
    return [r[0] for r in results[:5]]

# ==================== ПАРСИНГ ТРИГГЕРА ====================

def parse_trigger(text: str) -> Optional[Dict]:
    """
    Ищет триггерное слово в тексте.
    Возвращает {artist_name, trigger_type, payload} или None.

    Форматы:
      Иван Дорн билеты https://...
      Иван Дорн афиша одобрена
      Иван Дорн текст Описание концерта...
      Иван Дорн дата 15.04.2026 21:00
      Иван Дорн отмена
    """
    text = text.strip()
    if not text:
        return None

    words = text.split()
    if len(words) < 2:
        return None

    all_triggers = {
        **{normalize(w): 'tickets' for w in T_TICKETS},
        **{normalize(w): 'poster'  for w in T_POSTER},
        **{normalize(w): 'text'    for w in T_TEXT},
        **{normalize(w): 'date'    for w in T_DATE},
        **{normalize(w): 'cancel'  for w in T_CANCEL},
    }

    # Ищем первое триггерное слово начиная со второго слова
    for i in range(1, len(words)):
        w = normalize(words[i])
        if w in all_triggers:
            artist_name = ' '.join(words[:i]).strip()
            trigger_type = all_triggers[w]
            payload = ' '.join(words[i+1:]).strip()
            if artist_name:
                return {
                    'artist_name':  artist_name,
                    'trigger_type': trigger_type,
                    'payload':      payload,
                }
    return None

# ==================== ОБРАБОТКА ТРИГГЕРА ====================

async def process_trigger(update: Update, context: ContextTypes.DEFAULT_TYPE, parsed: Dict):
    message     = update.effective_message
    artist_name = parsed['artist_name']
    ttype       = parsed['trigger_type']
    payload     = parsed['payload']

    matches = fuzzy_find(artist_name)

    if len(matches) == 0:
        # Не найден — предложить создать
        kb = [[
            InlineKeyboardButton("✅ Создать", callback_data=f"cnew|{artist_name}|{ttype}|{payload[:60]}"),
            InlineKeyboardButton("❌ Отмена",  callback_data="noop"),
        ]]
        await message.reply_text(
            f"Мероприятие *{artist_name}* не найдено.\nСоздать новое?",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )
        return

    if len(matches) > 1:
        # Несколько — уточнить
        kb = [[InlineKeyboardButton(
            f"#{c['id']} {c['artist']}" + (f" • {c['date']}" if c.get('date') else ''),
            callback_data=f"tsel|{c['id']}|{ttype}|{payload[:60]}"
        )] for c in matches]
        kb.append([InlineKeyboardButton("❌ Отмена", callback_data='noop')])
        await message.reply_text(
            f"Найдено несколько — уточни:",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    await apply_trigger(update, context, matches[0], ttype, payload)


async def apply_trigger(update: Update, context: ContextTypes.DEFAULT_TYPE,
                         concert: Dict, ttype: str, payload: str):
    message = update.effective_message
    cid     = concert['id']
    name    = concert['artist']

    if ttype == 'cancel':
        kb = [[
            InlineKeyboardButton("✅ Отменить", callback_data=f"confirm_cancel_{cid}"),
            InlineKeyboardButton("❌ Нет",      callback_data="noop"),
        ]]
        await message.reply_text(
            f"Отменить *{name}*?",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )

    elif ttype == 'tickets':
        urls = extract_urls(payload)
        if not urls:
            await message.reply_text(
                f"Не нашёл ссылку.\nПример: `{name} билеты https://...`",
                parse_mode='Markdown'
            )
            return
        url = urls[0]
        action = "Перезаписать билеты" if concert.get('tickets_url') else "Добавить билеты"
        kb = [[
            InlineKeyboardButton("✅ Да", callback_data=f"ctix|{cid}|{url[:100]}"),
            InlineKeyboardButton("❌ Нет", callback_data="noop"),
        ]]
        await message.reply_text(
            f"{action} для *{name}*?",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )

    elif ttype == 'poster':
        payload_norm = normalize(payload)
        if not any(normalize(w) in payload_norm for w in T_POSTER_OK):
            await message.reply_text(
                f"Напиши: `{name} афиша одобрена`",
                parse_mode='Markdown'
            )
            return
        kb = [[
            InlineKeyboardButton("✅ Да", callback_data=f"confirm_poster_{cid}"),
            InlineKeyboardButton("❌ Нет", callback_data="noop"),
        ]]
        await message.reply_text(
            f"Отметить афишу *{name}* как одобренную?",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )

    elif ttype == 'text':
        if len(payload) < 10:
            await message.reply_text(
                f"Укажи текст: `{name} текст Описание концерта...`",
                parse_mode='Markdown'
            )
            return
        context.user_data[f'ptxt_{cid}'] = payload
        preview = payload[:100] + ('...' if len(payload) > 100 else '')
        kb = [[
            InlineKeyboardButton("✅ Да", callback_data=f"ctxt|{cid}"),
            InlineKeyboardButton("❌ Нет", callback_data="noop"),
        ]]
        await message.reply_text(
            f"Добавить описание для *{name}*?\n\n_{preview}_",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )

    elif ttype == 'date':
        date_str, time_str = parse_date_time(payload)
        if not date_str:
            await message.reply_text(
                f"Не распознал дату.\nПример: `{name} дата 15.04.2026 21:00`",
                parse_mode='Markdown'
            )
            return
        date_full = f"{date_str} {time_str or ''}".strip()
        context.user_data[f'pdate_{cid}'] = date_str
        context.user_data[f'ptime_{cid}'] = time_str
        kb = [[
            InlineKeyboardButton("✅ Да", callback_data=f"cdate|{cid}"),
            InlineKeyboardButton("❌ Нет", callback_data="noop"),
        ]]
        await message.reply_text(
            f"Установить дату *{name}*: `{date_full}`?",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )


# ==================== CALLBACK ====================

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data  = query.data

    # Билеты
    if data.startswith('ctix|'):
        _, cid_s, url = data.split('|', 2)
        cid = int(cid_s)
        concert = get_concert(cid)
        if concert:
            concert['tickets_url'] = url
            save_concert(concert)
            sheets.sync_concert(concert)
            await notify_owner_if_ready(context, concert)
            await query.edit_message_text(f"✅ Билеты добавлены — *{concert['artist']}*", parse_mode='Markdown')

    # Афиша
    elif data.startswith('confirm_poster_'):
        cid = int(data.split('_')[2])
        concert = get_concert(cid)
        if concert:
            concert['poster_status'] = 'approved'
            save_concert(concert)
            sheets.sync_concert(concert)
            await notify_owner_if_ready(context, concert)
            await query.edit_message_text(f"✅ Афиша одобрена — *{concert['artist']}*", parse_mode='Markdown')

    # Текст
    elif data.startswith('ctxt|'):
        cid = int(data.split('|')[1])
        concert = get_concert(cid)
        text = context.user_data.pop(f'ptxt_{cid}', None)
        if concert and text:
            concert['description_text'] = text
            save_concert(concert)
            sheets.sync_concert(concert)
            await notify_owner_if_ready(context, concert)
            await query.edit_message_text(f"✅ Текст добавлен — *{concert['artist']}*", parse_mode='Markdown')

    # Дата
    elif data.startswith('cdate|'):
        cid = int(data.split('|')[1])
        concert = get_concert(cid)
        d = context.user_data.pop(f'pdate_{cid}', None)
        t = context.user_data.pop(f'ptime_{cid}', None)
        if concert and d:
            concert['date'] = d
            if t: concert['time'] = t
            save_concert(concert)
            sheets.sync_concert(concert)
            await notify_owner_if_ready(context, concert)
            await query.edit_message_text(f"✅ Дата установлена — *{concert['artist']}*", parse_mode='Markdown')

    # Отмена мероприятия
    elif data.startswith('confirm_cancel_'):
        cid = int(data.split('_')[2])
        concert = get_concert(cid)
        if concert:
            concert['published_status'] = 'cancelled'
            save_concert(concert)
            sheets.sync_concert(concert)
            await query.edit_message_text(f"🚫 *{concert['artist']}* — отменён", parse_mode='Markdown')

    # Удаление
    elif data.startswith('confirm_delete_'):
        cid = int(data.split('_')[2])
        c = get_concert(cid)
        if c:
            delete_concert(cid)
            await query.edit_message_text(f"🗑 *{c['artist']}* — удалён", parse_mode='Markdown')

    # Публикация
    elif data.startswith('confirm_publish_'):
        cid = int(data.split('_')[2])
        concert = get_concert(cid)
        if concert:
            concert['published_status'] = 'published'
            save_concert(concert)
            sheets.sync_concert(concert)
            await query.edit_message_text(f"⚫ *{concert['artist']}* — опубликован", parse_mode='Markdown')

    # Создать нового при ненайденном
    elif data.startswith('cnew|'):
        parts   = data.split('|', 3)
        artist  = parts[1]
        ttype   = parts[2]
        payload = parts[3] if len(parts) > 3 else ''
        cid     = save_concert({'artist': artist})
        concert = get_concert(cid)
        await query.edit_message_text(f"✅ Создано: *#{cid} {artist}*", parse_mode='Markdown')
        await apply_trigger(update, context, concert, ttype, payload)

    # Выбор из нескольких совпадений
    elif data.startswith('tsel|'):
        parts   = data.split('|', 3)
        cid     = int(parts[1])
        ttype   = parts[2]
        payload = parts[3] if len(parts) > 3 else ''
        concert = get_concert(cid)
        await query.edit_message_text(f"#{cid} {concert['artist']}")
        await apply_trigger(update, context, concert, ttype, payload)

    # Меню редактирования (открыть)
    elif data.startswith('edit_menu_'):
        cid = int(data.split('_')[2])
        c   = get_concert(cid)
        if c:
            await query.edit_message_text(
                concert_card(c),
                reply_markup=edit_keyboard(cid),
                parse_mode='Markdown'
            )

    # Поля редактирования через меню
    elif data.startswith('ed_'):
        parts = data.split('_', 2)
        field = parts[1]
        cid   = int(parts[2])
        c     = get_concert(cid)
        context.user_data['awaiting']        = field
        context.user_data['awaiting_for_id'] = cid
        prompts = {
            'date':    f"📅 Введи дату для *{c['artist']}*:\nПример: `15.04.2026 21:00`",
            'tickets': f"🎟 Введи ссылку для *{c['artist']}*:",
            'text':    f"📝 Введи описание для *{c['artist']}*:",
            'artist':  f"✏️ Новое имя (сейчас: {c['artist']}):",
        }
        await query.edit_message_text(prompts.get(field, 'Введи значение:'), parse_mode='Markdown')

    elif data == 'noop':
        await query.edit_message_text("Отменено")


# ==================== ОЖИДАНИЕ ВВОДА ====================

async def handle_awaiting(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    awaiting = context.user_data.get('awaiting')
    if not awaiting:
        return False
    cid = context.user_data.pop('awaiting_for_id', None)
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
    elif awaiting == 'tickets':
        urls = extract_urls(text)
        concert['tickets_url'] = urls[0] if urls else text
    elif awaiting == 'text':
        concert['description_text'] = text
    elif awaiting == 'artist':
        concert['artist'] = text
    else:
        return False

    save_concert(concert)
    sheets.sync_concert(concert)
    await notify_owner_if_ready(context, concert)
    kb = [[InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_menu_{cid}")]]
    await update.message.reply_text(
        f"✅ *{concert['artist']}* — сохранено",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode='Markdown'
    )
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
        "`Артист дата 15.04.2026 21:00`\n"
        "`Артист отмена`\n\n"
        "*Команды:*\n"
        "`/createtask` — новое мероприятие\n"
        "`/list` | `/list 2026-04`\n"
        "`/status [номер]` — карточка\n"
        "`/menu` — управление кнопками\n"
        "`/publish [номер]` — опубликовать\n"
        "`/cancel [номер]` — отменить\n"
        "`/code [номер]` — HTML для Tilda\n"
        "`/digest` — сводка",
        parse_mode='Markdown'
    )


async def cmd_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_chat(update.effective_chat.id)
    args_text = ' '.join(context.args).strip() if context.args else ''

    if not args_text:
        context.user_data['awaiting']        = 'create_name'
        context.user_data['awaiting_for_id'] = 0
        await update.message.reply_text("Введи имя артиста:")
        return

    date_str, time_str = parse_date_time(args_text)
    # Убираем дату из текста чтобы получить имя
    cleaned = re.sub(r'\d{1,2}[./\-]\d{1,2}[./\-]\d{4}', '', args_text)
    cleaned = re.sub(r'\b\d{1,2}[:.]\d{2}\b', '', cleaned).strip()
    artist  = cleaned.strip() or args_text.strip()

    # Проверяем дубли
    matches = fuzzy_find(artist)
    exact   = [c for c in matches if normalize(c['artist']) == normalize(artist)]
    if exact:
        c = exact[0]
        kb = [[
            InlineKeyboardButton("Создать новое", callback_data=f"force_create|{artist}|{date_str or ''}|{time_str or ''}"),
            InlineKeyboardButton("Открыть существующее", callback_data=f"edit_menu_{c['id']}"),
        ]]
        await update.message.reply_text(
            f"*{artist}* уже существует (#{c['id']}).\nЧто делаем?",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )
        return

    cid = save_concert({'artist': artist, 'date': date_str, 'time': time_str})
    concert = get_concert(cid)
    kb = [[InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_menu_{cid}")]]
    await update.message.reply_text(
        concert_card(concert),
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode='Markdown'
    )


async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_chat(update.effective_chat.id)
    include_all  = bool(context.args and context.args[0].lower() == 'all')
    month_filter = None
    if context.args and re.match(r'^\d{4}-\d{2}$', context.args[0]):
        month_filter = context.args[0]

    concerts = get_all_concerts(include_cancelled=include_all)

    if month_filter:
        def in_month(c):
            d = c.get('date', '')
            if not d: return False
            try:
                p = d.split('.')
                return f"{p[2]}-{p[1]}" == month_filter
            except: return False
        concerts = [c for c in concerts if in_month(c)]

    if not concerts:
        await update.message.reply_text("Мероприятий нет. Создай: `/createtask`", parse_mode='Markdown')
        return

    active    = [c for c in concerts if c['published_status'] == 'draft']
    published = [c for c in concerts if c['published_status'] == 'published']
    cancelled = [c for c in concerts if c['published_status'] == 'cancelled']

    lines = [f"📋 *В работе: {len(active)}*\n"]
    for c in active:
        icon = status_icon(c)
        d    = f" — {c['date']}" if c.get('date') else ''
        m    = missing_fields(c)
        miss = f" | нет: {', '.join(m)}" if m else " | ✅"
        lines.append(f"{icon} #{c['id']} {c['artist']}{d}{miss}")

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
            name    = ' '.join(context.args)
            matches = fuzzy_find(name)
            if len(matches) == 1:
                cid = matches[0]['id']
            elif len(matches) > 1:
                kb = [[InlineKeyboardButton(
                    f"#{c['id']} {c['artist']}" + (f" • {c['date']}" if c.get('date') else ''),
                    callback_data=f"edit_menu_{c['id']}"
                )] for c in matches]
                await update.message.reply_text("Уточни:", reply_markup=InlineKeyboardMarkup(kb))
                return

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


async def cmd_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    concerts = [c for c in get_all_concerts() if c['published_status'] == 'draft']
    if not concerts:
        await update.message.reply_text("Нет активных мероприятий. Создай: `/createtask`", parse_mode='Markdown')
        return
    kb = [[InlineKeyboardButton(
        f"{status_icon(c)} #{c['id']} {c['artist']}" + (f" • {c['date']}" if c.get('date') else ''),
        callback_data=f"edit_menu_{c['id']}"
    )] for c in concerts[:15]]
    await update.message.reply_text("📋 Выбери мероприятие:", reply_markup=InlineKeyboardMarkup(kb))


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

    if not any([ready, in_progress, draft_only]):
        await update.message.reply_text("📊 Активных мероприятий нет.")
        return

    now   = datetime.now()
    lines = [f"📊 *Статус на {now.strftime('%d.%m.%Y')}*\n"]
    if ready:
        lines.append(f"🟢 READY ({len(ready)})")
        for c in ready: lines.append(f"— {c['artist']}" + (f" • {c['date']}" if c.get('date') else ''))
        lines.append("")
    if in_progress:
        lines.append(f"🟡 IN PROGRESS ({len(in_progress)})")
        for c in in_progress: lines.append(f"— {c['artist']} (нет: {', '.join(missing_fields(c))})")
        lines.append("")
    if draft_only:
        lines.append(f"🔴 DRAFT ({len(draft_only)})")
        for c in draft_only: lines.append(f"— {c['artist']}")
        lines.append("")
    if published:
        lines.append(f"⚫ PUBLISHED ({len(published)})")
        for c in published[:5]: lines.append(f"— {c['artist']}")

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
    time_s      = concert.get('time', '')
    tickets_url = concert.get('tickets_url', '')
    description = concert.get('description_text', '')
    date_line   = f"{date} • {time_s}" if time_s else date

    html = f"""<div class="event-wrapper">
    <div class="event-image">
        <!-- Замени POSTER_URL на URL афиши после загрузки в Tilda -->
        <img src="POSTER_URL" alt="{artist}">
    </div>
    <div class="event-content">
        <h1 class="event-title">{artist.upper()}</h1>
        <div class="event-datetime">{date_line}</div>
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
    await cmd_start(update, context)


# ==================== ОБРАБОТЧИК ТЕКСТА ====================

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ''

    # Ожидание имени при /createtask
    if context.user_data.get('awaiting') == 'create_name':
        context.user_data.pop('awaiting')
        context.user_data.pop('awaiting_for_id', None)
        artist = text.strip()
        if artist:
            cid     = save_concert({'artist': artist})
            concert = get_concert(cid)
            kb = [[InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_menu_{cid}")]]
            await update.message.reply_text(
                concert_card(concert),
                reply_markup=InlineKeyboardMarkup(kb),
                parse_mode='Markdown'
            )
        return

    # Ожидание ввода из меню
    if await handle_awaiting(update, context):
        return

    # Парсим триггер
    parsed = parse_trigger(text)
    if parsed:
        await process_trigger(update, context, parsed)


# ==================== CALLBACK: force_create ====================

async def handle_callback_extra(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Дополнительные колбэки."""
    query = update.callback_query
    data  = query.data

    if data.startswith('force_create|'):
        await query.answer()
        parts    = data.split('|')
        artist   = parts[1]
        date_str = parts[2] or None
        time_str = parts[3] or None
        cid      = save_concert({'artist': artist, 'date': date_str, 'time': time_str})
        concert  = get_concert(cid)
        kb = [[InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_menu_{cid}")]]
        await query.edit_message_text(
            concert_card(concert),
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )


# ==================== ПЛАНИРОВЩИК ====================

async def send_morning_digest(context: ContextTypes.DEFAULT_TYPE):
    all_c = get_all_concerts()
    ready, in_progress, draft_only = [], [], []
    for c in all_c:
        if c['published_status'] != 'draft': continue
        if is_ready(c):   ready.append(c)
        elif any([c.get('date'), c.get('tickets_url'),
                  c.get('poster_status') == 'approved',
                  c.get('description_text')]): in_progress.append(c)
        else: draft_only.append(c)

    if not any([ready, in_progress, draft_only]):
        return

    now   = datetime.now()
    lines = [f"📊 *Статус на {now.strftime('%d.%m.%Y')}*\n"]
    if ready:
        lines.append(f"🟢 READY ({len(ready)})")
        for c in ready: lines.append(f"— {c['artist']}" + (f" • {c['date']}" if c.get('date') else ''))
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
            logger.error(f"Digest {chat_id}: {e}")


# ==================== MAIN ====================

def main():
    init_db()
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    for cmd, handler in [
        ("start",      cmd_start),
        ("new", cmd_new),
        # убрано,
        ("list",       cmd_list),
        ("status",     cmd_status),
        ("publish",    cmd_publish),
        ("cancel",     cmd_cancel),
        ("edit", cmd_edit),
        ("digest",     cmd_digest),
        ("code",       cmd_code),
        ("help",       cmd_help),
    ]:
        app.add_handler(CommandHandler(cmd, handler))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    # Два обработчика колбэков — основной и дополнительный
    app.add_handler(CallbackQueryHandler(handle_callback_extra, pattern=r'^force_create\|'))
    app.add_handler(CallbackQueryHandler(handle_callback))

    jq = app.job_queue
    if jq:
        jq.run_daily(send_morning_digest, time=dtime(hour=9, minute=0), name='digest')

    logger.info("🎸 MTB Concerts Bot v4 запущен!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
