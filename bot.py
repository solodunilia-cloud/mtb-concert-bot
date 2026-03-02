#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MTB Concerts Bot — Collector Mode v2
mtbarmoscow.com

Логика:
- В группе бот МОЛЧИТ при обновлениях
- Когда концерт готов 100% — личное уведомление владельцу (OWNER_ID)
- /code [id] — HTML для Tilda
- /publish [id] — пометить как опубликованный
- Fuzzy-поиск артиста (RapidFuzz, порог 70%)
- Утренний дайджест 9:00
- Google Sheets — календарный вид
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

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '')
SHEETS_ID      = os.getenv('GOOGLE_SHEETS_ID', '')
OWNER_ID       = int(os.getenv('OWNER_ID', '534303997'))

DB_PATH = 'concerts.db'
sheets  = GoogleSheetsManager(spreadsheet_id=SHEETS_ID if SHEETS_ID else None)

FUZZY_THRESHOLD = 70
DESC_MIN_LENGTH = 120

POSTER_OK_PHRASES = ['афиша ок', 'афиша утверждена', 'афиша готова']
CANCEL_WORDS      = ['отмена', 'отменён', 'отменен', 'отменили', 'cancelled', 'canceled']
TICKET_WORDS      = ['билеты', 'билет', 'tickets', 'ticket', 'купить', 'продажа']
DATE_WORDS        = ['дата', 'date', 'перенос', 'перенесли', 'перенесен', 'перенести']


# ==================== HELPERS ====================

def normalize(text: str) -> str:
    text = text.lower().replace('ё', 'е')
    text = re.sub(r'[^\w\s]', '', text)
    return re.sub(r'\s+', ' ', text).strip()


def is_group(update: Update) -> bool:
    return update.message.chat.type in ('group', 'supergroup')


def has_word(text: str, words: List[str]) -> bool:
    t = normalize(text)
    return any(normalize(w) in t for w in words)


def extract_urls(text: str) -> List[str]:
    return re.findall(r'https?://[^\s<>"\']+', text)


def is_ticket_url(url: str) -> bool:
    u = url.lower()
    return any(x in u for x in [
        'afisha.yandex', 'widget.afisha', 'ticketmaster', 'kassy',
        'ponominalu', 'kassir', 'concert.ru', 'radario', 'parter',
        'bileter', 'ticketscloud', 'tickets'
    ])


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
    c.execute('''CREATE TABLE IF NOT EXISTS pending_photos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id    TEXT NOT NULL,
        chat_id    INTEGER NOT NULL,
        message_id INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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


def save_reminder(concert_id: int, remind_at: str):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('INSERT INTO reminders (concert_id, remind_at) VALUES (?,?)', (concert_id, remind_at))
    conn.commit()
    conn.close()


def get_pending_reminders() -> List[Dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('''SELECT r.*,c.artist FROM reminders r
        JOIN concerts c ON r.concert_id=c.id
        WHERE r.sent=0 AND r.remind_at<=?''', (datetime.now().isoformat(),))
    rows = c.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def mark_reminder_sent(rid: int):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('UPDATE reminders SET sent=1 WHERE id=?', (rid,))
    conn.commit()
    conn.close()


def save_pending_photo(file_id: str, chat_id: int, message_id: int):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('DELETE FROM pending_photos')
    c.execute('INSERT INTO pending_photos (file_id, chat_id, message_id) VALUES (?,?,?)',
              (file_id, chat_id, message_id))
    conn.commit()
    conn.close()


def get_latest_pending_photo() -> Optional[Dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('SELECT * FROM pending_photos ORDER BY created_at DESC LIMIT 1')
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None


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


def status_emoji(concert: Dict) -> str:
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


def missing_fields(concert: Dict) -> List[str]:
    m = []
    if not concert.get('date'):                         m.append('дата')
    if concert.get('poster_status') != 'approved':      m.append('афиша')
    if not concert.get('tickets_url'):                  m.append('билеты')
    if not concert.get('description_text'):             m.append('текст')
    return m


async def notify_owner_if_ready(context: ContextTypes.DEFAULT_TYPE, concert: Dict):
    if is_ready(concert) and concert.get('published_status') == 'draft':
        try:
            await context.bot.send_message(
                chat_id=OWNER_ID,
                text=(
                    f"🎤 *{concert['artist']}*\n"
                    f"Концерт полностью готов к публикации.\n"
                    f"ID: {concert['id']}\n\n"
                    f"`/code {concert['id']}`"
                ),
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"notify_owner error: {e}")


# ==================== HTML ДЛЯ TILDA ====================

def generate_concert_html(concert: Dict) -> str:
    artist      = concert.get('artist', '')
    date        = concert.get('date', '')
    time_str    = concert.get('time', '')
    city        = concert.get('city', '')
    tickets_url = concert.get('tickets_url', '')
    description = concert.get('description_text', '')
    date_line   = f"{date} • {time_str}" if time_str else date

    return f"""<div class="event-wrapper">
    <div class="event-image">
        <!-- Замени POSTER_URL на URL афиши после загрузки в Tilda -->
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
        <div class="event-description">
            <p>{description}</p>
        </div>
    </div>
</div>"""


# ==================== ДАЙДЖЕСТ ====================

def morning_digest_text() -> str:
    now   = datetime.now()
    all_c = get_all_concerts(include_cancelled=False)

    ready       = []
    in_progress = []
    draft_only  = []
    published   = []

    for c in all_c:
        if c['published_status'] == 'published':
            published.append(c)
        elif is_ready(c):
            ready.append(c)
        elif any([c.get('date'), c.get('tickets_url'),
                  c.get('poster_status') == 'approved',
                  c.get('description_text')]):
            in_progress.append(c)
        else:
            draft_only.append(c)

    if not ready and not in_progress and not draft_only:
        return "📊 Активных мероприятий нет."

    lines = [f"📊 *Статус концертов на {now.strftime('%d.%m.%Y')}*\n"]

    if ready:
        lines.append(f"🟢 READY ({len(ready)})")
        for c in ready:
            d = f" — {c['date']}" if c.get('date') else ''
            lines.append(f"— {c['artist']}{d}")
        lines.append("")

    if in_progress:
        lines.append(f"🟡 IN_PROGRESS ({len(in_progress)})")
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

    return '\n'.join(lines)


# ==================== FUZZY ПОИСК ====================

def fuzzy_find_concert(name: str) -> List[Dict]:
    concerts  = get_all_concerts()
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


# ==================== УМНЫЙ ПАРСИНГ ====================

async def try_smart_parse(update: Update, context: ContextTypes.DEFAULT_TYPE,
                           override_text: str = None) -> bool:
    message  = update.message
    text     = override_text or (message.text or '') or (message.caption or '')
    in_group = is_group(update)

    if not text:
        return False

    parts       = re.split(r'\s*[—\-:|]\s*', text, maxsplit=1)
    artist_name = None
    rest        = text

    if len(parts) == 2 and len(parts[0].strip()) > 1:
        artist_name = parts[0].strip()
        rest        = parts[1].strip()

    found_concert = None
    if artist_name:
        matches = fuzzy_find_concert(artist_name)
        if len(matches) == 1:
            found_concert = matches[0]
        elif len(matches) > 1:
            kb = [[InlineKeyboardButton(f"#{c['id']} {c['artist']}", callback_data=f"ctx_{c['id']}")] for c in matches[:5]]
            kb.append([InlineKeyboardButton("❌ Отмена", callback_data='ctx_cancel')])
            context.user_data['pending_text'] = text
            await message.reply_text("Уточни:", reply_markup=InlineKeyboardMarkup(kb))
            return True

    if not found_concert:
        cid = context.user_data.get('current_concert_id')
        if cid:
            found_concert = get_concert(cid)
            rest = text

    if not found_concert:
        return False

    rest_low = normalize(rest)
    updated  = False

    # Отмена
    if has_word(rest_low, CANCEL_WORDS):
        found_concert['published_status'] = 'cancelled'
        save_concert(found_concert)
        if not in_group:
            await message.reply_text(f"🚫 {found_concert['artist']} — отменён")
        return True

    # Афиша ок
    if any(normalize(p) in rest_low for p in POSTER_OK_PHRASES):
        pending = get_latest_pending_photo()
        if pending:
            found_concert['poster_status']  = 'approved'
            found_concert['poster_file_id'] = pending['file_id']
            save_concert(found_concert)
            sheets.sync_concert(found_concert)
            await notify_owner_if_ready(context, found_concert)
        elif not in_group:
            await message.reply_text("Пришли фото афиши — потом напиши «афиша ок»")
        return True

    # URL билеты
    urls = extract_urls(rest)
    for url in urls:
        if (is_ticket_url(url) or has_word(rest_low, TICKET_WORDS)) and not found_concert.get('tickets_url'):
            found_concert['tickets_url'] = url
            updated = True

    # Дата / время
    date_str, time_str = parse_date_time(rest)
    if date_str and (not found_concert.get('date') or has_word(rest_low, DATE_WORDS)):
        found_concert['date'] = date_str
        updated = True
    if time_str and not found_concert.get('time'):
        found_concert['time'] = time_str
        updated = True

    # Длинный текст — описание
    if not urls and len(rest) >= DESC_MIN_LENGTH and not has_word(rest_low, CANCEL_WORDS):
        context.user_data['pending_description'] = rest
        context.user_data['awaiting_for_id']     = found_concert['id']
        kb = [[
            InlineKeyboardButton("✅ Да", callback_data=f"confirm_desc_{found_concert['id']}"),
            InlineKeyboardButton("❌ Нет", callback_data="noop"),
        ]]
        await message.reply_text(
            f"Сохранить как описание для *{found_concert['artist']}*?",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )
        return True

    if updated:
        save_concert(found_concert)
        sheets.sync_concert(found_concert)
        await notify_owner_if_ready(context, found_concert)
        # В группе молчим
        if not in_group:
            await message.reply_text(f"✅ {found_concert['artist']} — сохранено")
        return True

    return False


# ==================== КОМАНДЫ ====================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_chat(update.message.chat_id)
    await update.message.reply_text(
        "🎸 *MTB Concerts — Collector Mode v2*\n\n"
        "*В группе пиши:*\n"
        "`Артист — афиша ок`\n"
        "`Артист — билеты https://...`\n"
        "`Артист — 15 марта 21:00`\n"
        "`Артист — отмена`\n\n"
        "Бот в группе *молчит* — только фиксирует.\n"
        "Когда всё готово — пишет тебе в личку.\n\n"
        "*Команды:*\n"
        "/add 12.03.2026 Артист Город\n"
        "/list | /list all\n"
        "/status [номер]\n"
        "/today | /calendar\n"
        "/code [номер] — HTML для Tilda\n"
        "/publish [номер] — опубликовано\n"
        "/remind 3d Артист\n"
        "/digest | /delete [номер]",
        parse_mode='Markdown'
    )


async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_chat(update.message.chat_id)
    args_text = ' '.join(context.args).strip() if context.args else ''
    if not args_text:
        await update.message.reply_text("Формат: `/add 12.03.2026 Артист Город`", parse_mode='Markdown')
        return

    date_str, time_str = parse_date_time(args_text)
    cleaned = re.sub(r'\d{1,2}[./\-]\d{1,2}[./\-]\d{4}', '', args_text)
    cleaned = re.sub(r'\b\d{1,2}:\d{2}\b', '', cleaned).strip()
    parts   = cleaned.split()

    if len(parts) >= 2:
        city, artist = parts[-1], ' '.join(parts[:-1])
    elif len(parts) == 1:
        artist, city = parts[0], None
    else:
        await update.message.reply_text("Укажи хотя бы имя артиста")
        return

    cid = save_concert({'artist': artist, 'date': date_str, 'time': time_str, 'city': city})
    context.user_data['current_concert_id'] = cid
    await update.message.reply_text(
        f"✅ *#{cid} {artist}*\n📅 {date_str or '—'} {time_str or ''} | 🏙 {city or '—'}",
        parse_mode='Markdown'
    )


async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_chat(update.message.chat_id)
    include_all = bool(context.args and context.args[0].lower() == 'all')
    concerts    = get_all_concerts(include_cancelled=include_all)

    if not concerts:
        await update.message.reply_text("Мероприятий нет. Добавь: `/add дата Артист Город`", parse_mode='Markdown')
        return

    active    = [c for c in concerts if c['published_status'] == 'draft']
    published = [c for c in concerts if c['published_status'] == 'published']
    cancelled = [c for c in concerts if c['published_status'] == 'cancelled']

    text = f"📋 *В работе: {len(active)}*\n\n"
    for c in active:
        icon = status_emoji(c)
        d    = f" — {c['date']}" if c.get('date') else ''
        city = f" ({c['city']})" if c.get('city') else ''
        m    = missing_fields(c)
        miss = f" | нет: {', '.join(m)}" if m else " | ✅ готов"
        text += f"{icon} #{c['id']} {c['artist']}{d}{city}{miss}\n"

    if published:
        text += f"\n⚫ Опубликовано: {len(published)}\n"
        for c in published[:5]:
            text += f"  #{c['id']} {c['artist']}\n"

    if include_all and cancelled:
        text += f"\n🚫 Отменены: {len(cancelled)}\n"
        for c in cancelled:
            text += f"  #{c['id']} {c['artist']}\n"

    await update.message.reply_text(text, parse_mode='Markdown')


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = None
    if context.args:
        try: cid = int(context.args[0])
        except ValueError: pass
    if not cid:
        cid = context.user_data.get('current_concert_id')
    if not cid:
        await update.message.reply_text("Укажи номер: `/status 5`", parse_mode='Markdown')
        return

    concert = get_concert(cid)
    if not concert:
        await update.message.reply_text(f"#{cid} не найдено")
        return

    icon      = status_emoji(concert)
    date_line = f"{concert['date']} {concert.get('time','')}" .strip() if concert.get('date') else '—'
    m         = missing_fields(concert)

    text = (
        f"{icon} *#{concert['id']} {concert['artist']}*\n"
        f"📅 {date_line} | 🏙 {concert.get('city') or '—'}\n\n"
        f"{'✅' if concert.get('poster_status')=='approved' else '❌'} Афиша\n"
        f"{'✅' if concert.get('tickets_url') else '❌'} Билеты\n"
        f"{'✅' if concert.get('description_text') else '❌'} Текст\n"
        f"{'✅' if concert.get('date') else '❌'} Дата\n"
    )
    if m:
        text += f"\n❗ Не хватает: {', '.join(m)}"
    else:
        text += f"\n🟢 Готов → `/code {cid}`"

    kb = [[InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_menu_{cid}")]]
    await update.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))


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

    html = generate_concert_html(concert)
    await update.message.reply_text(
        f"🎤 *{concert['artist']}* — HTML для Tilda:\n\n"
        f"1. Зайди в Tilda → создай страницу\n"
        f"2. Загрузи афишу → скопируй URL\n"
        f"3. Замени `POSTER_URL` на URL афиши\n"
        f"4. Вставь в Zero Block\n"
        f"5. После публикации → `/publish {cid}`",
        parse_mode='Markdown'
    )
    await update.message.reply_text(f"```html\n{html}\n```", parse_mode='Markdown')


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

    concert['published_status'] = 'published'
    save_concert(concert)
    sheets.sync_concert(concert)
    await update.message.reply_text(f"⚫ {concert['artist']} — опубликован")


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_chat(update.message.chat_id)
    today    = datetime.now().strftime('%d.%m.%Y')
    concerts = [c for c in get_all_concerts() if c.get('date') == today]

    if not concerts:
        await update.message.reply_text(f"Сегодня ({today}) концертов нет")
        return

    lines = [f"🎤 *Сегодня {today}:*\n"]
    for c in concerts:
        lines.append(f"{status_emoji(c)} *{c['artist']}*")
        if c.get('time'): lines.append(f"   🕐 {c['time']}")
        if c.get('city'): lines.append(f"   🏙 {c['city']}")
        m = missing_fields(c)
        if m: lines.append(f"   ❗ Нет: {', '.join(m)}")
        lines.append("")
    await update.message.reply_text('\n'.join(lines), parse_mode='Markdown')


async def cmd_calendar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_chat(update.message.chat_id)
    await update.message.reply_text(morning_digest_text(), parse_mode='Markdown')


async def cmd_digest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_chat(update.message.chat_id)
    await update.message.reply_text(morning_digest_text(), parse_mode='Markdown')


async def cmd_remind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Формат: `/remind 3d Артист` или `/remind 12.03.2026 Артист`", parse_mode='Markdown')
        return

    when_arg   = context.args[0]
    artist_arg = ' '.join(context.args[1:])
    matches    = fuzzy_find_concert(artist_arg)

    if not matches:
        await update.message.reply_text("Артист не найден")
        return
    if len(matches) > 1:
        kb = [[InlineKeyboardButton(f"#{c['id']} {c['artist']}", callback_data=f"remind_select_{c['id']}_{when_arg}")] for c in matches[:5]]
        await update.message.reply_text("Уточни:", reply_markup=InlineKeyboardMarkup(kb))
        return

    concert   = matches[0]
    remind_dt = None
    m = re.match(r'^(\d+)d$', when_arg)
    if m:
        remind_dt = datetime.now() + timedelta(days=int(m.group(1)))
    else:
        ds, _ = parse_date_time(when_arg)
        if ds:
            try: remind_dt = datetime.strptime(ds, '%d.%m.%Y')
            except: pass

    if not remind_dt:
        await update.message.reply_text("Не понял формат. Пример: `3d` или `12.03.2026`", parse_mode='Markdown')
        return

    save_reminder(concert['id'], remind_dt.isoformat())
    await update.message.reply_text(
        f"🔔 Напомню про *{concert['artist']}* {remind_dt.strftime('%d.%m.%Y')}",
        parse_mode='Markdown'
    )


async def cmd_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Укажи номер: `/delete 5`", parse_mode='Markdown')
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
        InlineKeyboardButton("✅ Удалить", callback_data=f"confirm_delete_{cid}"),
        InlineKeyboardButton("❌ Отмена",  callback_data="noop"),
    ]]
    await update.message.reply_text(
        f"Удалить *#{cid} {concert['artist']}*?",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode='Markdown'
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎸 *MTB Concerts Bot — Collector Mode v2*\n\n"
        "В группе пиши:\n"
        "`Артист — афиша ок` | `Артист — билеты https://...`\n"
        "`Артист — 15 марта 21:00` | `Артист — отмена`\n\n"
        "Бот в группе *молчит*. Когда готово — пишет тебе в личку.\n\n"
        "`/add дата Артист Город` | `/list` | `/list all`\n"
        "`/status [номер]` | `/today` | `/calendar`\n"
        "`/code [номер]` — HTML для Tilda\n"
        "`/publish [номер]` — опубликовано\n"
        "`/remind 3d Артист` | `/digest` | `/delete [номер]`",
        parse_mode='Markdown'
    )


# ==================== ОБРАБОТЧИКИ ====================

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get('awaiting'):
        await handle_awaiting_input(update, context, update.message.text or '')
        return
    await try_smart_parse(update, context)


async def handle_awaiting_input(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    awaiting = context.user_data.pop('awaiting', None)
    cid      = context.user_data.pop('awaiting_for_id', None)
    if not cid: return
    concert = get_concert(cid)
    if not concert: return

    if   awaiting == 'description': concert['description_text'] = text
    elif awaiting == 'tickets_url':
        urls = extract_urls(text)
        concert['tickets_url'] = urls[0] if urls else text
    elif awaiting == 'date_time':
        d, t = parse_date_time(text)
        if d: concert['date'] = d
        if t: concert['time'] = t
    elif awaiting == 'artist': concert['artist'] = text
    elif awaiting == 'city':   concert['city']   = text
    else: return

    save_concert(concert)
    sheets.sync_concert(concert)
    await notify_owner_if_ready(context, concert)
    await update.message.reply_text(f"✅ {concert['artist']} — сохранено")


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    photo   = message.photo[-1]
    save_pending_photo(photo.file_id, message.chat_id, message.message_id)

    if message.caption:
        if await try_smart_parse(update, context, override_text=message.caption):
            return

    awaiting_for = context.user_data.pop('awaiting_image_for', None)
    if awaiting_for:
        concert = get_concert(awaiting_for)
        if concert:
            concert['poster_status']  = 'approved'
            concert['poster_file_id'] = photo.file_id
            save_concert(concert)
            sheets.sync_concert(concert)
            await notify_owner_if_ready(context, concert)
            if not is_group(update):
                await message.reply_text(f"✅ Афиша {concert['artist']} — принята")
    # В группе молчим


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data  = query.data

    if data.startswith('confirm_desc_'):
        cid  = int(data.split('_')[2])
        desc = context.user_data.pop('pending_description', None)
        if desc:
            concert = get_concert(cid)
            if concert:
                concert['description_text'] = desc
                save_concert(concert)
                sheets.sync_concert(concert)
                await notify_owner_if_ready(context, concert)
                await query.edit_message_text(f"✅ {concert['artist']} — текст сохранён")

    elif data.startswith('confirm_delete_'):
        cid = int(data.split('_')[2])
        c   = get_concert(cid)
        if c:
            delete_concert(cid)
            await query.edit_message_text(f"🗑 {c['artist']} — удалён")

    elif data.startswith('edit_menu_'):
        cid = int(data.split('_')[2])
        c   = get_concert(cid)
        kb  = [
            [InlineKeyboardButton("📅 Дата/время", callback_data=f"set_date_{cid}"),
             InlineKeyboardButton("🏙 Город",       callback_data=f"set_city_{cid}")],
            [InlineKeyboardButton("🎟 Билеты",      callback_data=f"set_tickets_{cid}"),
             InlineKeyboardButton("🖼 Афиша",       callback_data=f"set_image_{cid}")],
            [InlineKeyboardButton("📝 Текст",       callback_data=f"set_desc_{cid}"),
             InlineKeyboardButton("✏️ Артист",      callback_data=f"set_artist_{cid}")],
            [InlineKeyboardButton("🚫 Отменить",    callback_data=f"cancel_event_{cid}")],
        ]
        await query.edit_message_text(
            f"✏️ *#{cid} {c['artist']}* — что изменить?",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown'
        )

    elif data.startswith('set_'):
        parts = data.split('_', 2)
        field, cid = parts[1], int(parts[2])
        c = get_concert(cid)
        context.user_data['current_concert_id'] = cid
        prompts = {
            'date':    ('date_time',   f"📅 Дата и время для *{c['artist']}*:"),
            'desc':    ('description', f"📝 Текст для *{c['artist']}*:"),
            'tickets': ('tickets_url', f"🎟 Ссылка на билеты для *{c['artist']}*:"),
            'image':   ('__image__',   f"🖼 Пришли фото афиши для *{c['artist']}*:"),
            'artist':  ('artist',      f"✏️ Новое имя (сейчас: {c['artist']}):"),
            'city':    ('city',        f"🏙 Город для *{c['artist']}*:"),
        }
        if field not in prompts: return
        key, prompt = prompts[field]
        if key == '__image__':
            context.user_data['awaiting_image_for'] = cid
        else:
            context.user_data['awaiting']        = key
            context.user_data['awaiting_for_id'] = cid
        await query.edit_message_text(prompt, parse_mode='Markdown')

    elif data.startswith('cancel_event_'):
        cid = int(data.split('_')[2])
        c   = get_concert(cid)
        c['published_status'] = 'cancelled'
        save_concert(c)
        await query.edit_message_text(f"🚫 {c['artist']} — отменён")

    elif data.startswith('ctx_'):
        val = data[4:]
        if val == 'cancel':
            await query.edit_message_text("Отменено")
            return
        cid = int(val)
        c   = get_concert(cid)
        context.user_data['current_concert_id'] = cid
        pending = context.user_data.pop('pending_text', None)
        await query.edit_message_text(f"#{cid} {c['artist']}")
        if pending:
            rest = pending.split('—', 1)[-1].strip() if '—' in pending else pending
            await try_smart_parse(update, context, override_text=rest)

    elif data.startswith('remind_select_'):
        parts = data.split('_')
        cid, when_arg = int(parts[2]), parts[3]
        c = get_concert(cid)
        remind_dt = None
        m = re.match(r'^(\d+)d$', when_arg)
        if m:
            remind_dt = datetime.now() + timedelta(days=int(m.group(1)))
        else:
            ds, _ = parse_date_time(when_arg)
            if ds:
                try: remind_dt = datetime.strptime(ds, '%d.%m.%Y')
                except: pass
        if remind_dt:
            save_reminder(cid, remind_dt.isoformat())
            await query.edit_message_text(f"🔔 Напомню про {c['artist']} {remind_dt.strftime('%d.%m.%Y')}")

    elif data == 'noop':
        await query.edit_message_text("Ок")


# ==================== ПЛАНИРОВЩИК ====================

async def send_morning_digest(context: ContextTypes.DEFAULT_TYPE):
    text  = morning_digest_text()
    chats = get_digest_chats()
    for chat_id in chats:
        try:
            await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Digest error {chat_id}: {e}")


async def check_reminders(context: ContextTypes.DEFAULT_TYPE):
    for r in get_pending_reminders():
        try:
            await context.bot.send_message(
                chat_id=OWNER_ID,
                text=f"🔔 Напоминание: *{r['artist']}*",
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Reminder error: {e}")
        mark_reminder_sent(r['id'])


# ==================== MAIN ====================

def main():
    init_db()
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    for cmd, handler in [
        ("start",    cmd_start),
        ("add",      cmd_add),
        ("new",      cmd_add),
        ("list",     cmd_list),
        ("status",   cmd_status),
        ("today",    cmd_today),
        ("calendar", cmd_calendar),
        ("digest",   cmd_digest),
        ("code",     cmd_code),
        ("publish",  cmd_publish),
        ("remind",   cmd_remind),
        ("delete",   cmd_delete),
        ("help",     cmd_help),
    ]:
        app.add_handler(CommandHandler(cmd, handler))

    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(handle_callback))

    jq = app.job_queue
    if jq:
        jq.run_daily(send_morning_digest, time=dtime(hour=9, minute=0), name='digest')
        jq.run_repeating(check_reminders, interval=900, first=60, name='reminders')
        logger.info("Дайджест: 9:00 | Напоминания: каждые 15 мин")

    logger.info("🎸 MTB Concerts Bot (Collector Mode v2) запущен!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
