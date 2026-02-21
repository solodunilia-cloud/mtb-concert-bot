#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MTB Concerts Bot - Умный помощник для управления мероприятиями
mtbarmoscow.com

Логика работы:
- Бот понимает свободный текст вида "Алёна Апина — текст ок"
- Минимум команд, максимум распознавания контекста
- Утренний дайджест в 9:00
- Статусы по каждому мероприятию
"""

import os
import logging
import re
import sqlite3
from datetime import datetime, time as dtime
from typing import Optional, Dict, Any, List, Tuple

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

from tilda_api import TildaAPI
from google_sheets import GoogleSheetsManager
from template_generator import generate_page_html

# ==================== НАСТРОЙКИ ====================

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '8260143545:AAHUZqBpc7BdVYaMC3zQ9ZcB9HViBsYnEgQ')
TILDA_PUBLIC   = os.getenv('TILDA_PUBLIC',   'q3cf8fa6jyqm41o9qc')
TILDA_SECRET   = os.getenv('TILDA_SECRET',   'e6ba61619adad57acccd')
TILDA_PROJECT  = os.getenv('TILDA_PROJECT',  '11288143')
SHEETS_ID      = os.getenv('GOOGLE_SHEETS_ID', '')

DB_PATH = 'concerts.db'

tilda  = TildaAPI(TILDA_PUBLIC, TILDA_SECRET, TILDA_PROJECT)
sheets = GoogleSheetsManager(spreadsheet_id=SHEETS_ID if SHEETS_ID else None)

# Ключевые слова для распознавания типа данных
APPROVE_WORDS  = ['ок', 'ok', 'одобрено', 'утверждено', 'approved', 'готово', 'подходит', 'берём', 'берем', '✅']
POSTER_WORDS   = ['афиша', 'постер', 'картинка', 'poster', 'image', 'фото']
TICKET_WORDS   = ['билеты', 'билет', 'tickets', 'ticket', 'купить', 'продажа']
TEXT_WORDS     = ['текст', 'описание', 'text', 'description', 'desc', 'инфо', 'инфа']
DATE_WORDS     = ['дата', 'date', 'перенос', 'перенесли', 'перенесен', 'перенести']
CANCEL_WORDS   = ['отмена', 'отменён', 'отменен', 'отменили', 'cancelled', 'canceled']
YANDEX_WORDS   = ['яндекс', 'yandex', 'музыка', 'music']

# ==================== БАЗА ДАННЫХ ====================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS concerts (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            title            TEXT NOT NULL,
            date             TEXT,
            time             TEXT,
            image_url        TEXT,
            image_file_id    TEXT,
            tickets_url      TEXT,
            description      TEXT,
            yandex_music_url TEXT,
            status           TEXT DEFAULT 'draft',
            tilda_page_id    TEXT,
            tilda_url        TEXT,
            progress         INTEGER DEFAULT 0,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS pending_photos (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id    TEXT NOT NULL,
            chat_id    INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS digest_chats (
            chat_id INTEGER PRIMARY KEY
        )
    ''')
    conn.commit()
    conn.close()


def save_concert(data: Dict[str, Any]) -> int:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if data.get('id'):
        c.execute('''
            UPDATE concerts SET
                title=?, date=?, time=?, image_url=?, image_file_id=?,
                tickets_url=?, description=?, yandex_music_url=?,
                status=?, tilda_page_id=?, tilda_url=?, progress=?,
                updated_at=?
            WHERE id=?
        ''', (
            data.get('title'), data.get('date'), data.get('time'),
            data.get('image_url'), data.get('image_file_id'),
            data.get('tickets_url'), data.get('description'),
            data.get('yandex_music_url'), data.get('status', 'draft'),
            data.get('tilda_page_id'), data.get('tilda_url'),
            data.get('progress', 0), datetime.now().isoformat(),
            data['id']
        ))
        cid = data['id']
    else:
        c.execute('''
            INSERT INTO concerts
                (title, date, time, image_url, image_file_id, tickets_url,
                 description, yandex_music_url, status, progress)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        ''', (
            data.get('title'), data.get('date'), data.get('time'),
            data.get('image_url'), data.get('image_file_id'),
            data.get('tickets_url'), data.get('description'),
            data.get('yandex_music_url'), data.get('status', 'draft'),
            data.get('progress', 0)
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


def get_all_concerts() -> List[Dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('SELECT * FROM concerts ORDER BY date ASC, created_at DESC')
    rows = c.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def search_concert_by_name(name: str) -> List[Dict]:
    """Нечёткий поиск концерта по части названия"""
    all_c = [c for c in get_all_concerts() if c['status'] not in ('cancelled', 'published')]
    name_lower = name.lower().strip()
    results = []
    for c in all_c:
        tl = c['title'].lower()
        if name_lower == tl:
            return [c]
        if name_lower in tl or tl in name_lower:
            results.append(c)
        elif any(w in tl for w in name_lower.split() if len(w) > 3):
            results.append(c)
    return results


def calculate_progress(concert: Dict) -> int:
    fields = ['title', 'date', 'time', 'image_url', 'tickets_url', 'description']
    filled = sum(1 for f in fields if concert.get(f))
    return int(filled / len(fields) * 100)


def save_pending_photo(file_id: str, chat_id: int, message_id: int):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
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


def clear_pending_photo(photo_id: int):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('DELETE FROM pending_photos WHERE id=?', (photo_id,))
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


# ==================== ПАРСЕРЫ ====================

def parse_date_time(text: str) -> Tuple[Optional[str], Optional[str]]:
    months_ru = {
        'января':1, 'февраля':2, 'марта':3, 'апреля':4,
        'мая':5, 'июня':6, 'июля':7, 'августа':8,
        'сентября':9, 'октября':10, 'ноября':11, 'декабря':12
    }
    date_str = time_str = None
    text_low = text.lower()

    m = re.search(r'(\d{1,2})[./\-](\d{1,2})[./\-](\d{4})', text)
    if m:
        d, mo, y = m.groups()
        date_str = f"{int(d):02d}.{int(mo):02d}.{y}"

    if not date_str:
        pattern = r'(\d{1,2})\s+(' + '|'.join(months_ru.keys()) + r')(?:\s+(\d{4}))?'
        m = re.search(pattern, text_low)
        if m:
            d = m.group(1)
            mo = months_ru[m.group(2)]
            y = m.group(3) or str(datetime.now().year)
            date_str = f"{int(d):02d}.{mo:02d}.{y}"

    m = re.search(r'\b(\d{1,2})[:\.](\d{2})\b', text)
    if m:
        h, mi = m.groups()
        if 0 <= int(h) <= 23:
            time_str = f"{int(h):02d}:{int(mi):02d}"

    return date_str, time_str


def extract_urls(text: str) -> List[str]:
    return re.findall(r'https?://[^\s<>"\']+', text)


def classify_url(url: str) -> str:
    u = url.lower()
    if 'music.yandex' in u:
        return 'yandex_music'
    if any(x in u for x in ['afisha.yandex', 'widget.afisha', 'ticketmaster', 'kassy',
                              'ponominalu', 'kassir', 'concert.ru', 'radario', 'parter',
                              'bileter', 'ticketscloud']):
        return 'tickets'
    return 'unknown'


def has_word(text: str, words: List[str]) -> bool:
    t = text.lower()
    return any(w in t for w in words)


# ==================== ФОРМАТИРОВАНИЕ ====================

def status_card(concert: Dict) -> str:
    prog = concert.get('progress', 0)
    if concert['status'] == 'cancelled':
        icon = '🚫'
    elif concert['status'] == 'published':
        icon = '🟢'
    elif prog == 100:
        icon = '🟡'
    elif prog >= 50:
        icon = '🟠'
    else:
        icon = '🔴'

    date_line = f"{concert['date']} {concert['time'] or ''}".strip() if concert['date'] else '—'

    lines = [
        f"{icon} *#{concert['id']} {concert['title']}*",
        f"📅 {date_line}",
        f"📊 Прогресс: {prog}%",
        "",
        f"{'✅' if concert['image_url'] else '❌'} Афиша",
        f"{'✅' if concert['tickets_url'] else '❌'} Билеты",
        f"{'✅' if concert['description'] else '❌'} Текст",
        f"{'✅' if concert['date'] else '❌'} Дата",
    ]
    if concert.get('tilda_url'):
        lines.append(f"\n🔗 {concert['tilda_url']}")
    return '\n'.join(lines)


def missing_list(concert: Dict) -> str:
    missing = []
    if not concert.get('date'):        missing.append('📅 дата')
    if not concert.get('time'):        missing.append('🕐 время')
    if not concert.get('image_url'):   missing.append('🖼 афиша')
    if not concert.get('tickets_url'): missing.append('🎟 билеты')
    if not concert.get('description'): missing.append('📝 текст')
    return ', '.join(missing) if missing else '✅ всё есть'


def morning_digest_text() -> str:
    concerts = [c for c in get_all_concerts() if c['status'] not in ('published', 'cancelled')]
    if not concerts:
        return "☀️ Доброе утро! Активных мероприятий нет."

    now = datetime.now()
    lines = [f"☀️ *Доброе утро! Сводка на {now.strftime('%d.%m.%Y')}*\n"]

    with_date    = sorted([c for c in concerts if c['date']], key=lambda x: x['date'])
    without_date = [c for c in concerts if not c['date']]

    for c in with_date + without_date:
        prog = c.get('progress', 0)
        icon = '🟡' if prog == 100 else ('🟠' if prog >= 50 else '🔴')
        lines.append(f"{icon} *{c['title']}*")
        if c['date']:
            lines.append(f"   📅 {c['date']} {c['time'] or ''}")
        m = missing_list(c)
        if m != '✅ всё есть':
            lines.append(f"   ❗ Не хватает: {m}")
        else:
            lines.append(f"   ✅ Готово к публикации — /publish {c['id']}")
        lines.append("")

    return '\n'.join(lines)


# ==================== УМНОЕ РАСПОЗНАВАНИЕ ====================

async def try_smart_parse(update: Update, context: ContextTypes.DEFAULT_TYPE,
                           override_text: str = None) -> bool:
    """
    Парсим свободный текст: "Артист — [тип] [данные]"
    Возвращает True если что-то распознал и обработал.
    """
    message = update.message
    text = override_text or message.text or message.caption or ''
    if not text:
        return False

    # Разбиваем по разделителю: —, -, :, |
    parts = re.split(r'\s*[—\-:|]\s*', text, maxsplit=1)
    artist_name = None
    rest = text

    if len(parts) == 2 and len(parts[0].strip()) > 1:
        artist_name = parts[0].strip()
        rest = parts[1].strip()

    # Ищем концерт по имени
    found_concert = None
    if artist_name:
        matches = search_concert_by_name(artist_name)
        if len(matches) == 1:
            found_concert = matches[0]
        elif len(matches) > 1:
            keyboard = [
                [InlineKeyboardButton(f"#{c['id']} {c['title']}", callback_data=f"ctx_{c['id']}")]
                for c in matches[:5]
            ]
            keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data='ctx_cancel')])
            context.user_data['pending_text'] = text
            await message.reply_text(
                f"Нашёл несколько похожих — к какому?",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return True

    # Нет совпадения — берём активный концерт
    if not found_concert:
        current_id = context.user_data.get('current_concert_id')
        if current_id:
            found_concert = get_concert(current_id)
            rest = text  # весь текст как данные

    if not found_concert:
        return False

    rest_low = rest.lower()
    updated_fields = []

    # --- Отмена мероприятия ---
    if has_word(rest_low, CANCEL_WORDS):
        found_concert['status'] = 'cancelled'
        save_concert(found_concert)
        await message.reply_text(
            f"🚫 *{found_concert['title']}* отмечен как отменённый",
            parse_mode='Markdown'
        )
        return True

    # --- Одобрена афиша ---
    if has_word(rest_low, APPROVE_WORDS) and has_word(rest_low, POSTER_WORDS):
        pending = get_latest_pending_photo()
        if pending:
            await message.reply_text(
                f"⏳ Загружаю афишу для *{found_concert['title']}*...",
                parse_mode='Markdown'
            )
            image_url = await upload_photo_to_tilda(context, pending['file_id'])
            if image_url:
                found_concert['image_url']     = image_url
                found_concert['image_file_id'] = pending['file_id']
                found_concert['progress']      = calculate_progress(found_concert)
                save_concert(found_concert)
                sheets.sync_concert(found_concert)
                clear_pending_photo(pending['id'])
                await message.reply_text(
                    f"✅ Афиша для *{found_concert['title']}* загружена!\n"
                    f"📊 Прогресс: {found_concert['progress']}%",
                    parse_mode='Markdown'
                )
                await maybe_suggest_publish(message, found_concert)
            else:
                await message.reply_text("❌ Ошибка загрузки в Tilda")
        else:
            await message.reply_text(
                "Не нашёл недавно отправленных фото. Пришли картинку прямо в бот."
            )
        return True

    # --- Одобрен текст ---
    if has_word(rest_low, APPROVE_WORDS) and has_word(rest_low, TEXT_WORDS):
        context.user_data['awaiting']        = 'description'
        context.user_data['awaiting_for_id'] = found_concert['id']
        await message.reply_text(
            f"📝 Пришли текст описания для *{found_concert['title']}*:",
            parse_mode='Markdown'
        )
        return True

    # --- Общее "одобрено" без уточнения ---
    if has_word(rest_low, APPROVE_WORDS) and not has_word(rest_low, POSTER_WORDS + TEXT_WORDS):
        keyboard = [
            [InlineKeyboardButton("🖼 Афиша (последнее фото)", callback_data=f"approve_poster_{found_concert['id']}")],
            [InlineKeyboardButton("📝 Текст (пришлю следующим)", callback_data=f"approve_text_{found_concert['id']}")],
        ]
        await message.reply_text(
            f"Что одобрено для *{found_concert['title']}*?",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return True

    # --- URL ---
    urls = extract_urls(rest)
    for url in urls:
        url_type = classify_url(url)
        if url_type == 'tickets' and not found_concert.get('tickets_url'):
            found_concert['tickets_url'] = url
            updated_fields.append('🎟 Ссылка на билеты')
        elif url_type == 'yandex_music' and not found_concert.get('yandex_music_url'):
            found_concert['yandex_music_url'] = url
            updated_fields.append('🎵 Яндекс.Музыка')
        elif url_type == 'unknown' and has_word(rest_low, TICKET_WORDS) and not found_concert.get('tickets_url'):
            found_concert['tickets_url'] = url
            updated_fields.append('🎟 Ссылка на билеты')

    # --- Дата / время ---
    date_str, time_str = parse_date_time(rest)
    if date_str and (not found_concert.get('date') or has_word(rest_low, DATE_WORDS)):
        found_concert['date'] = date_str
        updated_fields.append('📅 Дата')
    if time_str and not found_concert.get('time'):
        found_concert['time'] = time_str
        updated_fields.append('🕐 Время')

    # --- Длинный текст без URL как описание ---
    if not urls and len(rest) > 80 and not found_concert.get('description'):
        if not has_word(rest_low, APPROVE_WORDS + POSTER_WORDS + TICKET_WORDS + DATE_WORDS):
            found_concert['description'] = rest
            updated_fields.append('📝 Текст описания')

    if updated_fields:
        found_concert['progress'] = calculate_progress(found_concert)
        save_concert(found_concert)
        sheets.sync_concert(found_concert)

        result = f"✅ *{found_concert['title']}* — обновлено:\n"
        result += '\n'.join(f"  {f}" for f in updated_fields)
        result += f"\n\n📊 Прогресс: {found_concert['progress']}%"

        await message.reply_text(result, parse_mode='Markdown')
        await maybe_suggest_publish(message, found_concert)
        return True

    return False


async def upload_photo_to_tilda(context: ContextTypes.DEFAULT_TYPE, file_id: str) -> Optional[str]:
    try:
        file = await context.bot.get_file(file_id)
        tmp = f"/tmp/poster_{file_id}.jpg"
        await file.download_to_drive(tmp)
        image_url = await tilda.upload_image(tmp)
        if os.path.exists(tmp):
            os.remove(tmp)
        return image_url
    except Exception as e:
        logger.error(f"upload_photo_to_tilda error: {e}")
        return None


async def maybe_suggest_publish(message, concert: Dict):
    if concert.get('progress', 0) == 100 and concert.get('status') == 'draft':
        keyboard = [[InlineKeyboardButton("⚡ Опубликовать на сайт", callback_data=f"publish_{concert['id']}")]]
        await message.reply_text(
            f"🎉 *{concert['title']}* готов на 100%! Публикуем?",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )


# ==================== КОМАНДЫ ====================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_chat(update.message.chat_id)
    text = """🎸 *MTB Concerts Manager*

Привет! Пиши как в обычном чате:

*Примеры:*
`Алёна Апина — афиша ок` → привяжет последнее фото
`Алёна Апина — текст ок` → попросит описание
`Алёна Апина — билеты https://...` → сохранит ссылку
`Алёна Апина — 15 марта 21:00` → дата и время
`Алёна Апина — отмена` → отметить как отменённое

*Команды:*
/new Название — создать мероприятие
/list — все мероприятия
/status 5 — карточка по номеру
/select 5 — выбрать активное
/edit 5 — редактировать
/publish 5 — опубликовать
/digest — сводка прямо сейчас

Каждое утро в 9:00 пришлю сводку 🌅
"""
    await update.message.reply_text(text, parse_mode='Markdown')


async def cmd_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_chat(update.message.chat_id)
    title = ' '.join(context.args).strip() if context.args else ''
    if not title:
        await update.message.reply_text("Укажи название:\n`/new Алёна Апина`", parse_mode='Markdown')
        return

    cid = save_concert({'title': title, 'status': 'draft', 'progress': 17})
    context.user_data['current_concert_id'] = cid

    await update.message.reply_text(
        f"✅ Создано: *#{cid} {title}*\n\n"
        f"Теперь пиши:\n`{title} — [афиша/текст/билеты/дата]`\n\n"
        f"Или просто шли данные — привяжу к этому мероприятию.",
        parse_mode='Markdown'
    )


async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_chat(update.message.chat_id)
    concerts = get_all_concerts()
    if not concerts:
        await update.message.reply_text("Мероприятий нет. Создай: `/new Название`", parse_mode='Markdown')
        return

    active    = [c for c in concerts if c['status'] == 'draft']
    published = [c for c in concerts if c['status'] == 'published']
    cancelled = [c for c in concerts if c['status'] == 'cancelled']

    text = f"📋 *Всего: {len(concerts)}*\n\n"

    if active:
        text += "🔵 *В работе:*\n"
        for c in active:
            icon = '🟡' if c['progress'] == 100 else ('🟠' if c['progress'] >= 50 else '🔴')
            d = f" — {c['date']}" if c['date'] else ''
            text += f"{icon} #{c['id']} {c['title']}{d} ({c['progress']}%)\n"
        text += "\n"

    if published:
        text += "🟢 *Опубликованы:*\n"
        for c in published[:5]:
            text += f"  #{c['id']} {c['title']} — {c['date'] or '?'}\n"
        text += "\n"

    if cancelled:
        text += f"🚫 Отменены: {len(cancelled)}\n"

    text += "\n`/status [номер]` — детали | `/select [номер]` — выбрать активное"
    await update.message.reply_text(text, parse_mode='Markdown')


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        try:
            cid = int(context.args[0])
        except ValueError:
            await update.message.reply_text("Неверный номер")
            return
    else:
        cid = context.user_data.get('current_concert_id')
        if not cid:
            await update.message.reply_text("Укажи номер: `/status 5`", parse_mode='Markdown')
            return

    concert = get_concert(cid)
    if not concert:
        await update.message.reply_text(f"Мероприятие #{cid} не найдено")
        return

    text = status_card(concert)
    m = missing_list(concert)
    if m != '✅ всё есть':
        text += f"\n\n❗ *Не хватает:* {m}"

    keyboard = []
    if concert['progress'] == 100 and concert['status'] == 'draft':
        keyboard.append([InlineKeyboardButton("⚡ Опубликовать", callback_data=f"publish_{cid}")])
    keyboard.append([InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_menu_{cid}")])

    await update.message.reply_text(
        text, parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def cmd_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Укажи номер: `/select 5`", parse_mode='Markdown')
        return
    try:
        cid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Неверный номер")
        return

    concert = get_concert(cid)
    if not concert:
        await update.message.reply_text(f"Мероприятие #{cid} не найдено")
        return

    context.user_data['current_concert_id'] = cid
    await update.message.reply_text(
        f"✅ Активное: *#{cid} {concert['title']}*\n\n"
        f"Всё что шлёшь без имени артиста — идёт сюда.",
        parse_mode='Markdown'
    )


async def cmd_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        try:
            cid = int(context.args[0])
        except ValueError:
            cid = context.user_data.get('current_concert_id')
    else:
        cid = context.user_data.get('current_concert_id')

    if not cid:
        await update.message.reply_text("Укажи номер: `/edit 5`", parse_mode='Markdown')
        return

    concert = get_concert(cid)
    if not concert:
        await update.message.reply_text(f"Мероприятие #{cid} не найдено")
        return

    context.user_data['current_concert_id'] = cid
    keyboard = [
        [InlineKeyboardButton("📅 Дата/время", callback_data=f"set_date_{cid}"),
         InlineKeyboardButton("📝 Текст",       callback_data=f"set_desc_{cid}")],
        [InlineKeyboardButton("🎟 Билеты",      callback_data=f"set_tickets_{cid}"),
         InlineKeyboardButton("🖼 Афиша",       callback_data=f"set_image_{cid}")],
        [InlineKeyboardButton("🎵 Яндекс",      callback_data=f"set_yandex_{cid}"),
         InlineKeyboardButton("✏️ Название",    callback_data=f"set_title_{cid}")],
        [InlineKeyboardButton("🚫 Отменить мероприятие", callback_data=f"cancel_event_{cid}")],
    ]
    await update.message.reply_text(
        f"✏️ *#{cid} {concert['title']}*\nЧто изменить?",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


async def cmd_digest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_chat(update.message.chat_id)
    await update.message.reply_text(morning_digest_text(), parse_mode='Markdown')


async def cmd_publish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        try:
            cid = int(context.args[0])
        except ValueError:
            await update.message.reply_text("Укажи номер: `/publish 5`", parse_mode='Markdown')
            return
    else:
        cid = context.user_data.get('current_concert_id')
        if not cid:
            await update.message.reply_text("Укажи номер: `/publish 5`", parse_mode='Markdown')
            return

    concert = get_concert(cid)
    if not concert:
        await update.message.reply_text(f"#{cid} не найдено")
        return

    if concert['progress'] < 100:
        m = missing_list(concert)
        keyboard = [[
            InlineKeyboardButton("Да, публиковать", callback_data=f"publish_{cid}"),
            InlineKeyboardButton("Нет", callback_data="noop")
        ]]
        await update.message.reply_text(
            f"⚠️ Готово на {concert['progress']}%\n❗ Не хватает: {m}\n\nВсё равно публиковать?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    await do_publish(update.message, context, cid)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = """🎸 *Справка MTB Concerts Bot*

*Свободный формат:*
`Алёна Апина — афиша ок` → привяжет последнее фото как афишу
`Алёна Апина — текст ок` → попросит прислать описание
`Алёна Апина — билеты https://...` → ссылка на продажу
`Алёна Апина — 15 марта 21:00` → дата и время
`Алёна Апина — отмена` → мероприятие отменено

*Без имени артиста* — данные идут в активное мероприятие (/select)

*Команды:*
/new Название — создать мероприятие
/list — все мероприятия и статусы
/status [номер] — подробная карточка
/select [номер] — выбрать активное
/edit [номер] — меню редактирования
/publish [номер] — создать страницу в Tilda
/digest — сводка прямо сейчас

*Утренний дайджест* — каждый день в 9:00 🌅
"""
    await update.message.reply_text(text, parse_mode='Markdown')


# ==================== ОБРАБОТКА СООБЩЕНИЙ ====================

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    text = message.text or ''

    # Ожидаем конкретный ввод?
    awaiting = context.user_data.get('awaiting')
    if awaiting:
        await handle_awaiting_input(update, context, text)
        return

    recognized = await try_smart_parse(update, context)
    if not recognized:
        current_id = context.user_data.get('current_concert_id')
        if current_id:
            concert = get_concert(current_id)
            if concert:
                await message.reply_text(
                    f"Не понял 🤔\n\n"
                    f"Активное: *#{current_id} {concert['title']}*\n"
                    f"Пиши: `{concert['title']} — [что добавить]`\n"
                    f"Или /edit {current_id} для меню",
                    parse_mode='Markdown'
                )
                return
        await message.reply_text(
            "Не понял 🤔\n\n"
            "Попробуй:\n"
            "`Артист — афиша ок`\n"
            "`Артист — 15 марта 21:00`\n"
            "Или /list для выбора мероприятия",
            parse_mode='Markdown'
        )


async def handle_awaiting_input(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    awaiting = context.user_data.pop('awaiting', None)
    cid      = context.user_data.pop('awaiting_for_id', None)

    if not cid:
        return
    concert = get_concert(cid)
    if not concert:
        await update.message.reply_text("Мероприятие не найдено")
        return

    if awaiting == 'description':
        concert['description'] = text
        label = '📝 Текст описания'
    elif awaiting == 'tickets_url':
        urls = extract_urls(text)
        concert['tickets_url'] = urls[0] if urls else text
        label = '🎟 Ссылка на билеты'
    elif awaiting == 'yandex_music_url':
        urls = extract_urls(text)
        concert['yandex_music_url'] = urls[0] if urls else text
        label = '🎵 Яндекс.Музыка'
    elif awaiting == 'date_time':
        d, t = parse_date_time(text)
        if d: concert['date'] = d
        if t: concert['time'] = t
        label = f"📅 {d or ''} {t or ''}".strip()
    elif awaiting == 'title':
        concert['title'] = text
        label = f"✏️ Название: {text}"
    else:
        return

    concert['progress'] = calculate_progress(concert)
    save_concert(concert)
    sheets.sync_concert(concert)

    await update.message.reply_text(
        f"✅ *{concert['title']}* — {label} сохранено\n📊 Прогресс: {concert['progress']}%",
        parse_mode='Markdown'
    )
    await maybe_suggest_publish(update.message, concert)


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    photo   = message.photo[-1]
    caption = message.caption or ''

    # Сохраняем фото как pending
    save_pending_photo(photo.file_id, message.chat_id, message.message_id)

    # Есть подпись — пробуем распознать
    if caption:
        recognized = await try_smart_parse(update, context, override_text=caption)
        if recognized:
            return

    # Ожидаем фото для конкретного мероприятия?
    awaiting_image_for = context.user_data.pop('awaiting_image_for', None)
    if awaiting_image_for:
        concert = get_concert(awaiting_image_for)
        if concert:
            await message.reply_text(f"⏳ Загружаю афишу для *{concert['title']}*...", parse_mode='Markdown')
            image_url = await upload_photo_to_tilda(context, photo.file_id)
            if image_url:
                concert['image_url']     = image_url
                concert['image_file_id'] = photo.file_id
                concert['progress']      = calculate_progress(concert)
                save_concert(concert)
                sheets.sync_concert(concert)
                # Удаляем из pending т.к. уже привязали
                pending = get_latest_pending_photo()
                if pending and pending['file_id'] == photo.file_id:
                    clear_pending_photo(pending['id'])
                await message.reply_text(
                    f"✅ Афиша загружена!\n📊 Прогресс: {concert['progress']}%",
                    parse_mode='Markdown'
                )
                await maybe_suggest_publish(message, concert)
            else:
                await message.reply_text("❌ Ошибка загрузки в Tilda")
            return

    # Нет подписи — спрашиваем к чему привязать
    current_id = context.user_data.get('current_concert_id')
    if current_id:
        concert = get_concert(current_id)
        keyboard = [
            [InlineKeyboardButton(
                f"✅ Афиша для #{current_id} {concert['title']}",
                callback_data=f"approve_poster_{current_id}"
            )],
            [InlineKeyboardButton("📋 Другое мероприятие", callback_data="choose_for_photo")],
            [InlineKeyboardButton("🗑 Не привязывать",      callback_data="photo_ignore")],
        ]
        await message.reply_text(
            "📸 Фото получено! К какому мероприятию привязать?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        concerts = [c for c in get_all_concerts() if c['status'] == 'draft'][:6]
        if concerts:
            keyboard = [
                [InlineKeyboardButton(f"#{c['id']} {c['title']}", callback_data=f"approve_poster_{c['id']}")]
                for c in concerts
            ]
            keyboard.append([InlineKeyboardButton("🗑 Не привязывать", callback_data="photo_ignore")])
            await message.reply_text(
                "📸 Фото получено! К какому мероприятию привязать?",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await message.reply_text(
                "📸 Фото сохранено. Создай мероприятие (/new) — потом привяжу."
            )


# ==================== CALLBACK КНОПКИ ====================

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    # Публикация
    if data.startswith('publish_'):
        cid = int(data.split('_')[1])
        await do_publish_query(query, context, cid)

    # Одобрить афишу
    elif data.startswith('approve_poster_'):
        cid = int(data.split('_')[2])
        concert = get_concert(cid)
        if not concert:
            await query.edit_message_text("Мероприятие не найдено")
            return
        pending = get_latest_pending_photo()
        if not pending:
            await query.edit_message_text("Фото не найдено. Пришли картинку снова.")
            return
        await query.edit_message_text(f"⏳ Загружаю афишу для {concert['title']}...")
        image_url = await upload_photo_to_tilda(context, pending['file_id'])
        if image_url:
            concert['image_url']     = image_url
            concert['image_file_id'] = pending['file_id']
            concert['progress']      = calculate_progress(concert)
            save_concert(concert)
            sheets.sync_concert(concert)
            clear_pending_photo(pending['id'])
            text = f"✅ Афиша для *{concert['title']}* загружена!\n📊 Прогресс: {concert['progress']}%"
            if concert['progress'] == 100:
                keyboard = [[InlineKeyboardButton("⚡ Опубликовать", callback_data=f"publish_{cid}")]]
                await query.edit_message_text(
                    text + "\n\n🎉 Всё готово! Публикуем?",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='Markdown'
                )
            else:
                await query.edit_message_text(text, parse_mode='Markdown')
        else:
            await query.edit_message_text("❌ Ошибка загрузки в Tilda")

    # Одобрить текст
    elif data.startswith('approve_text_'):
        cid = int(data.split('_')[2])
        concert = get_concert(cid)
        context.user_data['awaiting']        = 'description'
        context.user_data['awaiting_for_id'] = cid
        await query.edit_message_text(
            f"📝 Пришли текст описания для *{concert['title']}*:",
            parse_mode='Markdown'
        )

    # Меню редактирования
    elif data.startswith('edit_menu_'):
        cid = int(data.split('_')[2])
        concert = get_concert(cid)
        keyboard = [
            [InlineKeyboardButton("📅 Дата/время", callback_data=f"set_date_{cid}"),
             InlineKeyboardButton("📝 Текст",       callback_data=f"set_desc_{cid}")],
            [InlineKeyboardButton("🎟 Билеты",      callback_data=f"set_tickets_{cid}"),
             InlineKeyboardButton("🖼 Афиша",       callback_data=f"set_image_{cid}")],
            [InlineKeyboardButton("🎵 Яндекс",      callback_data=f"set_yandex_{cid}"),
             InlineKeyboardButton("✏️ Название",    callback_data=f"set_title_{cid}")],
            [InlineKeyboardButton("🚫 Отменить мероприятие", callback_data=f"cancel_event_{cid}")],
        ]
        await query.edit_message_text(
            f"✏️ *#{cid} {concert['title']}*\nЧто изменить?",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

    # set_* поля
    elif data.startswith('set_'):
        parts = data.split('_', 2)
        field = parts[1]
        cid   = int(parts[2])
        concert = get_concert(cid)
        context.user_data['current_concert_id'] = cid

        prompts = {
            'date':    ('date_time',        f"📅 Введи дату и время для *{concert['title']}*:\nПример: 15 марта 21:00"),
            'desc':    ('description',      f"📝 Введи текст описания для *{concert['title']}*:"),
            'tickets': ('tickets_url',      f"🎟 Введи ссылку на билеты для *{concert['title']}*:"),
            'image':   ('__image__',        f"🖼 Пришли фото афиши для *{concert['title']}*:"),
            'yandex':  ('yandex_music_url', f"🎵 Введи ссылку на Яндекс.Музыку для *{concert['title']}*:"),
            'title':   ('title',            f"✏️ Введи новое название (сейчас: {concert['title']}):"),
        }

        if field not in prompts:
            return

        key, prompt = prompts[field]
        if key == '__image__':
            context.user_data['awaiting_image_for'] = cid
            await query.edit_message_text(prompt + "\n\n(просто пришли картинку следующим сообщением)", parse_mode='Markdown')
        else:
            context.user_data['awaiting']        = key
            context.user_data['awaiting_for_id'] = cid
            await query.edit_message_text(prompt, parse_mode='Markdown')

    # Отмена мероприятия
    elif data.startswith('cancel_event_'):
        cid = int(data.split('_')[2])
        concert = get_concert(cid)
        concert['status'] = 'cancelled'
        save_concert(concert)
        await query.edit_message_text(f"🚫 *{concert['title']}* отмечен как отменённый", parse_mode='Markdown')

    # Контекстный выбор концерта
    elif data.startswith('ctx_'):
        val = data[4:]
        if val == 'cancel':
            await query.edit_message_text("Отменено")
            return
        cid = int(val)
        concert = get_concert(cid)
        context.user_data['current_concert_id'] = cid
        await query.edit_message_text(
            f"✅ Буду работать с *{concert['title']}*\n\nПришли данные снова.",
            parse_mode='Markdown'
        )

    # Выбор мероприятия для фото
    elif data == 'choose_for_photo':
        concerts = [c for c in get_all_concerts() if c['status'] == 'draft'][:8]
        keyboard = [
            [InlineKeyboardButton(f"#{c['id']} {c['title']}", callback_data=f"approve_poster_{c['id']}")]
            for c in concerts
        ]
        await query.edit_message_text(
            "Выбери мероприятие:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data in ('photo_ignore', 'noop'):
        await query.edit_message_text("Ок")


# ==================== ПУБЛИКАЦИЯ ====================

async def do_publish(message, context: ContextTypes.DEFAULT_TYPE, cid: int):
    concert = get_concert(cid)
    await message.reply_text(f"⏳ Создаю страницу для *{concert['title']}*...", parse_mode='Markdown')
    try:
        html   = generate_page_html(concert)
        result = await tilda.create_page(title=concert['title'], html=html)
        if result:
            concert['tilda_page_id'] = result.get('id')
            concert['tilda_url']     = result.get('url')
            concert['status']        = 'published'
            save_concert(concert)
            sheets.sync_concert(concert)
            keyboard = [[InlineKeyboardButton("🔗 Открыть в Tilda", url=concert['tilda_url'])]]
            await message.reply_text(
                f"✅ *{concert['title']}* опубликован!\n🔗 {concert['tilda_url']}",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
        else:
            await message.reply_text("❌ Ошибка создания страницы в Tilda")
    except Exception as e:
        logger.error(f"Publish error: {e}")
        await message.reply_text(f"❌ Ошибка: {e}")


async def do_publish_query(query, context: ContextTypes.DEFAULT_TYPE, cid: int):
    concert = get_concert(cid)
    await query.edit_message_text(f"⏳ Создаю страницу для {concert['title']}...")
    try:
        html   = generate_page_html(concert)
        result = await tilda.create_page(title=concert['title'], html=html)
        if result:
            concert['tilda_page_id'] = result.get('id')
            concert['tilda_url']     = result.get('url')
            concert['status']        = 'published'
            save_concert(concert)
            sheets.sync_concert(concert)
            keyboard = [[InlineKeyboardButton("🔗 Открыть в Tilda", url=concert['tilda_url'])]]
            await query.edit_message_text(
                f"✅ {concert['title']} опубликован!\n🔗 {concert['tilda_url']}",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await query.edit_message_text("❌ Ошибка создания страницы в Tilda")
    except Exception as e:
        logger.error(f"Publish error: {e}")
        await query.edit_message_text(f"❌ Ошибка: {e}")


# ==================== УТРЕННИЙ ДАЙДЖЕСТ ====================

async def send_morning_digest(context: ContextTypes.DEFAULT_TYPE):
    text  = morning_digest_text()
    chats = get_digest_chats()
    for chat_id in chats:
        try:
            await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Digest send error to {chat_id}: {e}")


# ==================== MAIN ====================

def main():
    init_db()

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("new",     cmd_new))
    app.add_handler(CommandHandler("list",    cmd_list))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CommandHandler("select",  cmd_select))
    app.add_handler(CommandHandler("edit",    cmd_edit))
    app.add_handler(CommandHandler("publish", cmd_publish))
    app.add_handler(CommandHandler("digest",  cmd_digest))
    app.add_handler(CommandHandler("help",    cmd_help))

    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(handle_callback))

    # Утренний дайджест в 9:00
    job_queue = app.job_queue
    if job_queue:
        job_queue.run_daily(
            send_morning_digest,
            time=dtime(hour=9, minute=0),
            name='morning_digest'
        )
        logger.info("Утренний дайджест настроен на 9:00")

    logger.info("🎸 MTB Concerts Bot запущен!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
