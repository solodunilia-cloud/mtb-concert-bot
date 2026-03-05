#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MTB Concerts Bot — v6
Google Sheets = единственная база данных. SQLite убран полностью.
При старте бот читает все концерты из Sheets в память.
При любом изменении — пишет в Sheets и обновляет память.
"""

import os
import re
import logging
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

sheets = GoogleSheetsManager(spreadsheet_id=SHEETS_ID if SHEETS_ID else None)

KW = {
    'tickets': ['билеты', 'билет', 'ticket', 'tickets'],
    'poster':  ['афиша', 'poster'],
    'text':    ['текст', 'описание', 'text'],
    'date':    ['дата', 'date', 'перенос'],
    'cancel':  ['отмена', 'отменен', 'отменён', 'отменили', 'cancel'],
}
POSTER_OK = ['одобрена', 'ок', 'ok', 'утверждена', 'готова', 'approved']

# ─── IN-MEMORY ХРАНИЛИЩЕ ──────────────────────────────────────────────────────
# Загружается из Google Sheets при старте. Sheets = источник правды.

_concerts: List[dict] = []   # все концерты
_chats:    List[int]  = []   # зарегистрированные chat_id

def _concerts_sorted(include_cancelled=False) -> List[dict]:
    items = _concerts if include_cancelled else [c for c in _concerts if c.get('status') != 'cancelled']
    return sorted(items, key=lambda c: (c.get('date') or '9999', -c.get('id', 0)))

def db_get(cid: int) -> Optional[dict]:
    for c in _concerts:
        if c.get('id') == cid:
            return c
    return None

def db_all(include_cancelled=False) -> List[dict]:
    return _concerts_sorted(include_cancelled)

def db_save(data: dict) -> int:
    """Создаёт или обновляет концерт в памяти. Запись в Sheets — отдельно через sheets.sync_concert."""
    if data.get('id'):
        existing = db_get(data['id'])
        if existing:
            existing.update({k: v for k, v in data.items() if k != '_row'})
            return data['id']
    # Новый концерт
    new_id = sheets.next_id(_concerts) if sheets.is_connected() else (max((c['id'] for c in _concerts), default=0) + 1)
    new_c  = {
        'id':               new_id,
        'artist':           data.get('artist', ''),
        'date':             data.get('date'),
        'time':             data.get('time'),
        'poster_status':    data.get('poster_status', 'none'),
        'poster_file_id':   data.get('poster_file_id'),
        'tickets_url':      data.get('tickets_url'),
        'description_text': data.get('description_text'),
        'status':           data.get('status', 'draft'),
        'created_at':       datetime.now().isoformat(),
        'updated_at':       datetime.now().isoformat(),
        '_row':             None,
    }
    _concerts.append(new_c)
    return new_id

def db_delete(cid: int):
    global _concerts
    c = db_get(cid)
    if c:
        sheets.delete_concert(c, _concerts)
        _concerts = [x for x in _concerts if x.get('id') != cid]

def register_chat(chat_id: int):
    if chat_id not in _chats:
        _chats.append(chat_id)
        sheets.save_chat(chat_id, _chats)

def get_chats() -> List[int]:
    return list(_chats)

# ─── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ──────────────────────────────────────────────────

# Латиница → кириллица для часто путаемых символов
_CYR_LAT = str.maketrans('aceopxyABCEHKMOPTX', 'асеорхуАВСЕНКМОРТХ')

def norm(text: str) -> str:
    """Нормализация с заменой латиницы на кириллицу."""
    text = text.lower()
    text = text.translate(_CYR_LAT)
    text = text.replace('ё', 'е')
    text = text.replace('`', '').replace("'", '').replace('\u2019', '').replace('\u02bc', '')  # апострофы
    text = re.sub(r'[^\w\s]', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()

def transliterate(text: str) -> str:
    """ИВАН ДОРН → ivan-dorn"""
    table = {
        'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e',
        'ж':'zh','з':'z','и':'i','й':'y','к':'k','л':'l','м':'m',
        'н':'n','о':'o','п':'p','р':'r','с':'s','т':'t','у':'u',
        'ф':'f','х':'kh','ц':'ts','ч':'ch','ш':'sh','щ':'sch',
        'ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya',
    }
    result = ''
    for ch in text.lower():
        if ch in table:
            result += table[ch]
        elif ch.isascii() and (ch.isalnum() or ch == ' '):
            result += ch
    result = re.sub(r'\s+', '-', result.strip())
    return re.sub(r'-+', '-', result)

def make_slug(artist: str, date_str: str = '') -> str:
    """Иван Дорн + 15.04.2026 → ivan-dorn-15-04-2026"""
    slug = transliterate(artist)
    if date_str:
        slug += '-' + date_str.replace('.', '-')
    return slug.lower()

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

    # Время HH:MM или HH.MM — ищем ТОЛЬКО после удаления всех дат из текста
    # чтобы "15.04.2026" не давало время "15:04"
    text_no_date = re.sub(r'\d{1,2}[./\-]\d{1,2}[./\-]\d{4}', '', text)
    text_no_date = re.sub(r'\b\d{1,2}[./]\d{1,2}\b', '', text_no_date)
    for mo in MONTHS:
        text_no_date = re.sub(r'(?i)\b' + mo + r'\b', '', text_no_date)
    # Ищем время только в очищенном тексте, и только HH:MM (двоеточие) или HH.MM
    m = re.search(r'\b(\d{1,2}):(\d{2})\b', text_no_date)
    if not m:
        # Точка — только если это явно время (не часть числа/даты)
        m = re.search(r'(?<!\d)(\d{1,2})\.(\d{2})(?!\d)', text_no_date)
    if m:
        h, mi = m.groups()
        if 0 <= int(h) <= 23 and 0 <= int(mi) <= 59:
            time_str = f"{int(h):02d}:{int(mi):02d}"

    return date_str, time_str

def strip_date_time(text: str) -> str:
    """Убирает дату и время из текста."""
    cleaned = re.sub(r'\d{1,2}[./\-]\d{1,2}[./\-]\d{4}', '', text)
    cleaned = re.sub(r'\b\d{1,2}[./]\d{1,2}\b', '', cleaned)
    # Убираем время — HH:MM или HH.MM (включая без \b перед числом)
    cleaned = re.sub(r'(?<!\d)\d{1,2}[:.]\d{2}(?!\d)', '', cleaned)
    for mo in MONTHS:
        cleaned = re.sub(r'(?i)\b' + mo + r'\b', '', cleaned)
    return re.sub(r'\s+', ' ', cleaned).strip()

def fuzzy_find(name: str, soft=False) -> List[dict]:
    """
    soft=True — возвращает также совпадения 60-69 (для "ты имел в виду?")
    """
    all_c  = db_all()
    name_n = norm(name)
    results = []
    for c in all_c:
        c_n   = norm(c['artist'])
        score = max(
            fuzz.token_set_ratio(name_n, c_n),
            fuzz.partial_ratio(name_n, c_n),
            fuzz.WRatio(name_n, c_n),
        )
        threshold = 60 if soft else 65
        if score >= threshold:
            results.append((c, score))
    if not results:
        return []
    results.sort(key=lambda x: x[1], reverse=True)
    top = results[0][1]
    # Одно явное совпадение
    if top >= 90 or (len(results) >= 2 and top - results[1][1] >= 20):
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

# Флаг уведомлений (вкл/выкл через /notify_on и /notify_off)
_notify_enabled: bool = True

def edit_kb(cid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🗑 Сброс даты",    callback_data=f"clr|date|{cid}"),
         InlineKeyboardButton("📅 Дата",           callback_data=f"ed|date|{cid}")],
        [InlineKeyboardButton("🗑 Сброс афиши",   callback_data=f"clr|poster|{cid}"),
         InlineKeyboardButton("🖼 Афиша ✓",        callback_data=f"do|poster|{cid}")],
        [InlineKeyboardButton("🗑 Сброс билетов", callback_data=f"clr|tickets|{cid}"),
         InlineKeyboardButton("🎟 Билеты",         callback_data=f"ed|tickets|{cid}")],
        [InlineKeyboardButton("🗑 Сброс текста",  callback_data=f"clr|text|{cid}"),
         InlineKeyboardButton("📝 Текст",          callback_data=f"ed|text|{cid}")],
        [InlineKeyboardButton("🚫 Отменить",       callback_data=f"do|cancel|{cid}"),
         InlineKeyboardButton("✏️ Артист",         callback_data=f"ed|artist|{cid}")],
        [InlineKeyboardButton("🗑 Удалить",        callback_data=f"do|delete|{cid}"),
         InlineKeyboardButton("⚫ Опубликовать",    callback_data=f"do|publish|{cid}")],
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

            # 1. Прямое вхождение подстроки (ловит "билеты!" "билеты,")
            if kw_n in text_n:
                return action, kw

            # 2. Fuzzy по каждому слову (ловит "билетсы", "тикетс")
            for w in words:
                if len(w) >= 4 and fuzz.partial_ratio(kw_n, w) >= 80:
                    return action, w

            # 3. Fuzzy по всей строке (ловит опечатки в разных позициях)
            if fuzz.partial_ratio(kw_n, text_n) >= 85:
                return action, kw

    return None

STOP_WORDS = [
    'пожалуйста', 'плиз', 'please', 'брат', 'срочно', 'давай',
    'поставь', 'добавь', 'обнови', 'вот', 'держи', 'смотри',
]

def parse_trigger(text: str) -> Optional[dict]:
    """Любой порядок: Артист билеты URL / билеты Артист URL / и т.д."""
    text = text.strip()
    if not text:
        return None
    result = detect_kw(text)
    if not result:
        return None
    action, found_kw = result

    # ✅ Для текста — парсим ДО/ПОСЛЕ ключевого слова СРАЗУ,
    # до проверки длины артиста (иначе длинный текст убивает триггер)
    if action == 'text':
        for kw in KW['text']:
            pattern = re.compile(re.escape(kw), re.IGNORECASE)
            m = pattern.search(text)
            if m:
                before = text[:m.start()].strip()
                after  = text[m.end():].strip()
                if after and len(before) >= 2:
                    artist = before
                    for sw in STOP_WORDS:
                        artist = re.sub(r'(?i)\b' + re.escape(sw) + r'\b', '', artist)
                    artist = re.sub(r'\s+', ' ', artist).strip()
                    if artist and len(artist) >= 2:
                        return {'artist': artist, 'action': 'text', 'payload': after}
        return None

    # Убираем URL, ключевые слова, даты — остаток = артист
    cleaned = re.sub(r'https?://\S+', '', text)
    all_kw  = [w for ws in KW.values() for w in ws] + POSTER_OK
    for kw in all_kw:
        cleaned = re.sub(r'(?i)\b' + re.escape(kw) + r'\b', '', cleaned)
    cleaned = strip_date_time(cleaned)

    # Убираем стоп-слова (мусор)
    for sw in STOP_WORDS:
        cleaned = re.sub(r'(?i)\b' + re.escape(sw) + r'\b', '', cleaned)

    artist = re.sub(r'\s+', ' ', cleaned).strip()

    if not artist or len(artist) < 2:
        return None

    # Защита от мусора — слишком длинное "имя артиста"
    if len(artist.split()) > 6:
        return None

    # URL всегда берём из исходного текста
    url     = extract_url(text)
    payload = url or ''

    if action == 'tickets':
        # Артист — всё ДО ключевого слова (без URL)
        for kw in KW['tickets']:
            pattern = re.compile(re.escape(kw), re.IGNORECASE)
            m = pattern.search(text)
            if m:
                before = re.sub(r'https?://\S+', '', text[:m.start()]).strip()
                if before and len(before) >= 2:
                    artist = re.sub(r'\s+', ' ', before).strip()
                break

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
        sheets.sync_concert(c, _concerts)
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
                db_save(c); sheets.sync_concert(c, _concerts)
                await notify_ready(ctx, c)
                await q.edit_message_text(f"✅ Билеты добавлены — *{c['artist']}*", parse_mode='Markdown')

        elif action == 'poster':
            c['poster_status'] = 'approved'
            db_save(c); sheets.sync_concert(c, _concerts)
            await notify_ready(ctx, c)
            await q.edit_message_text(f"✅ Афиша одобрена — *{c['artist']}*", parse_mode='Markdown')

        elif action == 'text':
            txt = ctx.user_data.pop(f'v_{cid}', None)
            if txt:
                c['description_text'] = txt
                db_save(c); sheets.sync_concert(c, _concerts)
                await notify_ready(ctx, c)
                await q.edit_message_text(f"✅ Текст добавлен — *{c['artist']}*", parse_mode='Markdown')

        elif action == 'date':
            val = ctx.user_data.pop(f'v_{cid}', None)
            if val:
                d, t = val
                c['date'] = d
                if t: c['time'] = t
                db_save(c); sheets.sync_concert(c, _concerts)
                await notify_ready(ctx, c)
                await q.edit_message_text(f"✅ Дата установлена — *{c['artist']}*", parse_mode='Markdown')

        elif action == 'cancel':
            c['status'] = 'cancelled'
            db_save(c); sheets.sync_concert(c, _concerts)
            kb = [[InlineKeyboardButton("♻️ Восстановить", callback_data=f"do|restore|{cid}")]]
            await q.edit_message_text(f"🚫 *{c['artist']}* — отменён", parse_mode='Markdown',
                                      reply_markup=InlineKeyboardMarkup(kb))

        elif action == 'restore':
            c['status'] = 'draft'
            db_save(c); sheets.sync_concert(c, _concerts)
            await q.edit_message_text(card(c), reply_markup=edit_kb(cid), parse_mode='Markdown')

        elif action == 'publish':
            c['status'] = 'published'
            db_save(c); sheets.sync_concert(c, _concerts)
            slug = make_slug(c.get('artist', ''))
            page_url = f"https://mtbarmoscow.com/{slug}"
            await q.edit_message_text(
                f"⚫ *{c['artist']}* — опубликован\n\n🔗 Ссылка для рекламы:\n{page_url}",
                parse_mode='Markdown'
            )

        elif action == 'delete':
            name = c['artist']
            c['status'] = 'cancelled'
            db_save(c); sheets.sync_concert(c, _concerts)
            await q.edit_message_text(f"🗑 *{name}* — перемещён в архив", parse_mode='Markdown')
        return

    if data.startswith('clr|'):
        _, field, cid_s = data.split('|')
        cid = int(cid_s)
        c   = db_get(cid)
        if not c:
            await q.edit_message_text("Не найдено")
            return
        field_labels = {'date': 'Дата', 'poster': 'Афиша', 'tickets': 'Билеты', 'text': 'Текст'}
        if field == 'date':
            c['date'] = None; c['time'] = None
        elif field == 'poster':
            c['poster_status'] = 'none'; c['poster_file_id'] = None
        elif field == 'tickets':
            c['tickets_url'] = None
        elif field == 'text':
            c['description_text'] = None
        db_save(c); sheets.sync_concert(c, _concerts)
        await q.edit_message_text(
            f"🗑 *{field_labels.get(field, field)}* сброшена — {c['artist']}\n\n" + card(c),
            reply_markup=edit_kb(cid), parse_mode='Markdown'
        )
        return

    if data.startswith('edit_menu_'):
        cid = int(data.replace('edit_menu_', ''))
        c   = db_get(cid)
        if c:
            await q.edit_message_text(card(c), reply_markup=edit_kb(cid), parse_mode='Markdown')
        else:
            await q.edit_message_text("Мероприятие не найдено")
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
        sheets.sync_concert(c, _concerts)
        await q.edit_message_text(card(c), reply_markup=edit_kb(cid), parse_mode='Markdown')

    if data.startswith('new_confirm|'):
        parts  = data.split('|')
        artist = parts[1]
        d      = parts[2] or None
        t      = parts[3] or None
        cid    = db_save({'artist': artist, 'date': d, 'time': t})
        c      = db_get(cid)
        sheets.sync_concert(c, _concerts)
        await q.edit_message_text(card(c), reply_markup=edit_kb(cid), parse_mode='Markdown')

    if data.startswith('upd_date|'):
        # Обновить дату существующего артиста из свободного ввода
        parts = data.split('|')
        cid   = int(parts[1])
        d     = parts[2] or None
        t     = parts[3] or None
        c     = db_get(cid)
        if c and d:
            c['date'] = d
            if t: c['time'] = t
            db_save(c); sheets.sync_concert(c, _concerts)
            await notify_ready(ctx, c)
            await q.edit_message_text(
                f"✅ Дата *{c['artist']}* обновлена: `{d} {t or ''}`.strip()",
                parse_mode='Markdown'
            )

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
        db_save(c); sheets.sync_concert(c, _concerts)
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
                db_save(c); sheets.sync_concert(c, _concerts)
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

    db_save(c); sheets.sync_concert(c, _concerts)
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
    sheets.sync_concert(c, _concerts)

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
            date_part = c.get('date', '')
            time_part = c.get('time', '')
            if date_part and time_part:
                d = f" — {date_part} {time_part}"
            elif date_part:
                d = f" — {date_part}"
            else:
                d = ''
            lines.append(f"#{c['id']} *{c['artist']}*{d}")
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
        date_part = c.get('date', '')
        time_part = c.get('time', '')
        if date_part and time_part:
            d = f" — {date_part} {time_part}"
        elif date_part:
            d = f" — {date_part}"
        else:
            d = ''
        m    = missing(c)
        miss = f" | нет: {', '.join(m)}" if m else " | ✅"
        lines.append(f"{s_icon(c)} #{c['id']} *{c['artist']}*{d}{miss}")

    if published:
        lines.append(f"\n⚫ Опубликовано: {len(published)}")
        for c in published[:5]: lines.append(f"  #{c['id']} *{c['artist']}*")

    if inc_all and cancelled:
        lines.append(f"\n🚫 Отменены: {len(cancelled)}")
        for c in cancelled: lines.append(f"  #{c['id']} *{c['artist']}*")

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
        for c in ready: lines.append(f"— *{c['artist']}*" + (f" • {c['date']}" if c.get('date') else ''))
        lines.append("")
    if prog:
        lines.append(f"🟡 IN PROGRESS ({len(prog)})")
        for c in prog: lines.append(f"— *{c['artist']}* (нет: {', '.join(missing(c))})")
        lines.append("")
    if draft:
        lines.append(f"🔴 DRAFT ({len(draft)})")
        for c in draft: lines.append(f"— *{c['artist']}*")
        lines.append("")
    if pub:
        lines.append(f"⚫ PUBLISHED ({len(pub)})")
        for c in pub[:5]: lines.append(f"— *{c['artist']}*")

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

    # Определяем тип ссылки на билеты
    is_ticketscloud = url.startswith('#ticketscloud')
    if is_ticketscloud:
        buy_btn_html = f'<a class="buy-btn" href="{url}">Купить билет</a>'
        tc_script = '<script src="https://ticketscloud.com/static/scripts/widget/tcwidget.js"></script>'
    else:
        buy_btn_html = f'<button class="buy-btn" onclick="window.open(\'{url}\', \'_blank\')">Купить билет</button>'
        tc_script = ''
    first_para = paragraphs[0] if paragraphs else desc
    rest_paras = '<br><br>'.join(paragraphs[1:]) if len(paragraphs) > 1 else ''

    # Полный шаблон — HTML + CSS + JS
    full_code = f"""<div class="event-wrapper">
    <button class="back-btn" onclick="goBackSafe(); return false;">
        <span class="arrow-left">←</span>
        <span>Назад</span>
    </button>

    <div class="event-image">
        <img src="{poster_url}" alt="{artist}">
    </div>

    <div class="event-content">
        <h1 class="event-title">{artist.upper()}</h1>
        <div class="event-datetime">{dt}</div>

        <div class="buttons-row">
            {buy_btn_html}
        </div>

        <div class="text-container" id="textContainer">
            <div class="text-scroll-zone" id="textZone">
                <p class="text-preview">
                    {first_para}
                </p>
                <p class="full-text">
                    {rest_paras}
                </p>
            </div>

            <div class="toggle-btn-wrapper" id="toggleWrapper" style="display: none;">
                <button class="toggle-btn" onclick="toggleText()">
                    <span class="btn-text">Читать далее</span>
                    <span class="arrow">▼</span>
                </button>
            </div>
        </div>
    </div>
</div>

<style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ background: #070707; color: #fff; font-family: 'Winston', sans-serif; font-weight: 400; overflow-x: hidden; }}

    .event-wrapper {{
        max-width: 1200px; margin: 0 auto; display: flex; gap: 40px;
        padding: 80px 20px 40px; position: relative; align-items: flex-start;
    }}

    .back-btn {{
        position: absolute; top: 25px; left: 20px;
        background: transparent; border: none; color: #fff;
        font-size: 14px; font-weight: 500; cursor: pointer;
        display: inline-flex; align-items: center; gap: 8px; z-index: 10;
    }}

    .event-image {{
        flex: 0 0 450px; width: 450px; aspect-ratio: 1 / 1 !important;
        overflow: hidden; background: #111;
    }}
    .event-image img {{ width: 100%; height: 100%; object-fit: cover; display: block; }}

    .event-content {{ flex: 1; min-width: 0; display: flex; flex-direction: column; align-self: stretch; }}

    .event-title {{ font-size: 38px; letter-spacing: 2px; line-height: 1.1; text-transform: uppercase; font-weight: 400 !important; margin-bottom: 10px; }}
    .event-datetime {{ font-size: 18px; color: #f5ce3e; margin-bottom: 25px; }}
    .buttons-row {{ display: flex; gap: 15px; margin-bottom: 25px; }}

    .buy-btn {{
        padding: 14px 30px; font-size: 15px; font-family: 'Winston', sans-serif; font-weight: 600;
        border-radius: 30px; cursor: pointer; transition: 0.3s; border: none;
        background: #f5ce3e; color: #000 !important;
    }}

    .text-container {{ flex: 1; display: flex; flex-direction: column; min-height: 0; }}
    .text-preview, .full-text {{ font-size: 16px; line-height: 1.6; opacity: 0.9; }}
    .full-text {{ margin-top: 10px; }}

    .text-scroll-zone {{ position: relative; overflow: hidden; transition: max-height 0.5s ease; padding-bottom: 10px; }}

    @media (min-width: 961px) {{
        .text-scroll-zone {{ max-height: 260px; overflow: hidden; }}
        .text-scroll-zone.expanded {{ max-height: 2000px !important; overflow: visible; }}
        .toggle-btn-wrapper {{ margin-top: auto; padding-top: 15px; display: none; }}
    }}

    @media (max-width: 960px) {{
        .event-wrapper {{ flex-direction: column; align-items: center; padding: 75px 20px 40px; gap: 0; }}
        .event-image {{ width: 100%; max-width: 450px; flex: none; margin-bottom: 20px; }}
        .event-content {{ width: 100%; align-items: center; gap: 12px; }}
        .event-title {{ font-size: 32px; text-align: center; margin-bottom: 0; }}
        .event-datetime {{ text-align: center; margin-bottom: 8px; }}
        .buttons-row {{ justify-content: center !important; margin-bottom: 10px; }}
        .text-container {{ width: 100%; }}
        .text-scroll-zone {{ max-height: 115px; }}
        .text-scroll-zone.active {{ max-height: 2000px !important; }}

        .text-scroll-zone:not(.expanded):not(.active)::after {{
            content: ''; position: absolute; bottom: 0; left: 0; width: 100%; height: 50px;
            background: linear-gradient(transparent, #070707); pointer-events: none; z-index: 2;
        }}

        .toggle-btn-wrapper {{ width: 100%; display: flex; justify-content: center; margin-top: 15px; }}
        .text-preview, .full-text {{ text-align: justify !important; }}
    }}

    @media (min-width: 601px) and (max-width: 960px) {{
        .text-container {{ max-width: 700px; padding: 0 45px; margin: 0 auto; }}
    }}

    .toggle-btn {{
        background: transparent; border: 1px solid rgba(255,255,255,0.5); color: #fff;
        padding: 10px 24px; border-radius: 30px; cursor: pointer;
        display: flex; align-items: center; gap: 8px; font-size: 14px;
    }}
    .toggle-btn .arrow {{ font-size: 10px; transition: 0.3s; }}
</style>

<script>
    function goBackSafe() {{
        if (document.referrer.includes(window.location.hostname)) {{ window.history.back(); }}
        else {{ window.location.href = 'https://mtbarmoscow.com/'; }}
    }}

    function toggleText() {{
        const zone = document.getElementById('textZone');
        const btnText = document.querySelector('.toggle-btn .btn-text');
        const arrow = document.querySelector('.toggle-btn .arrow');
        const isDesktop = window.innerWidth > 960;

        if (isDesktop) {{ zone.classList.toggle('expanded'); }}
        else {{ zone.classList.toggle('active'); }}

        const isOpen = zone.classList.contains('expanded') || zone.classList.contains('active');
        btnText.textContent = isOpen ? 'Свернуть' : 'Читать далее';
        arrow.style.transform = isOpen ? 'rotate(180deg)' : 'rotate(0deg)';
    }}

    window.addEventListener('load', () => {{
        const zone = document.getElementById('textZone');
        const wrapper = document.getElementById('toggleWrapper');
        const isDesktop = window.innerWidth > 960;

        if (isDesktop) {{
            const imgHeight = document.querySelector('.event-image').offsetHeight;
            const contentTop = zone.getBoundingClientRect().top;
            const wrapperTop = document.querySelector('.event-wrapper').getBoundingClientRect().top;
            const offset = contentTop - wrapperTop;
            const availableHeight = imgHeight - offset - 15;

            // Снэпаем к целым строкам чтобы не резать по середине
            const lineHeight = parseFloat(getComputedStyle(zone).lineHeight) || 25.6;
            const snappedHeight = Math.floor(availableHeight / lineHeight) * lineHeight;

            zone.style.maxHeight = snappedHeight + 'px';

            if (zone.scrollHeight > snappedHeight + 20) {{
                wrapper.style.display = 'flex';
            }} else {{
                zone.style.maxHeight = 'none';
            }}
        }} else {{
            if (zone.scrollHeight > 125) {{
                wrapper.style.display = 'flex';
            }} else {{
                zone.style.maxHeight = 'none';
            }}
        }}
    }});
</script>
{tc_script}"""

    m = missing(c)
    warnings = []
    if 'афиша'  in m: warnings.append('⚠️ Афиша не добавлена — замени ССЫЛКА_НА_АФИШУ')
    if 'билеты' in m: warnings.append('⚠️ Билеты не добавлены')
    if 'текст'  in m: warnings.append('⚠️ Текст не добавлен')
    if 'дата'   in m: warnings.append('⚠️ Дата не установлена')

    header = f'🎤 *{artist}* — код для Tilda Zero Block'
    if warnings:
        header += '\n\n' + '\n'.join(warnings)
    header += f'\n\nВставь содержимое файла в Zero Block → HTML\nПосле публикации → `/publish {cid}`'

    await upd.message.reply_text(header, parse_mode='Markdown')

    # Отправляем как файл — без обрезки
    import io
    fname   = f"{artist.lower().replace(' ', '_')}_{cid}.html"
    bio     = io.BytesIO(full_code.encode('utf-8'))
    bio.name = fname
    await upd.message.reply_document(document=bio, filename=fname)

    # Второе сообщение — SEO данные для Tilda
    slug      = make_slug(artist)  # без дат — только имя артиста
    seo_title = f"{artist} — праздничный концерт в Мумий Тролль Бар, Москва"
    seo_desc  = (
        f"Билеты на концерт {artist} в Мумий Тролль Бар, "
        f"музыкальный бар, ресторан с живой музыкой, "
        f"концертная площадка, Мумий Тролль Бар"
    )
    # Заголовок для блока: дата • артист
    header_line = f"{date_str} • {artist}" if date_str else artist

    seo_msg = (
        f"📋 *SEO для страницы*\n\n"
        f"*Заголовок блока:*\n`{header_line}`\n\n"
        f"*Адрес страницы (slug):*\n`{slug}`\n\n"
        f"*SEO заголовок:*\n`{seo_title}`\n\n"
        f"*SEO описание:*\n`{seo_desc}`"
    )
    await upd.message.reply_text(seo_msg, parse_mode='Markdown')


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

    # Свободный ввод: Имя + дата → предложить создать или обновить дату
    free = parse_free_text(text)
    if free:
        artist  = free['artist']
        d, t    = free['date'], free['time']
        matches = fuzzy_find(artist)
        if matches:
            # Артист найден → предложить обновить дату существующего
            c  = matches[0]
            dt = f"{d} {t or ''}".strip()
            kb = [[
                InlineKeyboardButton(f"📅 Обновить дату #{c['id']}", callback_data=f"upd_date|{c['id']}|{d}|{t or ''}"),
                InlineKeyboardButton("➕ Создать новое",              callback_data=f"new_confirm|{artist}|{d or ''}|{t or ''}"),
                InlineKeyboardButton("❌ Отмена",                     callback_data="noop"),
            ]]
            await upd.message.reply_text(
                f"Найден *#{c['id']} {c['artist']}*.\n"
                f"Обновить дату на `{dt}` или создать новое мероприятие?",
                reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown'
            )
        else:
            # Артист не найден → предложить создать
            dt = f"{d} {t or ''}".strip()
            kb = [[
                InlineKeyboardButton("✅ Создать", callback_data=f"new_confirm|{artist}|{d or ''}|{t or ''}"),
                InlineKeyboardButton("❌ Отмена",  callback_data="noop"),
            ]]
            await upd.message.reply_text(
                f"Создать мероприятие *{artist}* на `{dt}`?",
                reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown'
            )
        return

    # "Ты имел в виду?" — мягкий fuzzy (60-65) без триггера
    if len(text.split()) <= 5:
        soft = fuzzy_find(text, soft=True)
        strict = fuzzy_find(text)
        if soft and not strict:
            c  = soft[0]
            cn = norm(c['artist'])
            tn = norm(text)
            score = max(fuzz.token_set_ratio(tn, cn), fuzz.partial_ratio(tn, cn))
            if 60 <= score < 65:
                kb = [[
                    InlineKeyboardButton(f"✅ Да, #{c['id']} {c['artist']}", callback_data=f"edit_menu_{c['id']}"),
                    InlineKeyboardButton("❌ Нет", callback_data="noop"),
                ]]
                await upd.message.reply_text(
                    f"Ты имел в виду *{c['artist']}*?",
                    reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown'
                )


async def cmd_rebuild(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Пересобирает все календари и чистит имена артистов в Sheets."""
    global _concerts
    msg = await upd.message.reply_text("🔄 Пересобираю календари...")

    # Чистим имена артистов в памяти (убираем " —" и лишние пробелы)
    for c in _concerts:
        c['artist'] = c['artist'].rstrip(' —').strip()

    # Пересобираем все месяцы
    months = set()
    for c in _concerts:
        if c.get('date'):
            try:
                from datetime import datetime as _dt
                dt = _dt.strptime(c['date'], '%d.%m.%Y')
                months.add((dt.month, dt.year))
            except Exception:
                pass

    count = 0
    for month, year in sorted(months):
        try:
            sheets.rebuild_month_calendar(month, year, _concerts)
            count += 1
        except Exception as e:
            logger.error(f"rebuild {month}/{year}: {e}")

    # Синхронизируем все концерты в лист Данные (с чистыми именами)
    for c in _concerts:
        try:
            sheets._sync_data_row(c)
        except Exception as e:
            logger.error(f"sync row {c['id']}: {e}")

    await msg.edit_text(
        f"✅ Готово!\n"
        f"Пересобрано календарей: {count}\n"
        f"Концертов обновлено: {len(_concerts)}"
    )

async def cmd_notify_on(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global _notify_enabled
    _notify_enabled = True
    await upd.message.reply_text("🔔 Утренние уведомления включены")

async def cmd_notify_off(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global _notify_enabled
    _notify_enabled = False
    await upd.message.reply_text("🔕 Утренние уведомления выключены")


# ─── УТРЕННИЙ ДАЙДЖЕСТ ────────────────────────────────────────────────────────

async def morning_digest(ctx: ContextTypes.DEFAULT_TYPE):
    global _concerts

    # Автоархив прошедших опубликованных концертов
    now      = datetime.now()
    archived = []
    for c in list(_concerts):
        if c.get('status') == 'published' and c.get('date'):
            try:
                event_dt = datetime.strptime(c['date'], '%d.%m.%Y')
                if event_dt.date() < now.date():
                    c['status'] = 'cancelled'
                    db_save(c); sheets.sync_concert(c, _concerts)
                    archived.append(c['artist'])
            except Exception:
                pass

    if archived:
        msg = "📦 *Концерты перенесены в архив* (дата прошла):\n" + \
              '\n'.join(f"— *{a}*" for a in archived) + \
              "\n\n⚠️ Сними с сайта!"
        for chat_id in get_chats():
            try:
                await ctx.bot.send_message(chat_id, msg, parse_mode='Markdown')
            except Exception as e:
                logger.error(f"archive notify {chat_id}: {e}")

    if not _notify_enabled:
        return

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

    lines = [f"📊 *Статус на {now.strftime('%d.%m.%Y')}*\n"]
    if ready:
        lines.append(f"🟢 READY ({len(ready)})")
        for c in ready: lines.append(f"— *{c['artist']}*" + (f" • {c['date']}" if c.get('date') else ''))
        lines.append("")
    if prog:
        lines.append(f"🟡 IN PROGRESS ({len(prog)})")
        for c in prog: lines.append(f"— *{c['artist']}* (нет: {', '.join(missing(c))})")
        lines.append("")
    if draft:
        lines.append(f"🔴 DRAFT ({len(draft)})")
        for c in draft: lines.append(f"— *{c['artist']}*")

    text = '\n'.join(lines)
    for chat_id in get_chats():
        try:
            await ctx.bot.send_message(chat_id, text, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"digest {chat_id}: {e}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    global _concerts, _chats
    # Загружаем данные из Google Sheets — это и есть наша БД
    _concerts = sheets.load_all_concerts()
    _chats    = sheets.load_chats()
    logger.info(f"🎸 Загружено концертов: {len(_concerts)}, чатов: {len(_chats)}")
    app = Application.builder().token(TOKEN).build()

    for cmd, fn in [
        ('start',   cmd_start),
        ('new',     cmd_new),
        ('edit',    cmd_edit),
        ('list',    cmd_list),
        ('status',  cmd_status),
        ('publish', cmd_publish),
        ('cancel',  cmd_cancel),
        ('digest',     cmd_digest),
        ('code',       cmd_code),
        ('help',       cmd_start),
        ('notify_on',  cmd_notify_on),
        ('notify_off', cmd_notify_off),
        ('rebuild',    cmd_rebuild),
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
