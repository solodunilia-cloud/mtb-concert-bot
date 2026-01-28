#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MTB Concerts Bot - Main Bot File
Управление концертами для mtbarmoscow.com
"""

import os
import logging
import json
import re
from datetime import datetime
from typing import Optional, Dict, Any
import sqlite3
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters
)

from tilda_api import TildaAPI
from google_sheets import GoogleSheetsManager
from template_generator import generate_page_html

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Конфигурация
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '8260143545:AAHUZqBpc7BdVYaMC3zQ9ZcB9HViBsYnEgQ')
TILDA_PUBLIC = os.getenv('TILDA_PUBLIC', 'q3cf8fa6jyqm41o9qc')
TILDA_SECRET = os.getenv('TILDA_SECRET', 'e6ba61619adad57acccd')
TILDA_PROJECT = os.getenv('TILDA_PROJECT', '11288143')

# Инициализация
tilda = TildaAPI(TILDA_PUBLIC, TILDA_SECRET, TILDA_PROJECT)
db_path = 'concerts.db'


# ==================== DATABASE ====================

def init_db():
    """Инициализация базы данных"""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS concerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            date TEXT,
            time TEXT,
            image_url TEXT,
            tickets_url TEXT,
            description TEXT,
            yandex_music_url TEXT,
            status TEXT DEFAULT 'draft',
            tilda_page_id TEXT,
            tilda_url TEXT,
            progress INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()


def save_concert(concert_data: Dict[str, Any]) -> int:
    """Сохранить концерт в БД"""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    if 'id' in concert_data and concert_data['id']:
        # Обновление
        c.execute('''
            UPDATE concerts 
            SET title=?, date=?, time=?, image_url=?, tickets_url=?, 
                description=?, yandex_music_url=?, status=?, progress=?, updated_at=?
            WHERE id=?
        ''', (
            concert_data.get('title'),
            concert_data.get('date'),
            concert_data.get('time'),
            concert_data.get('image_url'),
            concert_data.get('tickets_url'),
            concert_data.get('description'),
            concert_data.get('yandex_music_url'),
            concert_data.get('status', 'draft'),
            concert_data.get('progress', 0),
            datetime.now().isoformat(),
            concert_data['id']
        ))
        concert_id = concert_data['id']
    else:
        # Создание
        c.execute('''
            INSERT INTO concerts 
            (title, date, time, image_url, tickets_url, description, yandex_music_url, status, progress)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            concert_data.get('title'),
            concert_data.get('date'),
            concert_data.get('time'),
            concert_data.get('image_url'),
            concert_data.get('tickets_url'),
            concert_data.get('description'),
            concert_data.get('yandex_music_url'),
            concert_data.get('status', 'draft'),
            concert_data.get('progress', 0)
        ))
        concert_id = c.lastrowid
    
    conn.commit()
    conn.close()
    return concert_id


def get_concert(concert_id: int) -> Optional[Dict[str, Any]]:
    """Получить концерт по ID"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('SELECT * FROM concerts WHERE id=?', (concert_id,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_concerts() -> list:
    """Получить все концерты"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('SELECT * FROM concerts ORDER BY date DESC, created_at DESC')
    rows = c.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def calculate_progress(concert: Dict[str, Any]) -> int:
    """Рассчитать прогресс заполнения концерта"""
    fields = ['title', 'date', 'time', 'image_url', 'tickets_url', 'description']
    filled = sum(1 for field in fields if concert.get(field))
    # Яндекс Музыка не обязательна, поэтому не учитываем в прогрессе
    return int((filled / len(fields)) * 100)


# ==================== PARSERS ====================

def parse_date_from_text(text: str) -> Optional[tuple]:
    """Парсинг даты и времени из текста"""
    # Паттерны для даты
    date_patterns = [
        r'(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})',  # 05.03.2026
        r'(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})?',  # 5 марта 2026
    ]
    
    # Паттерн для времени
    time_pattern = r'(\d{1,2})[:\.](\d{2})'
    
    months_ru = {
        'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
        'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
        'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
    }
    
    date_str = None
    time_str = None
    
    # Парсинг даты
    for pattern in date_patterns:
        match = re.search(pattern, text.lower())
        if match:
            if len(match.groups()) == 3 and match.group(2) in months_ru:
                # Формат: 5 марта 2026
                day = match.group(1)
                month = months_ru[match.group(2)]
                year = match.group(3) if match.group(3) else '2026'
                date_str = f"{int(day):02d}.{month:02d}.{year}"
            else:
                # Формат: 05.03.2026
                day, month, year = match.groups()
                date_str = f"{int(day):02d}.{int(month):02d}.{year}"
            break
    
    # Парсинг времени
    time_match = re.search(time_pattern, text)
    if time_match:
        hour, minute = time_match.groups()
        time_str = f"{int(hour):02d}:{int(minute):02d}"
    
    return (date_str, time_str) if date_str or time_str else None


def detect_url_type(url: str) -> str:
    """Определить тип URL"""
    url_lower = url.lower()
    if 'afisha.yandex' in url_lower or 'widget.afisha' in url_lower:
        return 'tickets'
    elif 'music.yandex' in url_lower:
        return 'yandex_music'
    return 'unknown'


# ==================== HANDLERS ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    welcome_text = """
🎸 **MTB Concerts Manager**

Привет! Я помогу тебе управлять концертами на сайте mtbarmoscow.com

**Основные команды:**
/new [название] - создать новый концерт
/list - список всех концертов
/status [номер] - статус концерта
/help - помощь

**Как работать:**
Просто пересылай мне данные из рабочего чата:
• Картинки (афиши)
• Текст с датой и описанием
• Ссылки на билеты
• Ссылки на Яндекс.Музыку

Я сам разберусь что куда! 🚀
    """
    await update.message.reply_text(welcome_text, parse_mode='Markdown')


async def new_concert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Создать новый концерт"""
    # Получаем название из команды
    title = ' '.join(context.args) if context.args else None
    
    if not title:
        await update.message.reply_text(
            "Укажи название концерта:\n/new Metallica Tribute"
        )
        return
    
    # Создаём концерт
    concert_data = {
        'title': title,
        'status': 'draft',
        'progress': calculate_progress({'title': title})
    }
    concert_id = save_concert(concert_data)
    
    # Сохраняем ID в контекст
    context.user_data['current_concert_id'] = concert_id
    
    concert = get_concert(concert_id)
    progress = concert['progress']
    
    status_text = f"""
✅ Концерт создан!

📝 **#{concert_id} {title}**
📊 Прогресс: {progress}%

Не хватает:
❌ Дата и время
❌ Афиша
❌ Билеты
❌ Описание

Пришли данные или используй команды:
/status {concert_id} - проверить статус
/edit {concert_id} - редактировать
    """
    
    await update.message.reply_text(status_text, parse_mode='Markdown')


async def list_concerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Список всех концертов"""
    concerts = get_all_concerts()
    
    if not concerts:
        await update.message.reply_text("Пока нет концертов. Создай первый: /new Название")
        return
    
    # Группируем по статусу
    published = [c for c in concerts if c['status'] == 'published']
    ready = [c for c in concerts if c['status'] == 'draft' and c['progress'] == 100]
    in_progress = [c for c in concerts if c['status'] == 'draft' and c['progress'] < 100]
    
    text = f"📋 **Всего концертов: {len(concerts)}**\n\n"
    
    if published:
        text += "🟢 **Опубликованные:**\n"
        for c in published[:5]:
            text += f"#{c['id']} {c['title']} - {c['date'] or '?'}\n"
        text += "\n"
    
    if ready:
        text += "🟡 **Готовы к публикации:**\n"
        for c in ready[:5]:
            text += f"#{c['id']} {c['title']} - {c['date'] or '?'}\n"
        text += "\n"
    
    if in_progress:
        text += "🔴 **В работе:**\n"
        for c in in_progress[:5]:
            text += f"#{c['id']} {c['title']} ({c['progress']}%)\n"
        text += "\n"
    
    text += "\nИспользуй /status [номер] для деталей"
    
    await update.message.reply_text(text, parse_mode='Markdown')


async def concert_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статус конкретного концерта"""
    if not context.args:
        await update.message.reply_text("Укажи номер: /status 23")
        return
    
    try:
        concert_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Неверный номер концерта")
        return
    
    concert = get_concert(concert_id)
    if not concert:
        await update.message.reply_text(f"Концерт #{concert_id} не найден")
        return
    
    # Формируем статус
    status_emoji = {
        'published': '🟢',
        'draft': '🟡' if concert['progress'] == 100 else '🔴'
    }
    
    text = f"""
{status_emoji.get(concert['status'], '⚪')} **Концерт #{concert['id']}**

🎸 **{concert['title']}**
📅 {concert['date'] or '❌ Нет даты'} • {concert['time'] or '❌ Нет времени'}
📊 Прогресс: {concert['progress']}%

**Данные:**
{'✅' if concert['image_url'] else '❌'} Афиша
{'✅' if concert['tickets_url'] else '❌'} Билеты
{'✅' if concert['description'] else '❌'} Описание
{'✅' if concert['yandex_music_url'] else '❌'} Яндекс.Музыка

**Статус:** {concert['status']}
    """
    
    if concert['tilda_url']:
        text += f"\n🔗 Страница: {concert['tilda_url']}"
    
    # Кнопки действий
    keyboard = []
    if concert['progress'] == 100 and concert['status'] == 'draft':
        keyboard.append([InlineKeyboardButton("⚡ Опубликовать", callback_data=f"publish_{concert_id}")])
    if concert['status'] == 'published':
        keyboard.append([InlineKeyboardButton("📝 Редактировать", callback_data=f"edit_{concert_id}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    
    await update.message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка обычных сообщений"""
    message = update.message
    
    # Проверяем есть ли активный концерт
    current_id = context.user_data.get('current_concert_id')
    if not current_id:
        await message.reply_text(
            "Сначала создай концерт: /new Название концерта\n"
            "Или используй /list чтобы увидеть существующие"
        )
        return
    
    concert = get_concert(current_id)
    if not concert:
        await message.reply_text("Концерт не найден. Создай новый: /new Название")
        return
    
    # Обрабатываем сообщение
    updated = False
    
    # Текстовое сообщение
    if message.text:
        text = message.text
        
        # Парсим дату/время
        date_time = parse_date_from_text(text)
        if date_time:
            date_str, time_str = date_time
            if date_str and not concert['date']:
                concert['date'] = date_str
                updated = True
            if time_str and not concert['time']:
                concert['time'] = time_str
                updated = True
        
        # Проверяем ссылки
        urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
        for url in urls:
            url_type = detect_url_type(url)
            if url_type == 'tickets' and not concert['tickets_url']:
                concert['tickets_url'] = url
                updated = True
            elif url_type == 'yandex_music' and not concert['yandex_music_url']:
                concert['yandex_music_url'] = url
                updated = True
        
        # Если нет описания и текст длинный - считаем описанием
        if not concert['description'] and len(text) > 50 and not urls:
            concert['description'] = text
            updated = True
    
    if updated:
        concert['progress'] = calculate_progress(concert)
        save_concert(concert)
        
        progress = concert['progress']
        status_text = f"✅ Данные обновлены!\n\n📊 Прогресс: {progress}%"
        
        if progress == 100:
            status_text += "\n\n🎉 Концерт готов на 100%!"
            keyboard = [[InlineKeyboardButton("⚡ Опубликовать", callback_data=f"publish_{current_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await message.reply_text(status_text, reply_markup=reply_markup)
        else:
            await message.reply_text(status_text)
    else:
        await message.reply_text("Не смог распознать данные. Попробуй по-другому или используй /help")


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка фото (афиши)"""
    current_id = context.user_data.get('current_concert_id')
    if not current_id:
        await update.message.reply_text("Сначала создай концерт: /new Название")
        return
    
    # Получаем фото
    photo = update.message.photo[-1]  # Берём самое большое
    file = await context.bot.get_file(photo.file_id)
    
    # Скачиваем
    file_path = f"temp_{photo.file_id}.jpg"
    await file.download_to_drive(file_path)
    
    try:
        # Загружаем в Tilda
        image_url = await tilda.upload_image(file_path)
        
        if image_url:
            # Обновляем концерт
            concert = get_concert(current_id)
            concert['image_url'] = image_url
            concert['progress'] = calculate_progress(concert)
            save_concert(concert)
            
            await update.message.reply_text(
                f"✅ Афиша загружена!\n📊 Прогресс: {concert['progress']}%"
            )
        else:
            await update.message.reply_text("❌ Ошибка загрузки афиши в Tilda")
    
    finally:
        # Удаляем временный файл
        if os.path.exists(file_path):
            os.remove(file_path)


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатий кнопок"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data.startswith('publish_'):
        concert_id = int(data.split('_')[1])
        concert = get_concert(concert_id)
        
        if not concert:
            await query.edit_message_text("Концерт не найден")
            return
        
        await query.edit_message_text("⏳ Создаю страницу в Tilda...")
        
        try:
            # Генерируем HTML
            html_content = generate_page_html(concert)
            
            # Создаём страницу в Tilda
            page_result = await tilda.create_page(
                title=concert['title'],
                html=html_content
            )
            
            if page_result:
                # Обновляем концерт
                concert['tilda_page_id'] = page_result.get('id')
                concert['tilda_url'] = page_result.get('url')
                concert['status'] = 'published'
                save_concert(concert)
                
                success_text = f"""
✅ Страница создана!

🔗 {concert['tilda_url']}

Статус: Черновик (проверь в Tilda)
                """
                
                keyboard = [[InlineKeyboardButton("🔗 Открыть в Tilda", url=concert['tilda_url'])]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await query.edit_message_text(success_text, reply_markup=reply_markup)
            else:
                await query.edit_message_text("❌ Ошибка создания страницы")
        
        except Exception as e:
            logger.error(f"Publish error: {e}")
            await query.edit_message_text(f"❌ Ошибка: {str(e)}")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда помощи"""
    help_text = """
🎸 **Как пользоваться ботом:**

**1. Создай концерт:**
/new Metallica Tribute

**2. Добавь данные:**
Просто пересылай из рабочего чата:
• Картинку афиши
• Текст с датой ("5 марта 20:00")
• Ссылку на билеты
• Описание концерта

**3. Проверь статус:**
/status 23

**4. Опубликуй:**
Когда прогресс 100% - нажми "Опубликовать"

**Команды:**
/new - новый концерт
/list - все концерты
/status - статус концерта
/help - эта помощь

**Поддержка:** @your_username
    """
    await update.message.reply_text(help_text, parse_mode='Markdown')


# ==================== MAIN ====================

def main():
    """Запуск бота"""
    # Инициализация БД
    init_db()
    
    # Создаём приложение
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("new", new_concert))
    application.add_handler(CommandHandler("list", list_concerts))
    application.add_handler(CommandHandler("status", concert_status))
    application.add_handler(CommandHandler("help", help_command))
    
    # Обработчики сообщений
    application.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Обработчик кнопок
    application.add_handler(CallbackQueryHandler(button_callback))
    
    # Запуск
    logger.info("🎸 MTB Concerts Bot запущен!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
