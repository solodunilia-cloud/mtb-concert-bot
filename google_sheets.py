"""
Модуль для синхронизации с Google Sheets
"""

import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class GoogleSheetsManager:
    """
    Класс для работы с Google Sheets
    
    ВАЖНО: Для работы нужно:
    1. Создать проект в Google Cloud Console
    2. Включить Google Sheets API
    3. Создать Service Account и скачать credentials.json
    4. Дать доступ к таблице email из Service Account
    """
    
    def __init__(self, spreadsheet_id: str = None, credentials_path: str = 'credentials.json'):
        self.spreadsheet_id = spreadsheet_id
        self.credentials_path = credentials_path
        self.client = None
        self.worksheet = None
        
        # Попытка инициализации (опционально)
        try:
            self._init_client()
        except Exception as e:
            logger.warning(f"Google Sheets не инициализированы: {e}")
    
    def _init_client(self):
        """Инициализация клиента Google Sheets"""
        try:
            import gspread
            from google.oauth2.service_account import Credentials
            
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            creds = Credentials.from_service_account_file(
                self.credentials_path,
                scopes=scopes
            )
            
            self.client = gspread.authorize(creds)
            
            if self.spreadsheet_id:
                spreadsheet = self.client.open_by_key(self.spreadsheet_id)
                self.worksheet = spreadsheet.sheet1
                logger.info("Google Sheets подключены успешно")
        
        except ImportError:
            logger.warning("Библиотека gspread не установлена. Установите: pip install gspread google-auth")
        except Exception as e:
            logger.error(f"Ошибка инициализации Google Sheets: {e}")
    
    def sync_concert(self, concert: Dict[str, Any]):
        """Синхронизировать концерт с таблицей"""
        if not self.worksheet:
            logger.warning("Google Sheets не настроены, пропускаем синхронизацию")
            return
        
        try:
            # Формируем строку данных
            row_data = self._format_concert_row(concert)
            
            # Ищем существующую строку
            existing_row = self._find_concert_row(concert['id'])
            
            if existing_row:
                # Обновляем существующую
                self.worksheet.update(f'A{existing_row}:I{existing_row}', [row_data])
                logger.info(f"Концерт #{concert['id']} обновлён в таблице")
            else:
                # Добавляем новую
                self.worksheet.append_row(row_data)
                logger.info(f"Концерт #{concert['id']} добавлен в таблицу")
        
        except Exception as e:
            logger.error(f"Ошибка синхронизации с Google Sheets: {e}")
    
    def _format_concert_row(self, concert: Dict[str, Any]) -> List[str]:
        """Форматировать данные концерта для строки таблицы"""
        return [
            str(concert['id']),
            concert.get('title', ''),
            concert.get('date', ''),
            concert.get('time', ''),
            '✅' if concert.get('image_url') else '❌',
            '✅' if concert.get('tickets_url') else '❌',
            '✅' if concert.get('description') else '❌',
            '✅' if concert.get('yandex_music_url') else '❌',
            f"{concert.get('progress', 0)}%",
            self._get_status_emoji(concert)
        ]
    
    def _get_status_emoji(self, concert: Dict[str, Any]) -> str:
        """Получить эмодзи статуса"""
        if concert.get('status') == 'published':
            return '🟢 ОПУБЛ'
        elif concert.get('progress', 0) == 100:
            return '🟡 ГОТОВ'
        elif concert.get('progress', 0) >= 80:
            return '🟠 ПОЧТИ'
        else:
            return f"🔴 {concert.get('progress', 0)}%"
    
    def _find_concert_row(self, concert_id: int) -> int:
        """Найти номер строки концерта по ID"""
        if not self.worksheet:
            return None
        
        try:
            cell = self.worksheet.find(str(concert_id))
            return cell.row if cell else None
        except:
            return None
    
    def create_dashboard_table(self) -> str:
        """
        Создать новую таблицу-dashboard
        Возвращает ID созданной таблицы
        """
        if not self.client:
            logger.error("Google Sheets client не инициализирован")
            return None
        
        try:
            # Создаём новую таблицу
            spreadsheet = self.client.create('MTB Concerts Dashboard')
            spreadsheet_id = spreadsheet.id
            
            # Делаем таблицу доступной по ссылке
            spreadsheet.share(None, perm_type='anyone', role='reader')
            
            # Настраиваем первый лист
            worksheet = spreadsheet.sheet1
            worksheet.update_title('Концерты')
            
            # Заголовки
            headers = [
                '№', 'Название', 'Дата', 'Время',
                'Афиша', 'Билеты', 'Текст', 'Яндекс',
                'Прогресс', 'Статус'
            ]
            worksheet.append_row(headers)
            
            # Форматирование заголовков
            worksheet.format('A1:J1', {
                'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
                'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                'horizontalAlignment': 'CENTER'
            })
            
            # Ширина колонок
            worksheet.columns_auto_resize(0, 9)
            
            logger.info(f"Dashboard создан: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
            
            self.spreadsheet_id = spreadsheet_id
            self.worksheet = worksheet
            
            return spreadsheet_id
        
        except Exception as e:
            logger.error(f"Ошибка создания dashboard: {e}")
            return None


# Функция-помощник для инициализации
def setup_google_sheets():
    """
    Инструкция по настройке Google Sheets
    """
    instructions = """
    📊 НАСТРОЙКА GOOGLE SHEETS:
    
    1. Перейди на https://console.cloud.google.com
    2. Создай новый проект (или выбери существующий)
    3. Включи Google Sheets API:
       - APIs & Services → Enable APIs
       - Найди "Google Sheets API" → Enable
    4. Создай Service Account:
       - APIs & Services → Credentials
       - Create Credentials → Service Account
       - Скачай JSON ключ
    5. Переименуй файл в credentials.json
    6. Положи в папку с ботом
    
    Готово! Бот автоматически создаст таблицу при первом запуске.
    """
    return instructions
