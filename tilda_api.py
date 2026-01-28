"""
Модуль для работы с Tilda API
"""

import aiohttp
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class TildaAPI:
    """Класс для работы с Tilda API"""
    
    def __init__(self, public_key: str, secret_key: str, project_id: str):
        self.public_key = public_key
        self.secret_key = secret_key
        self.project_id = project_id
        self.base_url = "https://api.tildacdn.info/v1"
    
    async def upload_image(self, file_path: str) -> Optional[str]:
        """
        Загрузить изображение в Tilda
        Возвращает URL загруженного изображения
        """
        try:
            async with aiohttp.ClientSession() as session:
                data = aiohttp.FormData()
                data.add_field('publickey', self.public_key)
                data.add_field('secretkey', self.secret_key)
                data.add_field('projectid', self.project_id)
                
                with open(file_path, 'rb') as f:
                    data.add_field('file', f, filename='image.jpg')
                    
                    async with session.post(
                        f"{self.base_url}/uploadfile",
                        data=data
                    ) as response:
                        result = await response.json()
                        
                        if result.get('status') == 'FOUND':
                            image_url = result.get('uploadurl')
                            logger.info(f"Image uploaded: {image_url}")
                            return image_url
                        else:
                            logger.error(f"Upload failed: {result}")
                            return None
        
        except Exception as e:
            logger.error(f"Upload image error: {e}")
            return None
    
    async def create_page(self, title: str, html: str) -> Optional[Dict[str, Any]]:
        """
        Создать страницу в Tilda
        Возвращает информацию о созданной странице
        """
        try:
            # Генерируем alias (URL) из названия
            alias = self._generate_alias(title)
            
            async with aiohttp.ClientSession() as session:
                # Сначала создаём страницу
                params = {
                    'publickey': self.public_key,
                    'secretkey': self.secret_key,
                    'projectid': self.project_id,
                    'title': title,
                    'descr': f'Концерт: {title}'
                }
                
                async with session.post(
                    f"{self.base_url}/createpage",
                    params=params
                ) as response:
                    result = await response.json()
                    
                    if result.get('status') != 'FOUND':
                        logger.error(f"Create page failed: {result}")
                        return None
                    
                    page_id = result['result']['id']
                    logger.info(f"Page created: {page_id}")
                
                # Теперь добавляем HTML
                params = {
                    'publickey': self.public_key,
                    'secretkey': self.secret_key,
                    'pageid': page_id,
                    'html': html
                }
                
                async with session.post(
                    f"{self.base_url}/updatepage",
                    params=params
                ) as response:
                    update_result = await response.json()
                    
                    if update_result.get('status') != 'FOUND':
                        logger.error(f"Update page failed: {update_result}")
                        return None
                    
                    logger.info(f"Page HTML updated: {page_id}")
                
                # Возвращаем информацию
                return {
                    'id': page_id,
                    'url': f"https://tilda.cc/page/?pageid={page_id}",
                    'alias': alias
                }
        
        except Exception as e:
            logger.error(f"Create page error: {e}")
            return None
    
    async def publish_page(self, page_id: str) -> bool:
        """
        Опубликовать страницу
        """
        try:
            async with aiohttp.ClientSession() as session:
                params = {
                    'publickey': self.public_key,
                    'secretkey': self.secret_key,
                    'pageid': page_id
                }
                
                async with session.post(
                    f"{self.base_url}/publishpage",
                    params=params
                ) as response:
                    result = await response.json()
                    
                    if result.get('status') == 'FOUND':
                        logger.info(f"Page published: {page_id}")
                        return True
                    else:
                        logger.error(f"Publish failed: {result}")
                        return False
        
        except Exception as e:
            logger.error(f"Publish page error: {e}")
            return False
    
    def _generate_alias(self, title: str) -> str:
        """
        Генерировать URL-friendly alias из названия
        """
        # Транслитерация
        translit_dict = {
            'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
            'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
            'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
            'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
            'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
        }
        
        alias = title.lower()
        
        # Транслитерация кириллицы
        for rus, eng in translit_dict.items():
            alias = alias.replace(rus, eng)
        
        # Удаляем всё кроме букв и цифр
        alias = ''.join(c for c in alias if c.isalnum())
        
        # Обрезаем до 50 символов
        alias = alias[:50]
        
        return alias
