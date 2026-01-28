"""
Генератор HTML страниц концертов
"""

from typing import Dict, Any


def generate_page_html(concert: Dict[str, Any]) -> str:
    """
    Генерирует HTML код страницы концерта на основе шаблона
    """
    
    # Форматирование даты
    date_str = format_date(concert.get('date'), concert.get('time'))
    
    # Проверка наличия Яндекс.Музыки
    yandex_music_url = concert.get('yandex_music_url', '')
    yandex_button_class = 'hidden' if not yandex_music_url else ''
    
    # Генерация HTML
    html = f"""
<div class="event-wrapper">
    <button class="back-btn" onclick="goBackSafe(); return false;">
        <span class="arrow-left">←</span>
        <span>Назад</span>
    </button>

    <div class="event-image">
        <img src="{concert.get('image_url', '')}" alt="{concert.get('title', '')}">
    </div>

    <div class="event-content">
        <h1 class="event-title">{concert.get('title', '')}</h1>
        <div class="event-datetime">{date_str}</div>
        
        <div class="buttons-row">
            <button class="buy-btn" onclick="window.open('{concert.get('tickets_url', '')}', '_blank')">
                Купить билет
            </button>
            <button class="yandex-btn {yandex_button_class}" data-yandex-link="{yandex_music_url}" onclick="window.open(this.getAttribute('data-yandex-link'), '_blank')">
                Яндекс Музыка
            </button>
        </div>

        <div class="text-content-wrapper">
            <div class="text-content" id="textContent">
                <div class="text-full">
                    {format_description(concert.get('description', ''))}
                </div>
            </div>
            
            <div class="toggle-btn-wrapper">
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
    
    body {{ 
        background: #070707; 
        color: #fff; 
        font-family: 'Winston', sans-serif; 
        font-weight: 400;
        overflow-x: hidden; 
    }}
    .event-wrapper {{
        max-width: 1200px;
        margin: 0 auto;
        display: flex;
        gap: 40px;
        padding: 80px 20px 40px;
        position: relative;
        align-items: flex-start;
    }}
    .back-btn {{
        position: absolute; top: 25px; left: 20px;
        background: transparent; border: none; color: #fff;
        font-size: 14px; font-weight: 500; cursor: pointer; display: inline-flex; align-items: center; gap: 8px; z-index: 10;
    }}
    .event-image {{
        flex: 0 0 450px; 
        width: 450px;
        height: 450px;
        overflow: hidden; 
        background: #111; 
        border-radius: 0 !important;
        flex-shrink: 0;
    }}
    .event-image img {{ 
        width: 100%; 
        height: 100%; 
        object-fit: cover; 
        display: block; 
    }}
    .event-content {{ 
        flex: 1; 
        min-width: 0;
        display: flex;
        flex-direction: column;
    }}
    
    .event-title {{ 
        font-size: 38px; letter-spacing: 2px; margin-bottom: 10px; line-height: 1.1; 
        text-transform: uppercase; font-weight: 400 !important;
    }}
    .event-datetime {{ font-size: 18px; color: #f5ce3e; margin-bottom: 25px; }}
    .buttons-row {{ display: flex; gap: 15px; margin-bottom: 25px; flex-wrap: wrap; }}
    
    .buy-btn, .yandex-btn, .toggle-btn {{
        padding: 14px 30px; 
        font-size: 15px; 
        font-family: 'Winston', sans-serif; 
        font-weight: 600 !important;
        border-radius: 30px; 
        cursor: pointer; 
        transition: 0.3s; 
        text-transform: none !important;
        width: fit-content;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        text-align: center;
    }}
    
    .buy-btn {{ background: #f5ce3e; color: #000; border: none; }}
    .yandex-btn, .toggle-btn {{ 
        background: transparent; 
        border: 1px solid rgba(255,255,255,0.5); 
        color: #fff; 
        gap: 8px;
    }}
    
    .yandex-btn.hidden {{
        display: none !important;
    }}
    
    .text-content-wrapper {{
        flex: 1;
        display: flex;
        flex-direction: column;
        min-height: 0;
    }}
    
    .text-content {{
        flex: 0 0 auto;
    }}
    
    .text-full {{
        font-size: 16px; 
        line-height: 1.6; 
        opacity: 0.9;
    }}
    
    .text-full p {{
        margin-bottom: 10px;
    }}
    
    .text-full p:last-child {{
        margin-bottom: 0;
    }}
    
    .text-content:not(.expanded) .text-full {{
        display: -webkit-box;
        -webkit-line-clamp: 8;
        -webkit-box-orient: vertical;
        overflow: hidden;
    }}
    
    .text-content.expanded .text-full {{
        display: block;
    }}
    
    .toggle-btn-wrapper {{ 
        width: 100%; 
        display: flex; 
        justify-content: flex-start;
        margin-top: 20px;
    }}
    
    .toggle-btn {{ 
        font-size: 14px; 
        padding: 10px 24px;
        display: none;
    }}
    
    .toggle-btn.visible {{
        display: inline-flex;
    }}
    
    .toggle-btn .arrow {{ font-size: 10px; transition: 0.3s; }}
    
    @media (min-width: 961px) {{
        .event-content {{
            min-height: 450px;
        }}
        .toggle-btn-wrapper {{
            margin-top: auto;
        }}
    }}
    
    @media (max-width: 960px) {{
        .event-wrapper {{ 
            flex-direction: column; 
            align-items: center; 
            padding: 70px 20px 40px;
        }}
        .event-image {{ 
            width: 100%; 
            max-width: 450px;
            height: auto;
            aspect-ratio: 1 / 1;
            flex: none; 
            margin-bottom: 8px;
        }}
        .event-title {{ 
            text-align: center; 
            width: 100%;
            margin-bottom: 8px;
        }}
        .event-datetime {{ 
            text-align: center; 
            width: 100%;
            margin-bottom: 20px;
        }}
        .buttons-row {{ justify-content: center; width: 100%; }}
        .toggle-btn-wrapper {{ justify-content: center; }}
        .text-content-wrapper {{ width: 100%; }}
        .text-full {{ text-align: justify !important; }}
        .event-content {{ min-height: 0; }}
        
        .text-content:not(.expanded) .text-full {{
            -webkit-line-clamp: 5;
        }}
    }}
    @media (max-width: 480px) {{
        .event-title {{ font-size: 28px; }}
        .buttons-row {{ flex-direction: column; align-items: center; }}
        .buy-btn, .yandex-btn {{ min-width: 260px; }} 
    }}
</style>

<script>
    function goBackSafe() {{
        const currentHost = window.location.hostname;
        const referrer = document.referrer;
        if (referrer && referrer.includes(currentHost)) {{
            window.history.back();
            setTimeout(() => {{
                if (window.location.href.includes(window.location.pathname)) {{
                   window.location.href = 'https://mtbarmoscow.com/';
                }}
            }}, 200);
        }} else {{
            window.location.href = 'https://mtbarmoscow.com/';
        }}
    }}
    
    function toggleText() {{
        const content = document.getElementById('textContent');
        const btnText = document.querySelector('.toggle-btn .btn-text');
        const arrow = document.querySelector('.toggle-btn .arrow');
        
        content.classList.toggle('expanded');
        btnText.textContent = content.classList.contains('expanded') ? 'Свернуть' : 'Читать далее';
        arrow.style.transform = content.classList.contains('expanded') ? 'rotate(180deg)' : 'rotate(0deg)';
    }}
    
    function checkIfToggleNeeded() {{
        const textContent = document.getElementById('textContent');
        const textFull = document.querySelector('.text-full');
        const toggleBtn = document.querySelector('.toggle-btn');
        
        if (!textFull || !toggleBtn) return;
        
        const lineHeight = parseFloat(window.getComputedStyle(textFull).lineHeight);
        const maxHeight = lineHeight * 8;
        
        if (textFull.scrollHeight > maxHeight) {{
            toggleBtn.classList.add('visible');
        }} else {{
            toggleBtn.classList.remove('visible');
        }}
    }}
    
    function checkYandexButton() {{
        const yandexBtn = document.querySelector('.yandex-btn');
        if (!yandexBtn) return;
        
        const yandexLink = yandexBtn.getAttribute('data-yandex-link');
        
        if (!yandexLink || yandexLink.trim() === '') {{
            yandexBtn.classList.add('hidden');
        }}
    }}
    
    window.addEventListener('DOMContentLoaded', function() {{
        checkIfToggleNeeded();
        checkYandexButton();
    }});
    window.addEventListener('resize', checkIfToggleNeeded);
</script>
"""
    
    return html


def format_date(date: str, time: str) -> str:
    """Форматирование даты и времени для отображения"""
    if not date:
        return "Дата уточняется"
    
    # Парсим дату (формат: DD.MM.YYYY)
    try:
        day, month, year = date.split('.')
        
        months_ru = {
            '01': 'января', '02': 'февраля', '03': 'марта', '04': 'апреля',
            '05': 'мая', '06': 'июня', '07': 'июля', '08': 'августа',
            '09': 'сентября', '10': 'октября', '11': 'ноября', '12': 'декабря'
        }
        
        month_name = months_ru.get(month, month)
        date_formatted = f"{int(day)} {month_name} {year}"
        
        if time:
            return f"{date_formatted} • {time}"
        else:
            return date_formatted
    
    except:
        return date if not time else f"{date} • {time}"


def format_description(description: str) -> str:
    """Форматирование описания с параграфами"""
    if not description:
        return "<p>Описание скоро появится</p>"
    
    # Разбиваем на параграфы
    paragraphs = description.split('\n\n')
    html_paragraphs = []
    
    for para in paragraphs:
        para = para.strip()
        if para:
            # Заменяем одиночные переносы на <br>
            para = para.replace('\n', '<br>')
            html_paragraphs.append(f"<p>{para}</p>")
    
    return '\n'.join(html_paragraphs) if html_paragraphs else f"<p>{description}</p>"
