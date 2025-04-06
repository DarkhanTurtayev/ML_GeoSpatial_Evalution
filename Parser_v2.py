import tkinter as tk
from tkinter import simpledialog, filedialog
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
import re
from datetime import datetime
from time import sleep
import os

# Ввод ссылки через диалоговое окно
root = tk.Tk()
root.withdraw()
url = simpledialog.askstring(title="Krisha.kz Parser", prompt="Enter URL")

# Настройка ChromeDriver
chrome_options = Options()
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-popup-blocking")
chrome_options.add_argument("--disable-notifications")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-infobars")
# блокируем изображения, уведомления и медиа
prefs = {
    "profile.managed_default_content_settings.images": 2,
    "profile.default_content_setting_values.notifications": 2,
    "profile.default_content_setting_values.plugins": 2,
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.set_capability("pageLoadStrategy", "eager")

driver = webdriver.Chrome(options=chrome_options)
driver.set_page_load_timeout(60)

# Определение количества страниц
driver.get(url)
subtitle_text = driver.find_element(By.CLASS_NAME, 'a-search-subtitle').text
total_ads = int(''.join(filter(str.isdigit, subtitle_text)))
last_page = (total_ads // 20) + 2 if total_ads >= 20 else 2

# Проверка типа пагинации
try:
    driver.get(url + '&page=2')
    driver.find_element(By.CLASS_NAME, 'error-content__title')
    page_param = '?page='
except:
    page_param = '&page='

# Сбор данных
data = {
    'Ссылка': [], 'Комнаты': [], 'Площадь': [], 'Этаж': [], 'Доп сведения': [],
    'Цена': [], 'Район': [], 'Улица': [], 'Адрес': [],
    'Тип': [], 'Год Постройки': [], 'Цена за квадрат': [], 'Страница': []
}

for page in range(1, last_page + 1):
    try:
        driver.get(url + page_param + str(page))
        current_page = page
        titles = driver.find_elements(By.CLASS_NAME, 'a-card__title')
        subtitles = driver.find_elements(By.CLASS_NAME, 'a-card__subtitle')
        previews = driver.find_elements(By.CLASS_NAME, 'a-card__text-preview')
        prices = driver.find_elements(By.CLASS_NAME, 'a-card__price')

        for i in range(len(titles)):
            try:
                title_text = titles[i].text
                link = titles[i].get_attribute('href')

                # Пример строки: "2 · 60 м² · 3/3 этаж"
                parts = [p.strip() for p in title_text.split('·')]

                rooms = parts[0] if len(parts) > 0 else None

                try:
                    area_match = re.findall(r'\d+\.?\d*', parts[1]) if len(parts) > 1 else None
                    area = float(area_match[0]) if area_match else None
                except:
                    area = None

                floor = parts[2] if len(parts) > 2 else None
            except:
                link, rooms, area, floor = None, None, None, None

            try:
                address_text = subtitles[i].text
                address_parts = address_text.split(',')
                district = address_parts[0].strip()
                street = address_parts[1].strip() if len(address_parts) > 1 else None
            except:
                district, street, address_text = None, None, None

            try:
                preview_parts = previews[i].text.split(',')
                building_type = preview_parts[0] if not any(char.isdigit() for char in preview_parts[0]) else None
                year = int(re.sub(r'\D', '', preview_parts[1])) if len(preview_parts) > 1 else None
                year = year if year and year >= 1900 else None
                extra = previews[i].text
            except:
                building_type, year = None, None

            try:
                price_raw = prices[i].text.replace('\xa0', '')
                price = int(''.join(filter(str.isdigit, price_raw)))
                price_per_sq = round(price / area) if area else None
            except:
                price, price_per_sq = None, None

            data['Ссылка'].append(link)
            data['Комнаты'].append(rooms)
            data['Площадь'].append(area)
            data['Этаж'].append(floor)
            data['Доп сведения'].append(extra)
            data['Цена'].append(price)
            data['Район'].append(district)
            data['Улица'].append(street)
            data['Адрес'].append(address_text)
            data['Тип'].append(building_type)
            data['Год Постройки'].append(year)
            data['Цена за квадрат'].append(price_per_sq)
            data['Страница'].append(current_page)

        sleep(1)
    except Exception as e:
        print(f"Ошибка на странице {page}: {e}")
        continue

driver.quit()

# Сохранение в CSV
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
file_path = os.path.join(os.getcwd(), f"krisha_data_{timestamp}.csv")
pd.DataFrame(data).to_csv(file_path, mode='a', header=True, index=False)
print(f"Файл сохранён: {file_path}")
