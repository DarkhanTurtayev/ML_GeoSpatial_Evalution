import requests
import threading
import queue
import random
import time
import re
import json
import csv
import os
import pandas as pd
from tkinter import filedialog, Tk
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pickle
from webdriver_manager.chrome import ChromeDriverManager

def get_browser_headers_and_cookies():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
    driver.get("https://krisha.kz/")
    time.sleep(3)
    cookies_list = driver.get_cookies()
    ua = driver.execute_script("return navigator.userAgent;")
    driver.quit()

    cookies = {cookie['name']: cookie['value'] for cookie in cookies_list}
    headers = {
        "User-Agent": ua,
        "Referer": "https://krisha.kz/",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8",
        "Connection": "keep-alive",
    }
    return headers, cookies

def parse_html_and_save(url, headers, cookies):
    try:
        response = requests.get(url, headers=headers, cookies=cookies, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        description_div = soup.find("div", class_="js-description a-text a-text-white-spaces")
        html_description = description_div.get_text(strip=True) if description_div else None
        response.raise_for_status()

        match = re.search(r"window\.data\s*=\s*({.*?});", response.text, re.DOTALL)
        if not match:
            raise ValueError("JSON not found in HTML")

        data_json = json.loads(match.group(1))
        advert = data_json.get("advert", {})
        title = advert.get("title")

        try:
            parts = title.split("·")
            area_text = parts[1].strip().replace("м²", "").strip() if len(parts) > 1 else None
            floor_text = parts[2].strip() if len(parts) > 2 else None
        except Exception:
            area_text = floor_text = None

        price = advert.get("price")
        rooms = advert.get("rooms")
        lat = advert.get("map", {}).get("lat")
        lon = advert.get("map", {}).get("lon")

        def extract_short_info(data_name):
            div = soup.find("div", attrs={"data-name": data_name})
            if div:
                info_div = div.find("div", class_="offer__advert-short-info")
                return info_div.get_text(strip=True) if info_div else None
            return None

        building_type = extract_short_info("flat.building")
        complex_name = extract_short_info("map.complex")
        build_year = extract_short_info("house.year")
        floor_info = extract_short_info("flat.floor")
        full_area = extract_short_info("live.square")
        ceiling_height = extract_short_info("ceiling")

        with log_lock:
            with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([url, title, price, area_text, floor_text, rooms, html_description, lat, lon, building_type, complex_name, build_year, floor_info, full_area, ceiling_height])

    except Exception as e:
        with log_lock:
            with open("/mnt/data/failed_links.log", "a") as f:
                f.write(f"{url} — {str(e)}\n")

def worker(url_chunk, headers, cookies):
    for url in url_chunk:
        parse_html_and_save(url, headers, cookies)
        time.sleep(random.uniform(1, 3))

NUM_WORKERS = 5
OUTPUT_FILE = os.path.join(os.getcwd(), f"flats_data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv")
CSV_HEADERS = ["url", "title", "price", "parsed_area", "parsed_floors", "parsed_rooms", "extra", "lat", "lon",
               "building_type", "complex_name", "build_year", "floor_info", "full_area", "ceiling_height"]
if not os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)
HEADERS_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
]
log_lock = threading.Lock()

root = Tk()
root.withdraw()
file_path = filedialog.askopenfilename(title="Выбери Excel-файл со ссылками", filetypes=[("Excel files", "*.csv")])
if not file_path:
    raise Exception("Файл не выбран.")

try:
    df = pd.read_csv(file_path, sep=",", on_bad_lines='skip', encoding="utf-8")
    if df.shape[1] == 1:
        df = pd.read_csv(file_path, sep=";", on_bad_lines='skip', encoding="utf-8")
except Exception as e:
    raise Exception(f"Ошибка при чтении CSV: {e}")

if "Ссылка" not in df.columns:
    raise Exception("Файл не содержит колонку 'Ссылка'.")

example_urls = df["Ссылка"].dropna().unique().tolist()

def chunkify(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

url_chunks = list(chunkify(example_urls, 50))

for i, chunk in enumerate(url_chunks):
    print(f"\n🔄 Чанк {i + 1}/{len(url_chunks)} — получаем cookies...")
    headers, cookies = get_browser_headers_and_cookies()

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        for j in range(NUM_WORKERS):
            part = chunk[j::NUM_WORKERS]
            executor.submit(worker, part, headers, cookies)

    time.sleep(random.uniform(10, 20))