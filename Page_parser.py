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

# Настройки
NUM_WORKERS = 10
OUTPUT_FILE = os.path.join(os.getcwd(), f"flats_data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv")
CSV_HEADERS = ["url", "price", "rooms", "area", "floor", "extra", "lat", "lon"]
if not os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)
HEADERS_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
]
# Очередь ссылок
url_queue = queue.Queue()
log_lock = threading.Lock()

# Загружаем ссылки из Excel
root = Tk()
root.withdraw()
file_path = filedialog.askopenfilename(title="Выбери Excel-файл со ссылками", filetypes=[("Excel files", "*.xlsx")])
if not file_path:
    raise Exception("Файл не выбран.")

df = pd.read_excel(file_path)
if "Ссылка" not in df.columns:
    raise Exception("Файл не содержит колонку 'Ссылка'.")

example_urls = df["Ссылка"].dropna().unique().tolist()

# Заполняем очередь
for url in example_urls:
    url_queue.put(url)

def parse_html_and_save(url):
    try:
        headers = {'User-Agent': random.choice(HEADERS_LIST)}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        # Ищем скрипт с данными
        match = re.search(r"window\.data\s*=\s*({.*?});", response.text, re.DOTALL)
        if not match:
            raise ValueError("JSON not found in HTML")

        data_json = json.loads(match.group(1))
        advert = data_json.get("advert", {})
        price = advert.get("price")
        rooms = advert.get("rooms")
        area = advert.get("area")
        floor = advert.get("floor")
        extra = advert.get("description")
        lat = advert.get("map", {}).get("lat")
        lon = advert.get("map", {}).get("lon")

        with log_lock:
            with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([url, price, rooms, area, floor, extra, lat, lon])

    except Exception as e:
        with log_lock:
            with open("/mnt/data/failed_links.log", "a") as f:
                f.write(f"{url} — {str(e)}\n")

def worker():
    while not url_queue.empty():
        url = url_queue.get()
        parse_html_and_save(url)
        time.sleep(random.uniform(0.5, 1.5))
        url_queue.task_done()

# Запускаем многопоточность
with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
    for _ in range(NUM_WORKERS):
        executor.submit(worker)