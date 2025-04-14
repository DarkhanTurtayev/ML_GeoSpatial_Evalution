import os
import csv
import json
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from tkinter import filedialog, Tk
from bs4 import BeautifulSoup
import regex as re

OUTPUT_FILE = "db.csv"
PROGRESS_FILE = "progress.json"
CSV_HEADERS = ["url", "title", "price", "parsed_area", "parsed_floors", "parsed_rooms", "extra", "lat", "lon",
               "building_type", "complex_name", "build_year", "floor_info", "full_area", "ceiling_height"]

def init_driver():

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
# save traffic
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2,
        "profile.default_content_setting_values.plugins": 2,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.set_capability("pageLoadStrategy", "eager")

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(60)
    # options.add_argument("--headless")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    return webdriver.Chrome(options=chrome_options)

def save_to_csv(row):
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)

def parse_page(driver, url):
    driver.get(url)
    time.sleep(3)
    soup = BeautifulSoup(driver.page_source, "html.parser")

    description_div = soup.find("div", class_="js-description a-text a-text-white-spaces")
    html_description = description_div.get_text(strip=True) if description_div else None

    data_script = soup.find("script", text=lambda x: x and "window.data" in x)
    if not data_script:
        raise Exception("Нет JSON в странице")

    json_text = data_script.string
    match = re.search(r"window\.data\s*=\s*({.*?});", json_text, re.DOTALL)
    data_json = json.loads(match.group(1))

    advert = data_json.get("advert", {})
    title = advert.get("title")
    parts = title.split("·") if title else []
    area_text = parts[1].strip().replace("м²", "").strip() if len(parts) > 1 else None
    floor_text = parts[2].strip() if len(parts) > 2 else None

    def extract_short_info(data_name):
        div = soup.find("div", attrs={"data-name": data_name})
        if div:
            info_div = div.find("div", class_="offer__advert-short-info")
            return info_div.get_text(strip=True) if info_div else None
        return None

    row = [
        url,
        title,
        advert.get("price"),
        area_text,
        floor_text,
        advert.get("rooms"),
        html_description,
        advert.get("map", {}).get("lat"),
        advert.get("map", {}).get("lon"),
        extract_short_info("flat.building"),
        extract_short_info("map.complex"),
        extract_short_info("house.year"),
        extract_short_info("flat.floor"),
        extract_short_info("live.square"),
        extract_short_info("ceiling"),
    ]

    with open(PROGRESS_FILE, "w") as pf:
        json.dump({"last_url": url}, pf)
    save_to_csv(row)

def get_urls():
    root = Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(title="Выбери Excel-файл со ссылками", filetypes=[("Excel files", "*.csv")])
    if not file_path:
        raise Exception("Файла нет.") 
    df = pd.read_csv(file_path, sep=None, engine='python')
    if "Ссылка" not in df.columns:
        raise Exception("Файл не содержит колонку 'Ссылка'.")
    return df["Ссылка"].dropna().unique().tolist()

if __name__ == "__main__":
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADERS)

    urls = get_urls()
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            data = json.load(f)
            if data.get("last_url") in urls:
                idx = urls.index(data["last_url"])
                urls = urls[idx:]

    driver = init_driver()
    try:
        for url in urls:
            try:
                print("Поик", url)
                parse_page(driver, url)
                time.sleep(2)
            except Exception as e:
                print("Ошибка:", e)
                continue
    finally:
        driver.quit()