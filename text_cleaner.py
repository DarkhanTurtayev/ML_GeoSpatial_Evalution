import pandas as pd
import re

df = pd.read_csv("db_full_uncleaned.csv")

df = df.drop(columns=["Unnamed: 0", "url", "title", "parsed_floors"])

def extract_kitchen_area(s):
    match = re.search(r"Площадь кухни — (\d+[.,]?\d*)", str(s))
    if match:
        return float(match.group(1).replace(",", "."))
    return None

df["kitchen_area"] = df["full_area"].apply(extract_kitchen_area)
df.drop(columns=["full_area"], inplace=True, errors="ignore")
# floor_info to floor_num и max_floor
def parse_floor(s):
    match = re.match(r"(\d+)\s*из\s*(\d+)", str(s))
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None

df[["floor_num", "max_floor"]] = df["floor_info"].apply(lambda x: pd.Series(parse_floor(x)))


df = df.drop(columns=["floor_info"])
#################
def remove_emojis(text):
    if not isinstance(text, str):
        return text
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # смайлы
        u"\U0001F300-\U0001F5FF"  # символы
        u"\U0001F680-\U0001F6FF"  # транспорт
        u"\U0001F1E0-\U0001F1FF"  # флаги
        u"\U00002700-\U000027BF"
        u"\U000024C2-\U0001F251"
        "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)

df["extra"] = df["extra"].apply(remove_emojis)
###################

def extract_height(s):
    if not isinstance(s, str):
        return None
    match = re.search(r"(\d+[.,]?\d*)", s)
    if match:
        return float(match.group(1).replace(",", "."))
    return None

df["ceiling_height"] = df["ceiling_height"].apply(extract_height)
df["complex_name"] = df["complex_name"].fillna("")
df["extra"] = df["extra"].fillna("")
df["build_year"] = pd.to_numeric(df["build_year"], errors="coerce")
df["parsed_rooms"] = pd.to_numeric(df["parsed_rooms"], errors="coerce")

df = df.dropna(subset=["parsed_area", "lat", "lon", "price"])


# price (target)
# parsed_area, floor_num, max_floor, parsed_rooms, build_year, lat, lon (Doubles)
# complex_name, building_type (Classes)
# extra (Textual)

df_clean = df.copy()
df_clean.to_csv("db_full_cleaned.csv")