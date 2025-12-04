import os
import re
import json
import requests
from tqdm import tqdm
from time import sleep
from bs4 import BeautifulSoup
from datetime import datetime


BORABOT_URL = "https://hil.ar/bora/"
OFFICIAL_NORMS_DIR_PATH = "boletines"
FILES_PATH = os.path.join("..", "data", OFFICIAL_NORMS_DIR_PATH)

TITLES = [
    r"\, Cont\.? Púb\.?|Cont\.? Púb\.?",
    r"Abogad[oa]\.?|Abg\.?|Abog\.?|Abgda:?",
    r"Ingenier[oa]|e Ing\.?|Ing\.?",
    r"Licenciad[oa]|Lic\.?|Lc\.?",
    r"Inspector General",
    r"Dr\.?|Dra\.?|Da\.?|D\.",
    r"Magíster|Magister",
    r"Ag\.",
    r"Tec\.",
    r"a\.",
]

__titles_pattern = re.compile(
    rf"^((({'|'.join(TITLES)})(\s+y\s+)?\s*)+)",
    flags=re.IGNORECASE
)


def __clean_name(name: str) -> str:
    cleaned = re.sub(r"^[,\s]+", "", name)          # quita comas y espacios al principio
    cleaned = __titles_pattern.sub("", cleaned)     # quita titulos
    cleaned = re.sub(r"\(\*\)\s*$", "", cleaned)    # quita esto: (*) al final
    return re.sub(r"\s+", " ", cleaned).strip()


def __normalize_record(record: dict) -> dict:
    if 'name' in record:
        record['name'] = __clean_name(record['name'])
        return record

    normalized = {
        "name": __clean_name(record.get("nombre_completo")),
        "gov_id": record.get("dni_cuit"),
        "gov_section": record.get("seccion_gobierno"),
        "position": record.get("cargo"),
        "position_start": record.get("fecha_inicio"),
        "position_duration_days": record.get("duracion_dias"),
        "via": record.get("via"),
        "norm_official_id": record.get("norm_official_id"),
        "norm_publish_date": record.get("norm_publish_date"),
    }
    return normalized


def __normalize_records(records: dict) -> dict:
    return {
        'in': [__normalize_record(record) for record in records['in']],
        'out': [__normalize_record(record) for record in records['out']],
    }


def download_all_official_norms():
    response = requests.get(BORABOT_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    li_tags = soup.find_all('li')  # vienen ordenados por fecha de manera decreciente
    li_tags.reverse()

    appointments_and_resignations = {}
    pattern = r"Boletín oficial del (\d{2}/\d{2}/\d{2})"
    today = datetime.today().strftime('%Y_%m_%d')

    for li in tqdm(li_tags):
        a_tag = li.find('a')
        li_text = li.get_text(strip=True)

        if match := re.search(pattern, li_text):
            li_date = match.group(1)
        else:
            raise AssertionError(f"Unexpected format for li tag with text: {li_text}")

        if a_tag and 'href' in a_tag.attrs:
            href = a_tag['href']
            json_url = f"{BORABOT_URL}{href}.personal.json"

            json_response = requests.get(json_url)
            if json_response.status_code == 200:
                data = json_response.json()
                appointments_and_resignations[li_date] = __normalize_records(data)
            else:
                raise AssertionError(f"JSON file for {li_date} not found under URL {json_url}")
        sleep(0.5)

    with open(os.path.join(FILES_PATH, f"{today}.json"), "w", encoding="utf-8") as f:
        json.dump(appointments_and_resignations, f, ensure_ascii=False, indent=2)
