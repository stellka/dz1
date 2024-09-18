from bs4 import BeautifulSoup
import requests
from datetime import datetime
import time
import pandas as pd
import os
from collections import Counter

# Словарь для нормализации брендов
brand_normalization = {
    'AT&T Inc.': 'AT&T',
    'AT&T': 'AT&T',
    'Google LLC': 'Google',
    'Google': 'Google',
}

def normalize_brand(brand_name):
    return brand_normalization.get(brand_name, brand_name)

# Функция для сбора данных с сайта
def fetch_data():
    url_openphish = 'https://openphish.com/'
    page = requests.get(url_openphish, stream=True, allow_redirects=True, timeout=10, verify=False)
    soup = BeautifulSoup(page.text, "html.parser")
    table = soup.find('table', class_='pure-table pure-table-striped')
    internal_table = table.find('tbody')

    alive_sites = []
    now = datetime.now()
    current_time = now.strftime("%m/%d/%Y %H:%M:%S")
    date = current_time.split(" ")[0]

    for tr in internal_table.find_all('tr'):
        row = []
        for td in tr.find_all('td'):
            row.append(td.text.strip())

        if row:
            url = row[0]
            target = normalize_brand(row[1])  # Нормализация бренда
            time_str = date + " " + row[2]
            datetime_object = datetime.strptime(time_str, "%m/%d/%Y %H:%M:%S")

            if (now - datetime_object).total_seconds() / 60 - 180 < 16:
                alive_sites.append([url, target, time_str])

    return alive_sites

# Функция для проверки на дубликаты
def remove_duplicates(new_data, existing_data):
    existing_urls = set(existing_data['URL'].tolist()) if not existing_data.empty else set()
    # Оставляем только те записи, URL которых нет в уже существующих данных
    unique_data = [row for row in new_data if row[0] not in existing_urls]
    return unique_data

# Функция для подсчета и вывода топ-3 наиболее атакуемых брендов
def print_top_brands(existing_data):
    # Подсчитываем количество упоминаний каждого бренда
    brand_counter = Counter(existing_data['Target'].tolist())
    # Получаем топ-3 брендов
    top_3_brands = brand_counter.most_common(3)
    print("\nТоп 3 наиболее часто атакуемых брендов:")
    for brand, count in top_3_brands:
        print(f"{brand}: {count} атак")

# Основной цикл с интервалом в 5 минут
def run_scraper(interval=300, output_file="test2.csv"):
    # Проверка существования файла и загрузка существующих данных
    if os.path.exists(output_file):
        existing_data = pd.read_csv(output_file, names=["URL", "Target", "Time"])
    else:
        existing_data = pd.DataFrame(columns=["URL", "Target", "Time"])

    try:
        while True:
            # Получаем новые данные
            alive_sites = fetch_data()

            # Проверяем на дубликаты
            alive_sites = remove_duplicates(alive_sites, existing_data)

            if alive_sites:
                # Добавляем новые записи в DataFrame
                new_data_df = pd.DataFrame(alive_sites, columns=["URL", "Target", "Time"])
                # Добавляем новые данные к существующим
                new_data_df.to_csv(output_file, mode='a', header=False, index=False)
                # Обновляем существующие данные для последующих итераций
                existing_data = pd.concat([existing_data, new_data_df])
                print(f"{len(alive_sites)} новых записей добавлено в {datetime.now()}")

            else:
                print(f"Нет новых данных в {datetime.now()}")

            # Ожидание перед следующим запросом
            time.sleep(interval)

    except KeyboardInterrupt:
        # При прерывании работы программы (Ctrl + C), выводим топ-3 атакуемых брендов
        print("\nПроцедура завершена.")
        print_top_brands(existing_data)

# Запуск скрипта
run_scraper()
