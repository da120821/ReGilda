from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import json
import pandas as pd
import re
from database import db_manager

edge_driver_path = r'Driver_Notes/msedgedriver.exe'
cookies_file = 'cookies.json'

def parse_table_for_service(url):
    return parse_table(url)

def normalize_username(username):
    """Убираем префикс U из имен пользователей"""
    if username.startswith('U') and len(username) > 1:
        if username[1].isupper():
            return username[1:]
    return username


def clean_text(text):
    """Очистка текста от NBSP и лишних пробелов"""
    if not text:
        return text
    text = text.replace('\u00a0', ' ').replace('\u2007', ' ').replace('\u202f', ' ')
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def extract_user_data(user_cell):
    """Извлекает имя пользователя из ячейки"""
    try:
        # Ищем основной span с именем пользователя
        name_span = user_cell.find('span', class_='font-medium')
        if name_span:
            return clean_text(name_span.get_text())

        # Альтернативный поиск
        name_elements = user_cell.find_all(text=True)
        for text in name_elements:
            cleaned = clean_text(text)
            if cleaned and cleaned not in ['', 'Пользователь', 'User']:
                return cleaned

        return "Неизвестный"
    except:
        return "Ошибка извлечения"


def extract_amount_data(amount_cell):
    """Извлекает сумму из ячейки"""
    try:
        # Ищем badge с суммой
        badge = amount_cell.find('div', {'data-slot': 'badge'})
        if badge:
            # Извлекаем текст до иконки
            text_parts = []
            for element in badge.contents:
                if element.name == 'svg':
                    break
                if hasattr(element, 'get_text'):
                    text_parts.append(element.get_text())
                else:
                    text_parts.append(str(element))

            amount_text = ''.join(text_parts).strip()
            return clean_text(amount_text)

        return "0"
    except:
        return "0"


def extract_date_data(date_cell):
    """Извлекает дату из ячейки"""
    try:
        date_span = date_cell.find('span', class_='text-muted-foreground')
        if date_span:
            return clean_text(date_span.get_text())

        return "Неизвестная дата"
    except:
        return "Неизвестная дата"


def convert_amount_to_int(amount_str):
    """Преобразует строку суммы '1 500' в число 1500"""
    try:
        if isinstance(amount_str, (int, float)):
            return int(amount_str)

        if isinstance(amount_str, str):
            # Убираем все пробелы и нецифровые символы
            cleaned = re.sub(r'[^\d]', '', amount_str)
            if cleaned:
                return int(cleaned)

        return 0
    except:
        return 0


def parse_table(url='https://remanga.org/guild/i-g-g-d-r-a-s-i-l--a1172e3f/settings/donations'):
    """
    Парсит виртуализированную таблицу бустов
    """
    # Настройка браузера
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    options.add_argument(
        '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

    service = Service(edge_driver_path)
    driver = webdriver.Edge(service=service, options=options)
    wait = WebDriverWait(driver, 120)

    try:
        # Открываем сайт
        print("Открываем страницу...")
        driver.get(url)

        # Загружаем куки
        print("Загружаем куки...")
        try:
            with open(cookies_file, 'r') as file:
                cookies = json.load(file)
            for cookie in cookies:
                driver.add_cookie(cookie)
        except Exception as e:
            print(f"Ошибка загрузки куки: {e}")

        # Перезагружаем страницу
        print("Перезагружаем страницу...")
        driver.refresh()
        time.sleep(10)

        # Ждем загрузки виртуализированной таблицы
        print("Ожидаем загрузки таблицы...")
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-sentry-component='VirtualizedDataTable']")))

        # Хранилище данных
        rows_data = []
        seen_records = set()
        previous_count = 0
        no_new_count = 0
        max_scroll_attempts = 50

        print("Начинаем сбор данных с прокруткой...")

        for attempt in range(max_scroll_attempts):
            # Получаем HTML
            page_html = driver.page_source
            soup = BeautifulSoup(page_html, 'html.parser')

            # Находим контейнер таблицы
            table_container = soup.find('div', {'data-sentry-component': 'VirtualizedDataTable'})
            if not table_container:
                print("Таблица не найдена")
                break

            # Находим тело таблицы
            tbody = table_container.find('tbody', {'data-sentry-component': 'TableBody'})
            if not tbody:
                print("Тело таблицы не найдено")
                break

            # Ищем все строки с абсолютным позиционированием
            rows = tbody.find_all('tr', style=re.compile(r'position:\s*absolute'))

            print(f"Попытка {attempt + 1}: найдено {len(rows)} строк")

            # Обрабатываем строки
            for row in rows:
                try:
                    # Получаем все ячейки
                    cells = row.find_all(['td', 'th'])
                    if len(cells) < 3:
                        continue

                    # Извлекаем данные из каждой ячейки
                    user_cell, amount_cell, date_cell = cells[0], cells[1], cells[2]

                    user = extract_user_data(user_cell)
                    amount = extract_amount_data(amount_cell)
                    date = extract_date_data(date_cell)

                    # Пропускаем заголовки и пустые строки
                    if not user or user in ['Пользователь', 'User', 'Неизвестный']:
                        continue

                    # Создаем уникальный идентификатор
                    row_id = f"{user}|{amount}|{date}"

                    if row_id not in seen_records:
                        rows_data.append([user, amount, date])
                        seen_records.add(row_id)

                except Exception as e:
                    print(f"Ошибка обработки строки: {e}")
                    continue

            print(f"Собрано записей: {len(rows_data)}")

            # Проверяем прогресс
            if len(rows_data) == previous_count:
                no_new_count += 1
                if no_new_count >= 5:
                    print("Новых данных нет, завершаем...")
                    break
            else:
                no_new_count = 0
                previous_count = len(rows_data)

            # Прокрутка вниз
            try:
                # Прокручиваем контейнер таблицы
                container = driver.find_element(By.CSS_SELECTOR, "[data-sentry-component='VirtualizedDataTable']")
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", container)
                time.sleep(2)

                # Дополнительная прокрутка страницы
                driver.execute_script("window.scrollBy(0, 500);")
                time.sleep(1)

            except Exception as e:
                print(f"Ошибка прокрутки: {e}")
                # Пробуем альтернативную прокрутку
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

        print(f"\n=== ЗАВЕРШЕНО ===")
        print(f"Всего собрано записей: {len(rows_data)}")

        # Создаем DataFrame
        if rows_data:
            df = pd.DataFrame(rows_data, columns=['Пользователь', 'Сумма', 'Дата'])
            df = df.drop_duplicates()
            print(f"Итоговое количество уникальных записей бустов: {len(df)}")

            print("🔄 Преобразуем суммы в числовой формат...")
            df['Сумма'] = df['Сумма'].apply(convert_amount_to_int)

            # Выводим статистику по новым бустам
            new_stats = db_manager.get_new_busters_stats(df)
            if new_stats:
                print("📊 Статистика собранных бустов:")
                print(f"  - Всего собрано бустов: {len(df)}")
                print(f"  - Новых бустов: {new_stats['new_busters_count']}")
                print(f"  - Новых бустеров: {new_stats['new_busters_users_count']}")
                print(f"  - Сумма новых бустов: {new_stats['new_busters_amount']:,} ⚡")
                print(f"  - Дубликатов будет пропущено: {len(df) - new_stats['new_busters_count']}")
            else:
                print("📊 Статистика собранных бустов:")
                print(f"  - Всего собрано бустов: {len(df)}")
                print(f"  - Уникальных бустеров: {df['Пользователь'].nunique()}")
                print(f"  - Общая сумма бустов: {df['Сумма'].sum():,} ⚡")

            print("💾 Сохраняем данные бустов в базу данных...")
            if db_manager.save_to_iggdrasil(df):
                print("✅ Данные бустов успешно сохранены в БД")
            else:
                print("❌ Ошибка сохранения бустов в БД")

            return df
        else:
            print("❌ Не удалось собрать данные бустов")
            return pd.DataFrame()

    except Exception as e:
        print(f"Произошла ошибка: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
    finally:
        print("Закрываем браузер...")
        driver.quit()