from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import json
import pandas as pd
import re
import os
import subprocess

cookies_file = 'cookies.json'


def setup_driver():
    """Настраивает драйвер для Browserless на Railway"""
    chrome_options = Options()

    # Аргументы для Railway
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")

    # Для обхода блокировок
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    try:
        browserless_endpoint = os.environ.get('BROWSERLESS_ENDPOINT','https://standalone-chrome-browserless.up.railway.app/wd/hub')
        driver = webdriver.Remote(
            command_executor=browserless_endpoint,
            options=chrome_options
        )
        print(f"✅ Успешно подключились к Browserless: {browserless_endpoint}")
        return driver
    except Exception as e:
        print(f"❌ Ошибка подключения к Browserless: {e}")
        return None


def check_chrome_installation():
    """Проверяет установлен ли Chrome/Chromium"""
    print("🔍 Проверяем подключение к Browserless...")

    try:
        driver = setup_driver()
        if driver:
            driver.quit()
            print("✅ Browserless доступен")
            return True
        else:
            print("❌ Browserless недоступен")
            return False
    except Exception as e:
        print(f"❌ Ошибка проверки Browserless: {e}")
        return False


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


def extract_guild_name_from_url(url):
    """Извлекает красивое название гильдии из URL"""
    try:
        # Извлекаем часть URL между /guild/ и /settings
        match = re.search(r'/guild/([^/]+)', url)
        if match:
            guild_slug = match.group(1)

            print(f"🔍 Извлекаем из URL: {guild_slug}")

            # Убираем уникальный ID в конце (например, --a1172e3f)
            guild_slug_clean = re.sub(r'--[a-f0-9]{8}$', '', guild_slug)

            # Если после очистки осталась пустая строка, используем оригинальный slug
            if not guild_slug_clean:
                guild_slug_clean = guild_slug

            # Преобразуем slug в читаемое название
            # "i-g-g-d-r-a-s-i-l" → "Iggdrasil"
            guild_name = guild_slug_clean.replace('-', ' ')

            # Убираем лишние пробелы и делаем правильный title case
            guild_name = ' '.join(word.capitalize() for word in guild_name.split())

            # Специальная обработка для аббревиатур типа "I G G D R A S I L"
            if len(guild_name) > 10 and ' ' in guild_name:
                # Если много отдельных букв, пробуем объединить их
                words = guild_name.split()
                if all(len(word) == 1 for word in words):
                    guild_name = ''.join(words).capitalize()

            print(f"✅ Преобразовано в: '{guild_name}'")
            return guild_name

        return "Неизвестная гильдия"
    except Exception as e:
        print(f"❌ Ошибка извлечения названия гильдии: {e}")
        return "Неизвестная гильдия"


def parse_table(url='https://remanga.org/guild/i-g-g-d-r-a-s-i-l--a1172e3f/settings/donations'):
    """
    Парсит виртуализированную таблицу бустов через Browserless
    """
    # Настройка браузера через Browserless
    driver = setup_driver()
    if not driver:
        print("❌ Не удалось подключиться к Browserless")
        return pd.DataFrame()

    wait = WebDriverWait(driver, 120)
    rows_data = []
    seen_records = set()
    previous_count = 0
    no_new_count = 0
    max_scroll_attempts = 50

    try:
        # ВОССТАНАВЛИВАЕМ получение названия гильдии (только для отображения)
        guild_name = extract_guild_name_from_url(url)

        # Открываем сайт
        print(f"Открываем страницу гильдии '{guild_name}'...")
        driver.get(url)


        # Загружаем куки
        print("Загружаем куки...")
        try:
            cookies = None
            cookies_json = os.getenv('COOKIES_JSON')
            if cookies_json:
                cookies = json.loads(cookies_json)
                print("✅ Куки загружены из переменных окружения")
            else:
                with open(cookies_file, 'r') as file:
                    cookies = json.load(file)
                print("✅ Куки загружены из файла")

            # Добавляем куки в браузер ДО перехода на страницу
            if cookies:
                # Сначала переходим на домен, чтобы установить куки
                driver.get("https://remanga.org")
                time.sleep(2)

                for cookie in cookies:
                    try:
                        # Убираем лишние поля которые могут мешать
                        cookie_copy = cookie.copy()
                        if 'sameSite' in cookie_copy:
                            cookie_copy['sameSite'] = 'Lax'
                        driver.add_cookie(cookie_copy)
                    except Exception as e:
                        print(f"⚠️ Ошибка добавления куки {cookie.get('name')}: {e}")

                print(f"✅ Добавлено {len(cookies)} куков")

                # Проверяем куки
                current_cookies = driver.get_cookies()
                print(f"📊 Текущие куки в браузере: {len(current_cookies)}")

            else:
                print("❌ Куки не загружены!")

        except Exception as e:
            print(f"Ошибка загрузки куки: {e}")

        # Перезагружаем страницу
        print("Перезагружаем страницу...")
        driver.refresh()
        time.sleep(10)

        # Ждем загрузки виртуализированной таблицы
        print("Ожидаем загрузки таблицы...")
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-sentry-component='VirtualizedDataTable']")))

        print("Начинаем сбор данных с прокруткой...")

        # ⚡ ДОБАВЛЯЕМ ОСНОВНОЙ БЛОК ПАРСИНГА!
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

            print("📊 Статистика собранных бустов:")
            print(f"  - Всего собрано бустов: {len(df)}")
            print(f"  - Уникальных бустеров: {df['Пользователь'].nunique()}")
            print(f"  - Общая сумма бустов: {df['Сумма'].sum():,} ⚡")

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