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
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

cookies_file = 'cookies.json'


def setup_driver():
    """Настраивает драйвер для Browserless"""
    chrome_options = Options()

    # Аргументы для Chrome
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--ignore-ssl-errors")
    chrome_options.add_argument("--allow-insecure-localhost")
    chrome_options.add_argument("--allow-running-insecure-content")

    # Добавляем дополнительные опции для стабильности
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    try:
        # Используем переменные окружения Railway
        browser_token = os.environ.get('BROWSER_TOKEN', '1gk2gW97XgdGHg9kZeEsefMW0GrfP49md66r48BWwFADYm3j')

        # Используем публичный endpoint из переменных окружения
        browserless_endpoint = os.environ.get(
            'BROWSER_WEBDRIVER_ENDPOINT',
            'https://browserless-browserless.up.railway.app/webdriver'
        )

        # Формируем URL с токеном
        if '?' in browserless_endpoint:
            webdriver_url = f"{browserless_endpoint}&token={browser_token}"
        else:
            webdriver_url = f"{browserless_endpoint}?token={browser_token}"

        print(f"🔗 Подключаемся к Browserless: {webdriver_url}")

        driver = webdriver.Remote(
            command_executor=webdriver_url,
            options=chrome_options
        )

        # Устанавливаем таймауты
        driver.set_page_load_timeout(30)
        driver.implicitly_wait(10)

        # Добавляем скрипт для маскировки веб-драйвера
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        print(f"✅ Успешно подключились к Browserless")
        return driver

    except Exception as e:
        print(f"❌ Ошибка подключения к Browserless: {e}")
        return None


def check_browserless_connection():
    """Проверяет подключение к Browserless"""
    print("🔍 Проверяем подключение к Browserless...")

    try:
        driver = setup_driver()
        if driver:
            # Проверяем, что браузер работает
            driver.get("https://www.google.com")
            title = driver.title
            driver.quit()
            print(f"✅ Browserless доступен, заголовок: {title}")
            return True
        else:
            print("❌ Browserless недоступен")
            return False
    except Exception as e:
        print(f"❌ Ошибка проверки Browserless: {e}")
        return False


def parse_table_for_service(url):
    return parse_table(url)


def normalize_username(username):
    """Убираем префикс U из имен пользователей"""
    if username and username.startswith('U') and len(username) > 1:
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


def load_cookies(driver):
    """Загружает куки в браузер"""
    try:
        cookies = None
        cookies_json = os.getenv('COOKIES_JSON')

        if cookies_json:
            cookies = json.loads(cookies_json)
            print("✅ Куки загружены из переменных окружения")
        elif os.path.exists(cookies_file):
            with open(cookies_file, 'r', encoding='utf-8') as file:
                cookies = json.load(file)
            print("✅ Куки загружены из файла")
        else:
            print("❌ Куки не найдены ни в переменных окружения, ни в файле")
            return False

        # Сначала переходим на домен, чтобы установить куки
        print("Переходим на домен для установки кук...")
        driver.get("https://remanga.org")
        time.sleep(3)

        # Удаляем существующие куки и добавляем новые
        driver.delete_all_cookies()

        cookies_added = 0
        for cookie in cookies:
            try:
                # Убираем лишние поля которые могут мешать
                cookie_copy = {k: v for k, v in cookie.items()
                               if k in ['name', 'value', 'domain', 'path', 'expiry', 'secure', 'httpOnly']}

                # Убеждаемся, что домен правильный
                if 'domain' in cookie_copy:
                    if cookie_copy['domain'].startswith('.'):
                        cookie_copy['domain'] = cookie_copy['domain'][1:]
                    # Убеждаемся, что домен соответствует remanga.org
                    if 'remanga.org' not in cookie_copy['domain']:
                        continue

                driver.add_cookie(cookie_copy)
                cookies_added += 1
            except Exception as e:
                print(f"⚠️ Ошибка добавления куки {cookie.get('name')}: {e}")

        print(f"✅ Добавлено {cookies_added} куков")

        # Проверяем куки
        current_cookies = driver.get_cookies()
        print(f"📊 Текущие куки в браузере: {len(current_cookies)}")

        return True

    except Exception as e:
        print(f"❌ Ошибка загрузки куки: {e}")
        return False


def parse_table(url='https://remanga.org/guild/i-g-g-d-r-a-s-i-l--a1172e3f/settings/donations'):
    """
    Парсит виртуализированную таблицу бустов через Browserless
    """
    # Сначала проверяем подключение
    if not check_browserless_connection():
        print("❌ Browserless недоступен, пропускаем парсинг")
        return pd.DataFrame()

    # Настройка браузера через Browserless
    driver = setup_driver()
    if not driver:
        print("❌ Не удалось подключиться к Browserless")
        return pd.DataFrame()

    wait = WebDriverWait(driver, 30)
    rows_data = []
    seen_records = set()
    previous_count = 0
    no_new_count = 0
    max_scroll_attempts = 20

    try:
        # Получаем название гильдии
        guild_name = extract_guild_name_from_url(url)

        # Загружаем куки ДО перехода на целевую страницу
        print("Загружаем куки...")
        cookies_loaded = load_cookies(driver)

        if not cookies_loaded:
            print("❌ Не удалось загрузить куки, продолжаем без них...")

        # Открываем целевую страницу
        print(f"Открываем страницу гильдии '{guild_name}'...")
        driver.get(url)
        time.sleep(5)

        # Проверяем, загрузилась ли страница
        current_url = driver.current_url
        if "remanga.org" not in current_url:
            print(f"❌ Не удалось загрузить целевую страницу. Текущий URL: {current_url}")
            return pd.DataFrame()

        # Проверяем, не перенаправило ли на страницу входа
        if "login" in current_url or "signin" in current_url:
            print("❌ Перенаправлено на страницу входа. Куки устарели.")
            return pd.DataFrame()

        # Ждем загрузки виртуализированной таблицы
        print("Ожидаем загрузки таблицы...")
        try:
            wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div[data-sentry-component*='Donations'], div[class*='table'], table")))
            print("✅ Таблица найдена")
        except Exception as e:
            print(f"⚠️ Таблица не загрузилась: {e}")
            # Продолжаем в надежде, что данные все равно есть

        print("⏳ Ждем загрузку данных... (5 секунд)")
        time.sleep(5)

        print("Начинаем сбор данных с прокруткой...")

        for attempt in range(max_scroll_attempts):
            # Получаем HTML
            page_html = driver.page_source
            soup = BeautifulSoup(page_html, 'html.parser')

            # Ищем таблицу разными способами
            table_selectors = [
                'div[data-sentry-component="VirtualizedDataTable"]',
                'div[data-sentry-component="GuildDonationsList"]',
                'div[class*="table"]',
                'table'
            ]

            table_container = None
            for selector in table_selectors:
                table_container = soup.select_one(selector)
                if table_container:
                    print(f"✅ Найдена таблица с селектором: {selector}")
                    break

            if not table_container:
                print("❌ Таблица не найдена в HTML")
                # Сохраняем HTML для отладки
                with open('debug_page.html', 'w', encoding='utf-8') as f:
                    f.write(page_html)
                print("✅ Сохранен HTML для отладки: debug_page.html")
                break

            # Ищем строки таблицы
            rows = table_container.find_all('tr', style=re.compile(r'position:\s*absolute'))

            # Альтернативный поиск строк
            if not rows:
                rows = table_container.find_all('tr')
                # Фильтруем только видимые строки с данными
                rows = [row for row in rows if row.find('td')]

            print(f"Попытка {attempt + 1}: найдено {len(rows)} строк")

            # Обрабатываем строки
            new_rows_found = 0
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
                        new_rows_found += 1

                except Exception as e:
                    print(f"Ошибка обработки строки: {e}")
                    continue

            print(f"Собрано записей: {len(rows_data)} (новых: {new_rows_found})")

            # Проверяем прогресс
            if len(rows_data) == previous_count:
                no_new_count += 1
                if no_new_count >= 3:
                    print("Новых данных нет, завершаем...")
                    break
            else:
                no_new_count = 0
                previous_count = len(rows_data)

            # Прокрутка вниз
            try:
                # Пробуем разные элементы для прокрутки
                scroll_selectors = [
                    "div[data-sentry-component='GuildDonationsList']",
                    "div[data-sentry-component='VirtualizedDataTable']",
                    ".table-container",
                    "div[class*='virtual']",
                    "body"
                ]

                for selector in scroll_selectors:
                    try:
                        element = driver.find_element(By.CSS_SELECTOR, selector)
                        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", element)
                        print(f"✅ Прокручен элемент: {selector}")
                        break
                    except:
                        continue
                else:
                    # Если не нашли специфичный элемент, прокручиваем страницу
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    print("✅ Прокручена вся страница")

                time.sleep(2)

            except Exception as e:
                print(f"Ошибка прокрутки: {e}")

        print(f"\n=== ЗАВЕРШЕНО ===")
        print(f"Всего собрано записей: {len(rows_data)}")

        # Создаем DataFrame
        if rows_data:
            df = pd.DataFrame(rows_data, columns=['Пользователь', 'Сумма', 'Дата'])
            df = df.drop_duplicates()

            print("🔄 Преобразуем суммы в числовой формат...")
            df['Сумма'] = df['Сумма'].apply(convert_amount_to_int)

            print("📊 Статистика собранных бустов:")
            print(f"  - Всего собрано бустов: {len(df)}")
            print(f"  - Уникальных бустеров: {df['Пользователь'].nunique()}")
            print(f"  - Общая сумма бустов: {df['Сумма'].sum():,} ⚡")

            # Сохраняем результат в CSV для отладки
            df.to_csv('donations_result.csv', index=False, encoding='utf-8')
            print("✅ Результат сохранен в donations_result.csv")

            return df
        else:
            print("❌ Не удалось собрать данные бустов")
            return pd.DataFrame()

    except Exception as e:
        print(f"❌ Произошла ошибка: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
    finally:
        print("Закрываем браузер...")
        try:
            driver.quit()
        except:
            pass


# Добавляем точку входа для тестирования
if __name__ == "__main__":
    print("🔧 Тестируем парсер...")

    # Сначала проверяем подключение
    if check_browserless_connection():
        result = parse_table()
        if not result.empty:
            print("✅ Парсер работает успешно!")
            print(result.head())
        else:
            print("❌ Парсер не смог собрать данные")
    else:
        print("❌ Не удалось подключиться к Browserless")