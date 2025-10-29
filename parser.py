from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import time
import json
import pandas as pd
import re
import os
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()


def setup_driver():
    """Настраивает Selenium драйвер для standalone Chrome на Railway"""
    chrome_options = Options()

    # Аргументы для Railway
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--ignore-ssl-errors")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    # Для Railway и подобных облачных сред
    chrome_options.add_argument("--remote-debugging-port=9222")
    chrome_options.add_argument("--disable-setuid-sandbox")

    # Дополнительные опции для стабильности
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    # Прокси для доступа к РФ сайтам (опционально)
    proxy_url = os.getenv('RUSSIAN_PROXY_URL')
    if proxy_url:
        chrome_options.add_argument(f'--proxy-server={proxy_url}')
        print(f"🔗 Используем прокси для РФ: {proxy_url}")

    try:
        # Получаем URL standalone Chrome сервиса из переменных окружения
        chrome_service_url = os.getenv('STANDALONE_CHROME_URL', 'http://standalone-chrome.railway.internal:4444')

        print(f"🔗 Подключаемся к standalone Chrome: {chrome_service_url}")

        driver = webdriver.Remote(
            command_executor=chrome_service_url,
            options=chrome_options
        )

        # Устанавливаем таймауты
        driver.set_page_load_timeout(30)
        driver.implicitly_wait(10)

        # Добавляем скрипт для маскировки веб-драйвера
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        print("✅ Успешно подключились к standalone Chrome")
        return driver

    except Exception as e:
        print(f"❌ Ошибка подключения к standalone Chrome: {e}")

        # Fallback: попробуем локальный Chrome
        try:
            print("🔄 Пробуем локальный Chrome как fallback...")
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager

            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            print("✅ Успешно запущен локальный Chrome")
            return driver
        except Exception as e2:
            print(f"❌ Локальный Chrome также не сработал: {e2}")
            return None


def check_browserless_connection():
    """Проверяет подключение через standalone Chrome"""
    print("🔍 Проверяем подключение через standalone Chrome...")

    try:
        driver = setup_driver()
        if driver:
            # Проверяем, что браузер работает
            driver.get("https://www.google.com")
            title = driver.title
            driver.quit()
            print(f"✅ Standalone Chrome доступен, заголовок: {title}")
            return True
        else:
            print("❌ Standalone Chrome недоступен")
            return False
    except Exception as e:
        print(f"❌ Ошибка проверки standalone Chrome: {e}")
        return False


def login_to_remanga(driver):
    """Выполняет вход на remanga.org с использованием логина и пароля"""
    try:
        username = os.getenv('REMANGALOGIN_USERNAME')
        password = os.getenv('REMANGALOGIN_PASSWORD')

        if not username or not password:
            print("❌ Логин или пароль не настроены")
            return False

        print("🔐 Выполняем вход на remanga.org...")

        # Переходим на главную страницу
        main_url = "https://remanga.org"
        driver.get(main_url)

        # Ждем полной загрузки DOM
        WebDriverWait(driver, 15).until(
            lambda driver: driver.execute_script("return document.readyState") == "complete"
        )
        print("✅ DOM полностью загружен")
        time.sleep(2)

        # ОЖИДАЕМ появления кнопки 'Вход/Регистрация'
        print("🔍 Ожидаем появления кнопки 'Вход/Регистрация'...")

        try:
            # Ждем появления и кликабельности кнопки по селектору
            login_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-sentry-component='UserAuthButtonMenuItem']"))
            )
            print("✅ Кнопка найдена и кликабельна")
        except Exception as e:
            print(f"❌ Кнопка не появилась за 10 секунд: {e}")
            print("🔄 Пробуем альтернативный поиск по тексту...")
            try:
                login_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Вход/Регистрация')]"))
                )
                print("✅ Нашли кнопку по тексту")
            except:
                print("❌ Кнопка не найдена даже по тексту")
                return False

        # КЛИКАЕМ по кнопке
        print("🖱️ Кликаем по кнопке...")
        try:
            driver.execute_script("arguments[0].click();", login_button)
            print("✅ Успешно кликнули через JavaScript")
        except Exception as e:
            print(f"❌ Ошибка клика: {e}")
            return False

        # Ждем открытия модального окна входа
        print("⏳ Ожидаем открытия модального окна входа...")
        time.sleep(3)

        # ОЖИДАЕМ появления полей формы в модальном окне
        print("🔍 Ожидаем появления полей формы...")

        try:
            # Ищем поле логина/email - пробуем разные селекторы
            username_selectors = [
                "input[name='login']",
                "input[name='username']",
                "input[name='email']",
                "input[type='text']",
                "input[placeholder*='логин' i]",
                "input[placeholder*='email' i]"
            ]

            username_field = None
            for selector in username_selectors:
                try:
                    username_field = WebDriverWait(driver, 3).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    print(f"✅ Поле логина найдено с селектором: {selector}")
                    break
                except:
                    continue

            if not username_field:
                print("❌ Поле логина не найдено")
                return False

        except Exception as e:
            print(f"❌ Ошибка поиска поля логина: {e}")
            return False

        try:
            # Ищем поле пароля - пробуем разные селекторы
            password_selectors = [
                "input[name='password']",
                "input[type='password']",
                "input[placeholder*='пароль' i]"
            ]

            password_field = None
            for selector in password_selectors:
                try:
                    password_field = WebDriverWait(driver, 3).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    print(f"✅ Поле пароля найдено с селектором: {selector}")
                    break
                except:
                    continue

            if not password_field:
                print("❌ Поле пароля не найдено")
                return False

        except Exception as e:
            print(f"❌ Ошибка поиска поля пароля: {e}")
            return False

        # Заполняем поля формы
        print("⌨️ Заполняем форму входа...")
        username_field.clear()
        username_field.send_keys(username)
        print("✅ Ввели логин")
        time.sleep(1)

        password_field.clear()
        password_field.send_keys(password)
        print("✅ Ввели пароль")
        time.sleep(1)

        # Ищем и кликаем кнопку входа
        print("🔍 Ищем кнопку входа...")
        submit_selectors = [
            "button[type='submit']",
            "button[class*='login']",
            "button[class*='submit']",
            "//button[contains(text(), 'Войти')]",
            "//button[contains(text(), 'Вход')]",
            "//button[contains(text(), 'Login')]"
        ]

        submit_button = None
        for selector in submit_selectors:
            try:
                if selector.startswith("//"):
                    submit_button = WebDriverWait(driver, 2).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                else:
                    submit_button = WebDriverWait(driver, 2).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                print(f"✅ Кнопка входа найдена с селектором: {selector}")
                break
            except:
                continue

        if submit_button:
            try:
                driver.execute_script("arguments[0].click();", submit_button)
                print("✅ Кликнули кнопку входа")
            except Exception as e:
                print(f"❌ Ошибка клика по кнопке: {e}")
                # Пробуем отправить форму через Enter
                password_field.send_keys(Keys.RETURN)
                print("⌨️ Отправляем форму Enter...")
        else:
            print("⌨️ Кнопка не найдена, отправляем форму Enter...")
            password_field.send_keys(Keys.RETURN)

        # Ждем завершения входа
        print("⏳ Ожидаем завершения входа...")
        time.sleep(5)

        # Проверка успешности входа несколькими способами
        current_url = driver.current_url
        print(f"📄 Текущий URL: {current_url}")

        # Проверяем, что мы не на странице входа
        if "signin" not in current_url and "login" not in current_url:
            print("✅ Успешно вошли в систему (проверка по URL)")
            return True
        else:
            # Дополнительная проверка по изменению интерфейса
            try:
                # После входа кнопка должна измениться на что-то другое
                WebDriverWait(driver, 5).until_not(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "button[data-sentry-component='UserAuthButtonMenuItem']"))
                )
                print("✅ Успешно вошли в систему (кнопка изменилась)")
                return True
            except:
                print("❌ Не удалось войти в систему")
                return False

    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False

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

        # Альтернативный поиск по классам
        for class_name in ['font-medium', 'username', 'user-name']:
            name_element = user_cell.find(class_=class_name)
            if name_element:
                return clean_text(name_element.get_text())

        # Поиск по любому тексту
        name_elements = user_cell.find_all(text=True)
        for text in name_elements:
            cleaned = clean_text(text)
            if cleaned and cleaned not in ['', 'Пользователь', 'User', 'Неизвестный']:
                return cleaned

        return "Неизвестный"
    except Exception as e:
        print(f"Ошибка извлечения пользователя: {e}")
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

        # Альтернативный поиск
        amount_elements = amount_cell.find_all(text=True)
        for text in amount_elements:
            cleaned = clean_text(text)
            if cleaned and re.search(r'\d', cleaned):
                return cleaned

        return "0"
    except Exception as e:
        print(f"Ошибка извлечения суммы: {e}")
        return "0"


def extract_date_data(date_cell):
    """Извлекает дату из ячейки"""
    try:
        # Ищем span с датой
        date_span = date_cell.find('span', class_='text-muted-foreground')
        if date_span:
            return clean_text(date_span.get_text())

        # Альтернативный поиск
        date_elements = date_cell.find_all(text=True)
        for text in date_elements:
            cleaned = clean_text(text)
            if cleaned and re.search(r'\d', cleaned):
                return cleaned

        return "Неизвестная дата"
    except Exception as e:
        print(f"Ошибка извлечения даты: {e}")
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

            print(f"✅ Название гильдии: '{guild_name}'")
            return guild_name

        return "Неизвестная гильдия"
    except Exception as e:
        print(f"❌ Ошибка извлечения названия гильдии: {e}")
        return "Неизвестная гильдия"


def parse_table(url='https://remanga.org/guild/i-g-g-d-r-a-s-i-l--a1172e3f/settings/donations'):
    """
    Парсит виртуализированную таблицу бустов через Selenium
    """
    print(f"🎯 Начинаем парсинг URL: {url}")

    # Настройка браузера через Selenium
    driver = setup_driver()
    if not driver:
        print("❌ Не удалось подключиться к Selenium")
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

        # Выполняем вход в систему
        print("🔐 Выполняем вход на remanga.org...")
        login_success = login_to_remanga(driver)

        if not login_success:
            print("❌ Не удалось войти в систему, пробуем продолжить без авторизации...")

        # Открываем целевую страницу
        print(f"📄 Открываем страницу гильдии '{guild_name}'...")
        driver.get(url)
        time.sleep(5)

        # Проверяем, загрузилась ли страница
        current_url = driver.current_url
        print(f"📄 Текущий URL: {current_url}")

        if "remanga.org" not in current_url:
            print(f"❌ Не удалось загрузить целевую страницу. Текущий URL: {current_url}")
            return pd.DataFrame()

        # Проверяем, не перенаправило ли на страницу входа
        if "signin" in current_url or "login" in current_url:
            print("❌ Перенаправлено на страницу входа. Авторизация не удалась.")
            return pd.DataFrame()

        # Проверяем доступ к странице
        page_text = driver.page_source.lower()
        if "доступ запрещен" in page_text or "access denied" in page_text or "недостаточно прав" in page_text:
            print("❌ Недостаточно прав для доступа к странице")
            return pd.DataFrame()

        # Ждем загрузки таблицы
        print("⏳ Ожидаем загрузки таблицы...")
        try:
            wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div[data-sentry-component*='Donations'], div[class*='table'], table")))
            print("✅ Таблица найдена")
        except Exception as e:
            print(f"⚠️ Таблица не загрузилась как ожидалось: {e}")
            # Продолжаем в надежде, что данные все равно есть

        print("⏳ Ждем загрузку данных...")
        time.sleep(3)

        print("🔄 Начинаем сбор данных с прокруткой...")

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

            print(f"📊 Попытка {attempt + 1}: найдено {len(rows)} строк")

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
                    print(f"⚠️ Ошибка обработки строки: {e}")
                    continue

            print(f"📈 Собрано записей: {len(rows_data)} (новых: {new_rows_found})")

            # Проверяем прогресс
            if len(rows_data) == previous_count:
                no_new_count += 1
                if no_new_count >= 3:
                    print("🛑 Новых данных нет, завершаем...")
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
                        print(f"⬇️  Прокручен элемент: {selector}")
                        break
                    except:
                        continue
                else:
                    # Если не нашли специфичный элемент, прокручиваем страницу
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    print("⬇️  Прокручена вся страница")

                time.sleep(2)

            except Exception as e:
                print(f"⚠️ Ошибка прокрутки: {e}")

        print(f"\n🎉 ПАРСИНГ ЗАВЕРШЕН")
        print(f"📋 Всего собрано записей: {len(rows_data)}")

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
            print("💾 Результат сохранен в donations_result.csv")

            return df
        else:
            print("❌ Не удалось собрать данные бустов")
            return pd.DataFrame()

    except Exception as e:
        print(f"❌ Критическая ошибка при парсинге: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
    finally:
        print("🔚 Закрываем браузер...")
        try:
            driver.quit()
        except:
            pass


def parse_table_for_service(url):
    """Функция для сервиса парсинга"""
    return parse_table(url)


# Точка входа для тестирования
if __name__ == "__main__":
    print("🔧 Запуск тестирования Selenium парсера...")

    # Сначала проверяем подключение
    if check_browserless_connection():
        print("\n🚀 Запускаем парсинг...")
        result = parse_table()
        if not result.empty:
            print("\n✅ Парсер успешно завершил работу!")
            print("📄 Первые 5 записей:")
            print(result.head())
        else:
            print("\n❌ Парсер не смог собрать данные")
    else:
        print("❌ Не удалось подключиться к Selenium")