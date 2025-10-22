import mysql.connector
from mysql.connector import Error
import os
import logging
import re
from dotenv import load_dotenv
load_dotenv()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        self.config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'railway'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', ''),
            'charset': 'utf8mb4',
            'port': int(os.getenv('DB_PORT', 3306))
        }

    def connect(self):
        """Подключается к базе данных"""
        try:
            connection = mysql.connector.connect(**self.config)
            if connection.is_connected():
                logger.info("✅ Успешное подключение к MySQL")
                return connection
        except Error as e:
            logger.error(f"❌ Ошибка подключения к MySQL: {e}")
            return None

    def setup_database(self):
        """Настраивает базу данных и все необходимые таблицы"""
        logger.info("🔧 Настройка базы данных...")

        # Проверяем и создаем базу данных
        if not self.check_database_exists():
            if not self.create_database():
                return False

        # Создаем таблицу гильдий
        if not self.setup_guilds_table():
            return False

        logger.info("✅ База данных настроена")
        return True

    def ensure_guild_table_exists(self, guild_name: str):
        """Гарантирует, что таблица для гильдии существует (создает если нет)"""
        if not self.check_donation_table_exists(guild_name):
            return self.create_donation_table(guild_name)
        return True

    def setup_guilds_table(self):
        """Создает таблицу для гильдий"""
        connection = self.connect()
        if not connection:
            return False

        try:
            cursor = connection.cursor()
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS guilds (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL UNIQUE,
                url VARCHAR(500) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor.execute(create_table_sql)
            connection.commit()
            logger.info("✅ Таблица guilds создана/проверена")
            return True
        except Error as e:
            logger.error(f"❌ Ошибка при создании таблицы guilds: {e}")
            return False
        finally:
            if connection.is_connected():
                connection.close()

    def save_guild(self, guild_name: str, url: str):
        """Сохраняет гильдию в БД и создает для нее таблицу донатов"""
        connection = self.connect()
        if not connection:
            return False

        try:
            cursor = connection.cursor()
            sql = "INSERT INTO guilds (name, url) VALUES (%s, %s) ON DUPLICATE KEY UPDATE url = VALUES(url), is_active = TRUE"
            cursor.execute(sql, (guild_name, url))
            connection.commit()
            logger.info(f"✅ Гильдия '{guild_name}' сохранена в БД")

            # Создаем таблицу для донатов этой гильдии
            self.ensure_guild_table_exists(guild_name)
            return True
        except Error as e:
            logger.error(f"❌ Ошибка сохранения гильдии '{guild_name}': {e}")
            return False
        finally:
            if connection.is_connected():
                connection.close()

    def load_all_guilds(self):
        """Загружает все активные гильдии из БД"""
        connection = self.connect()
        if not connection:
            return {}

        try:
            cursor = connection.cursor()
            sql = "SELECT name, url FROM guilds WHERE is_active = TRUE ORDER BY name"
            cursor.execute(sql)
            guilds = {name: url for name, url in cursor.fetchall()}
            logger.info(f"✅ Загружено {len(guilds)} гильдий из БД")
            return guilds
        except Error as e:
            logger.error(f"❌ Ошибка загрузки гильдий из БД: {e}")
            return {}
        finally:
            if connection.is_connected():
                connection.close()

    def get_safe_table_name(self, guild_name: str) -> str:
        """Создает безопасное имя таблицы из названия гильдии"""
        try:
            # Нормализуем название - убираем ID и лишние пробелы
            normalized_name = guild_name.strip().lower()
            normalized_name = re.sub(r'\s+--[a-f0-9]{8}$', '', normalized_name)

            # Транслитерируем кириллицу в латиницу
            translit_map = {
                'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
                'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
                'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
                'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch', 'ъ': '',
                'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
            }

            # Транслитерируем
            latin_name = ''
            for char in normalized_name:
                if char in translit_map:
                    latin_name += translit_map[char]
                elif char.isalnum() or char == ' ':
                    latin_name += char
                else:
                    latin_name += '_'

            # Заменяем пробелы на подчеркивания и убираем лишнее
            safe_name = latin_name.replace(' ', '_')
            safe_name = re.sub(r'[^a-zA-Z0-9_]', '', safe_name)
            safe_name = re.sub(r'_+', '_', safe_name).strip('_')

            if not safe_name:
                safe_name = 'unknown_guild'

            if safe_name and safe_name[0].isdigit():
                safe_name = 'g_' + safe_name

            safe_name = 'donations_' + safe_name

            logger.info(f"🔧 Создано имя таблицы: '{guild_name}' -> '{safe_name}'")
            return safe_name

        except Exception as e:
            logger.error(f"Ошибка создания имени таблицы: {e}")
            return 'donations_unknown_guild'

    def check_database_exists(self):
        """Проверяет существование базы данных"""
        try:
            temp_config = self.config.copy()
            temp_config.pop('database', None)

            connection = mysql.connector.connect(**temp_config)
            cursor = connection.cursor()

            cursor.execute("SHOW DATABASES LIKE 'railway'")
            result = cursor.fetchone()
            exists = result is not None

            if exists:
                logger.info("✅ База данных railway существует")
            else:
                logger.info("❌ База данных railway не существует")

            connection.close()
            return exists

        except Error as e:
            logger.error(f"Ошибка при проверке базы данных: {e}")
            return False

    def create_database(self):
        """Создает базу данных"""
        try:
            temp_config = self.config.copy()
            temp_config.pop('database', None)

            connection = mysql.connector.connect(**temp_config)
            cursor = connection.cursor()

            cursor.execute("CREATE DATABASE IF NOT EXISTS railway")
            connection.commit()
            logger.info("✅ База данных railway создана")

            connection.close()
            return True

        except Error as e:
            logger.error(f"❌ Не удалось создать базу данных: {e}")
            return False

    def check_donation_table_exists(self, guild_name: str):
        """Проверяет существование таблицы донатов для гильдии"""
        connection = self.connect()
        if not connection:
            return False

        try:
            table_name = self.get_safe_table_name(guild_name)
            cursor = connection.cursor()
            cursor.execute("SHOW TABLES LIKE %s", (table_name,))
            result = cursor.fetchone()
            exists = result is not None

            if exists:
                logger.debug(f"✅ Таблица {table_name} существует")
            else:
                logger.info(f"📋 Таблица {table_name} не существует")

            return exists
        except Error as e:
            logger.error(f"Ошибка при проверке таблицы: {e}")
            return False
        finally:
            if connection.is_connected():
                connection.close()

    def create_donation_table(self, guild_name: str):
        """Создает таблицу донатов для конкретной гильдии"""
        connection = self.connect()
        if not connection:
            return False

        try:
            cursor = connection.cursor()

            # Создаем безопасное имя таблицы
            table_name = self.get_safe_table_name(guild_name)

            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
              `id` int NOT NULL AUTO_INCREMENT,
              `user_name` varchar(25) DEFAULT NULL,
              `sum` int DEFAULT NULL,
              `date_buster` date DEFAULT NULL,
              `last_updated` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              PRIMARY KEY (`id`),
              UNIQUE KEY `unique_buster_{table_name}` (`user_name`, `sum`, `date_buster`)
            );
            """
            cursor.execute(create_table_sql)
            connection.commit()
            logger.info(f"✅ Таблица донатов {table_name} создана для гильдии '{guild_name}'")
            return True

        except Error as e:
            logger.error(f"❌ Ошибка при создании таблицы {table_name}: {e}")
            return False
        finally:
            if connection.is_connected():
                connection.close()

    def get_detailed_stats(self, guild_name: str):
        """Получает детальную статистику для конкретной гильдии"""
        # Гарантируем, что таблица существует
        if not self.ensure_guild_table_exists(guild_name):
            return None

        connection = self.connect()
        if not connection:
            return None

        try:
            table_name = self.get_safe_table_name(guild_name)
            cursor = connection.cursor(dictionary=True)

            cursor.execute(f"""
                SELECT 
                    COUNT(*) as total_transactions,
                    COUNT(DISTINCT user_name) as unique_users,
                    SUM(sum) as total_amount,
                    MAX(last_updated) as last_update
                FROM `{table_name}`
            """)
            stats = cursor.fetchone()

            if stats and stats['last_update']:
                stats['last_update'] = stats['last_update'].strftime("%d.%m.%Y %H:%M")
            else:
                stats['last_update'] = "неизвестно"

            return stats

        except Error as e:
            logger.error(f"Ошибка при получении статистики для {guild_name}: {e}")
            return None
        finally:
            if connection.is_connected():
                connection.close()

    def save_donations(self, df, guild_name: str):
        """Сохраняет донаты в таблицу указанной гильдии (автоматически создает таблицу если нужно)"""
        logger.info(f"💾 Сохраняем {len(df)} записей в таблицу гильдии {guild_name}")

        if not self.setup_database():
            logger.error(f"❌ Не удалось настроить базу данных")
            return False

        # Гарантируем, что таблица для гильдии существует
        if not self.ensure_guild_table_exists(guild_name):
            logger.error(f"❌ Не удалось создать таблицу для гильдии {guild_name}")
            return False

        connection = self.connect()
        if not connection:
            return False

        try:
            table_name = self.get_safe_table_name(guild_name)
            cursor = connection.cursor()
            saved_count = 0
            skipped_count = 0
            error_count = 0

            # Получаем существующие донаты для проверки дубликатов
            existing_donations = self.get_existing_donations_set(guild_name)

            for _, row in df.iterrows():
                try:
                    user_name = str(row['Пользователь'])[:25]
                    amount = row['Сумма']
                    date = self.parse_date(row['Дата'])

                    # Создаем уникальный идентификатор доната
                    donation_id = f"{user_name}|{amount}|{date}"

                    # Проверяем, есть ли уже такой донат в БД
                    if donation_id in existing_donations:
                        skipped_count += 1
                        continue

                    # Сохраняем только новый донат
                    insert_sql = f"""
                    INSERT INTO `{table_name}` (user_name, sum, date_buster)
                    VALUES (%s, %s, %s)
                    """
                    cursor.execute(insert_sql, (user_name, amount, date))
                    saved_count += 1

                    # Добавляем в множество, чтобы избежать дубликатов в текущей сессии
                    existing_donations.add(donation_id)

                except Exception as e:
                    logger.error(f"❌ Ошибка сохранения {row['Пользователь']}: {e}")
                    error_count += 1

            connection.commit()
            logger.info(
                f"✅ В таблицу {guild_name} сохранено: {saved_count} новых, пропущено: {skipped_count} дубликатов, ошибок: {error_count}")

            return True

        except Error as e:
            logger.error(f"❌ Общая ошибка БД: {e}")
            connection.rollback()
            return False
        finally:
            if connection.is_connected():
                connection.close()

    def get_existing_donations_set(self, guild_name: str):
        """Получает множество существующих донатов для конкретной гильдии"""
        # Гарантируем, что таблица существует
        if not self.ensure_guild_table_exists(guild_name):
            return set()

        connection = self.connect()
        if not connection:
            return set()

        try:
            table_name = self.get_safe_table_name(guild_name)
            cursor = connection.cursor()
            cursor.execute(f"SELECT user_name, sum, date_buster FROM `{table_name}`")
            existing_records = cursor.fetchall()

            existing_donations = set()
            for user_name, amount, date in existing_records:
                donation_id = f"{user_name}|{amount}|{date}"
                existing_donations.add(donation_id)

            logger.info(f"📊 Загружено {len(existing_donations)} существующих донатов для гильдии {guild_name}")
            return existing_donations

        except Error as e:
            logger.error(f"Ошибка при получении существующих донатов: {e}")
            return set()
        finally:
            if connection.is_connected():
                connection.close()

    def get_new_donations_stats(self, df, guild_name: str):
        """Получает статистику по новым донатам для конкретной гильдии"""
        if df.empty:
            return None

        try:
            existing_donations = self.get_existing_donations_set(guild_name)

            new_donations_count = 0
            new_donations_amount = 0
            new_donations_users = set()

            for _, row in df.iterrows():
                user_name = str(row['Пользователь'])[:25]
                amount = row['Сумма']
                date = self.parse_date(row['Дата'])

                donation_id = f"{user_name}|{amount}|{date}"

                if donation_id not in existing_donations:
                    new_donations_count += 1
                    new_donations_amount += amount
                    new_donations_users.add(user_name)

            return {
                'new_donations_count': new_donations_count,
                'new_donations_amount': new_donations_amount,
                'new_donations_users_count': len(new_donations_users)
            }

        except Exception as e:
            logger.error(f"Ошибка при получении статистики новых донатов: {e}")
            return None

    def parse_date(self, date_str):
        """Преобразует дату в формат MySQL DATE"""
        try:
            months = {
                'янв.': '01', 'фев.': '02', 'мар.': '03', 'апр.': '04',
                'мая': '05', 'июн.': '06', 'июл.': '07', 'авг.': '08',
                'сен.': '09', 'окт.': '10', 'нояб.': '11', 'дек.': '12'
            }

            date_part = date_str.split(',')[0].strip()
            parts = date_part.split()

            if len(parts) == 3:
                day, month_ru, year = parts
                month = months.get(month_ru, '01')
                return f"{year}-{month}-{day.zfill(2)}"

            return "2025-01-01"
        except:
            return "2025-01-01"

    def get_all_donations(self, guild_name: str):
        """Получает все донаты из таблицы гильдии"""
        # Гарантируем, что таблица существует
        if not self.ensure_guild_table_exists(guild_name):
            return None

        connection = self.connect()
        if not connection:
            return None

        try:
            table_name = self.get_safe_table_name(guild_name)
            cursor = connection.cursor(dictionary=True)
            sql = f"SELECT user_name, sum, date_buster FROM `{table_name}` ORDER BY date_buster DESC, user_name ASC"
            cursor.execute(sql)
            donations = cursor.fetchall()
            logger.info(f"📊 Получено {len(donations)} записей из таблицы {guild_name}")
            return donations
        except Error as e:
            logger.error(f"Ошибка при получении данных из таблицы {guild_name}: {e}")
            return None
        finally:
            if connection.is_connected():
                connection.close()

    def delete_guild(self, guild_name: str):
        """Удаляет гильдию из БД и её таблицу донатов"""
        connection = self.connect()
        if not connection:
            return False

        try:
            cursor = connection.cursor()

            # 1. Удаляем запись из таблицы guilds
            delete_sql = "DELETE FROM guilds WHERE name = %s"
            cursor.execute(delete_sql, (guild_name,))

            # 2. Удаляем таблицу донатов этой гильдии
            table_name = self.get_safe_table_name(guild_name)
            drop_table_sql = f"DROP TABLE IF EXISTS `{table_name}`"
            cursor.execute(drop_table_sql)

            connection.commit()
            logger.info(f"✅ Гильдия '{guild_name}' и её таблица удалены из БД")
            return True

        except Error as e:
            logger.error(f"❌ Ошибка удаления гильдии '{guild_name}': {e}")
            return False
        finally:
            if connection.is_connected():
                connection.close()

    def get_all_donations_grouped(self, guild_name: str, limit=50):
        """Получает донаты из таблицы гильдии с группировкой по пользователям"""
        # Гарантируем, что таблица существует
        if not self.ensure_guild_table_exists(guild_name):
            return None

        connection = self.connect()
        if not connection:
            return None

        try:
            table_name = self.get_safe_table_name(guild_name)
            cursor = connection.cursor()
            sql = f"""
            SELECT 
                user_name,
                SUM(sum) as total_donated,
                COUNT(*) as donation_count
            FROM `{table_name}` 
            GROUP BY user_name 
            ORDER BY total_donated DESC
            LIMIT %s
            """
            cursor.execute(sql, (limit,))
            donations = cursor.fetchall()
            logger.info(f"📊 Получено {len(donations)} группированных записей из таблицы {guild_name}")
            return donations
        except Error as e:
            logger.error(f"Ошибка при получении группированных данных из таблицы {guild_name}: {e}")
            return None
        finally:
            if connection.is_connected():
                connection.close()

    def clean_duplicates(self, guild_name: str):
        """Очищает дубликаты из таблицы гильдии"""
        # Гарантируем, что таблица существует
        if not self.ensure_guild_table_exists(guild_name):
            return False

        connection = self.connect()
        try:
            table_name = self.get_safe_table_name(guild_name)
            cursor = connection.cursor()

            # Создаем временную таблицу без дубликатов
            cursor.execute(f"""
                CREATE TEMPORARY TABLE temp_{table_name} AS
                SELECT DISTINCT user_name, sum, date_buster 
                FROM `{table_name}`
            """)

            # Очищаем основную таблицу
            cursor.execute(f"DELETE FROM `{table_name}`")

            # Восстанавливаем данные без дубликатов
            cursor.execute(f"""
                INSERT INTO `{table_name}` (user_name, sum, date_buster)
                SELECT user_name, sum, date_buster FROM temp_{table_name}
            """)

            connection.commit()

            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            new_count = cursor.fetchone()[0]

            logger.info(f"✅ Таблица {guild_name} очищена от дубликатов. Осталось записей: {new_count}")
            return True

        except Error as e:
            logger.error(f"❌ Ошибка очистки таблицы {guild_name}: {e}")
            return False
        finally:
            if connection.is_connected():
                connection.close()

    def delete_guild(self, guild_name: str):
        """Удаляет гильдию из БД и её таблицу донатов"""
        connection = self.connect()
        if not connection:
            return False

        try:
            cursor = connection.cursor()

            # 1. Удаляем запись из таблицы guilds
            delete_sql = "DELETE FROM guilds WHERE name = %s"
            cursor.execute(delete_sql, (guild_name,))

            # 2. Удаляем таблицу донатов этой гильдии
            table_name = self.get_safe_table_name(guild_name)
            drop_table_sql = f"DROP TABLE IF EXISTS `{table_name}`"
            cursor.execute(drop_table_sql)

            connection.commit()
            logger.info(f"✅ Гильдия '{guild_name}' и её таблица удалены из БД")
            return True

        except Error as e:
            logger.error(f"❌ Ошибка удаления гильдии '{guild_name}': {e}")
            return False
        finally:
            if connection.is_connected():
                connection.close()

# Создаем экземпляр менеджера базы данных
db_manager = DatabaseManager()