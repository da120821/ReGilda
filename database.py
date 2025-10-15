import mysql.connector
from mysql.connector import Error
import pandas as pd
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        self.config = {
            'host': 'localhost',
            'database': 'TgBot',
            'user': 'root',
            'password': 'da120821',
            'charset': 'utf8mb4',
            'port': 3306
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
        """Настраивает базу данных и таблицу"""
        logger.info("🔧 Настройка базы данных...")

        # Проверяем и создаем базу данных
        if not self.check_database_exists():
            if not self.create_database():
                return False

        # Проверяем и создаем таблицу
        if not self.check_table_exists():
            if not self.create_table():
                return False

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
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
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
        """Сохраняет гильдию в БД"""
        connection = self.connect()
        if not connection:
            return False

        try:
            cursor = connection.cursor()
            sql = "INSERT INTO guilds (name, url) VALUES (%s, %s)"
            cursor.execute(sql, (guild_name, url))
            connection.commit()
            logger.info(f"✅ Гильдия '{guild_name}' сохранена в БД")
            return True
        except Error as e:
            logger.error(f"❌ Ошибка сохранения гильдии '{guild_name}': {e}")
            return False
        finally:
            if connection.is_connected():
                connection.close()

    def load_all_guilds(self):
        """Загружает все гильдии из БД"""
        connection = self.connect()
        if not connection:
            return {}

        try:
            cursor = connection.cursor()
            sql = "SELECT name, url FROM guilds ORDER BY name"
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

    def check_database_exists(self):
        """Проверяет существование базы данных"""
        try:
            temp_config = self.config.copy()
            temp_config.pop('database', None)

            connection = mysql.connector.connect(**temp_config)
            cursor = connection.cursor()

            cursor.execute("SHOW DATABASES LIKE 'TgBot'")
            result = cursor.fetchone()
            exists = result is not None

            if exists:
                logger.info("✅ База данных TgBot существует")
            else:
                logger.info("❌ База данных TgBot не существует")

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

            cursor.execute("CREATE DATABASE IF NOT EXISTS TgBot")
            connection.commit()
            logger.info("✅ База данных TgBot создана")

            connection.close()
            return True

        except Error as e:
            logger.error(f"❌ Не удалось создать базу данных: {e}")
            return False

    def check_table_exists(self):
        """Проверяет существование таблицы"""
        connection = self.connect()
        if not connection:
            return False

        try:
            cursor = connection.cursor()
            cursor.execute("SHOW TABLES LIKE 'iggdrasil'")
            result = cursor.fetchone()
            exists = result is not None

            if exists:
                logger.info("✅ Таблица iggdrasil существует")
            else:
                logger.info("❌ Таблица iggdrasil не существует")

            return exists
        except Error as e:
            logger.error(f"Ошибка при проверке таблицы: {e}")
            return False
        finally:
            if connection.is_connected():
                connection.close()

    def create_table(self):
        """Создает таблицу"""
        connection = self.connect()
        if not connection:
            return False

        try:
            cursor = connection.cursor()

            create_table_sql = """
            CREATE TABLE IF NOT EXISTS `iggdrasil` (
              `id` int NOT NULL AUTO_INCREMENT,
              `user_name` varchar(25) DEFAULT NULL,
              `sum` int DEFAULT NULL,
              `date_buster` date DEFAULT NULL,
              `last_updated` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              PRIMARY KEY (`id`),
              UNIQUE KEY `unique_buster` (`user_name`, `sum`, `date_buster`)
            ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
            """
            cursor.execute(create_table_sql)
            connection.commit()
            logger.info("✅ Таблица iggdrasil создана")
            return True

        except Error as e:
            logger.error(f"❌ Ошибка при создании таблицы: {e}")
            return False
        finally:
            if connection.is_connected():
                connection.close()

    def get_detailed_stats(self):
        """Получает детальную статистику из БД"""
        connection = self.connect()
        if not connection:
            return None

        try:
            cursor = connection.cursor(dictionary=True)

            cursor.execute("""
                SELECT 
                    COUNT(*) as total_transactions,
                    COUNT(DISTINCT user_name) as unique_users,
                    SUM(sum) as total_amount,
                    MAX(last_updated) as last_update
                FROM iggdrasil
            """)
            stats = cursor.fetchone()

            if stats and stats['last_update']:
                stats['last_update'] = stats['last_update'].strftime("%d.%m.%Y %H:%M")
            else:
                stats['last_update'] = "неизвестно"

            return stats

        except Error as e:
            logger.error(f"Ошибка при получении статистики: {e}")
            return None
        finally:
            if connection.is_connected():
                connection.close()

    def save_to_iggdrasil(self, df):
        """Сохраняет только новые бусты в БД (без дубликатов)"""
        logger.info(f"💾 Сохраняем {len(df)} записей в БД с проверкой дубликатов")

        if not self.setup_database():
            logger.error("❌ Не удалось настроить базу данных")
            return False

        connection = self.connect()
        if not connection:
            return False

        try:
            cursor = connection.cursor()
            saved_count = 0
            skipped_count = 0
            error_count = 0

            # Получаем существующие бусты для проверки дубликатов
            existing_busters = self.get_existing_busters_set()

            for _, row in df.iterrows():
                try:
                    user_name = str(row['Пользователь'])[:25]
                    amount = row['Сумма']
                    date = self.parse_date(row['Дата'])

                    # Создаем уникальный идентификатор буста
                    bust_id = f"{user_name}|{amount}|{date}"

                    # Проверяем, есть ли уже такой буст в БД
                    if bust_id in existing_busters:
                        skipped_count += 1
                        continue

                    # Сохраняем только новый буст
                    insert_sql = """
                    INSERT INTO iggdrasil (user_name, sum, date_buster)
                    VALUES (%s, %s, %s)
                    """
                    cursor.execute(insert_sql, (user_name, amount, date))
                    saved_count += 1

                    # Добавляем в множество, чтобы избежать дубликатов в текущей сессии
                    existing_busters.add(bust_id)

                except Exception as e:
                    logger.error(f"❌ Ошибка сохранения {row['Пользователь']}: {e}")
                    error_count += 1

            connection.commit()
            logger.info(
                f"✅ Сохранено новых бустов: {saved_count}, пропущено дубликатов: {skipped_count}, ошибок: {error_count}")

            return True

        except Error as e:
            logger.error(f"❌ Общая ошибка БД: {e}")
            connection.rollback()
            return False
        finally:
            if connection.is_connected():
                connection.close()

    def get_existing_busters_set(self):
        """Получает множество существующих бустов для проверки дубликатов"""
        connection = self.connect()
        if not connection:
            return set()

        try:
            cursor = connection.cursor()
            cursor.execute("SELECT user_name, sum, date_buster FROM iggdrasil")
            existing_records = cursor.fetchall()

            existing_busters = set()
            for user_name, amount, date in existing_records:
                bust_id = f"{user_name}|{amount}|{date}"
                existing_busters.add(bust_id)

            logger.info(f"📊 Загружено {len(existing_busters)} существующих бустов для проверки")
            return existing_busters

        except Error as e:
            logger.error(f"Ошибка при получении существующих бустов: {e}")
            return set()
        finally:
            if connection.is_connected():
                connection.close()

    def get_new_busters_stats(self, df):
        """Получает статистику по новым бустам (которые будут добавлены)"""
        if df.empty:
            return None

        try:
            existing_busters = self.get_existing_busters_set()

            new_busters_count = 0
            new_busters_amount = 0
            new_busters_users = set()

            for _, row in df.iterrows():
                user_name = str(row['Пользователь'])[:25]
                amount = row['Сумма']
                date = self.parse_date(row['Дата'])

                bust_id = f"{user_name}|{amount}|{date}"

                if bust_id not in existing_busters:
                    new_busters_count += 1
                    new_busters_amount += amount
                    new_busters_users.add(user_name)

            return {
                'new_busters_count': new_busters_count,
                'new_busters_amount': new_busters_amount,
                'new_busters_users_count': len(new_busters_users)
            }

        except Exception as e:
            logger.error(f"Ошибка при получении статистики новых бустов: {e}")
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

    def get_all_donations(self):
        """Получает все бусты из БД"""
        connection = self.connect()
        if not connection:
            return None

        try:
            cursor = connection.cursor(dictionary=True)
            sql = "SELECT user_name, sum, date_buster FROM iggdrasil ORDER BY date_buster DESC, user_name ASC"
            cursor.execute(sql)
            donations = cursor.fetchall()
            logger.info(f"📊 Получено {len(donations)} записей из БД")
            return donations
        except Error as e:
            logger.error(f"Ошибка при получении данных из БД: {e}")
            return None
        finally:
            if connection.is_connected():
                connection.close()

    def get_all_donations_grouped(self, limit=50):
        """Получает бусты из БД с группировкой по пользователям"""
        connection = self.connect()
        if not connection:
            return None

        try:
            cursor = connection.cursor()
            sql = """
            SELECT 
                user_name,
                SUM(sum) as total_donated,
                COUNT(*) as donation_count
            FROM iggdrasil 
            GROUP BY user_name 
            ORDER BY total_donated DESC
            LIMIT %s
            """
            cursor.execute(sql, (limit,))
            donations = cursor.fetchall()
            logger.info(f"📊 Получено {len(donations)} группированных записей из БД")
            return donations
        except Error as e:
            logger.error(f"Ошибка при получении группированных данных из БД: {e}")
            return None
        finally:
            if connection.is_connected():
                connection.close()

    def clean_duplicates(self):
        """Очищает дубликаты из БД"""
        connection = self.connect()
        try:
            cursor = connection.cursor()

            # Создаем временную таблицу без дубликатов
            cursor.execute("""
                CREATE TEMPORARY TABLE temp_iggdrasil AS
                SELECT DISTINCT user_name, sum, date_buster 
                FROM iggdrasil
            """)

            # Очищаем основную таблицу
            cursor.execute("DELETE FROM iggdrasil")

            # Восстанавливаем данные без дубликатов
            cursor.execute("""
                INSERT INTO iggdrasil (user_name, sum, date_buster)
                SELECT user_name, sum, date_buster FROM temp_iggdrasil
            """)

            connection.commit()

            cursor.execute("SELECT COUNT(*) FROM iggdrasil")
            new_count = cursor.fetchone()[0]

            logger.info(f"✅ База очищена от дубликатов. Осталось записей: {new_count}")
            return True

        except Error as e:
            logger.error(f"❌ Ошибка очистки: {e}")
            return False
        finally:
            if connection.is_connected():
                connection.close()


# Создаем экземпляр менеджера базы данных
db_manager = DatabaseManager()