import schedule
import time
from database import db_manager
from parser import parse_table_for_service

print("🔧 Инициализация сервиса парсинга...")

# Загружаем гильдии из базы данных при запуске
def load_guilds_for_service():
    """Загружает гильдии из БД для сервиса парсинга"""
    print("📥 Загружаем гильдии из базы данных...")

    # Инициализируем таблицы БД
    db_manager.setup_database()
    db_manager.setup_guilds_table()

    guilds = db_manager.load_all_guilds()
    print(f"📊 Загружено {len(guilds)} гильдий для парсинга")

    if guilds:
        for i, (name, url) in enumerate(guilds.items(), 1):
            print(f"  {i}. {name}")
    else:
        print("❌ В базе данных нет гильдий для парсинга")
        print("💡 Добавьте гильдии через бота командой /start")

    return guilds

def scheduled_parsing():
    print(f"\n🔄 Начало планового парсинга в {time.strftime('%H:%M:%S')}")

    if not GUILD_URLS:
        print("❌ Нет гильдий для парсинга. Добавьте гильдии через бота.")
        return

    print(f"📊 Начинаем парсинг {len(GUILD_URLS)} гильдий...")

    for guild_name, url in GUILD_URLS.items():
        try:
            print(f"🎯 Парсим гильдию: {guild_name}")
            print(f"🔗 URL: {url}")

            # Сохраняем гильдию перед парсингом (на всякий случай)
            db_manager.save_guild(guild_name, url)

            # Запускаем парсинг
            df = parse_table_for_service(url)

            if not df.empty:
                success = db_manager.save_donations(df, guild_name)
                if success:
                    print(f"✅ {guild_name}: успешно сохранено {len(df)} записей")
                else:
                    print(f"❌ {guild_name}: ошибка сохранения в БД")
            else:
                print(f"⚠️ {guild_name}: не удалось получить данные (пустой DataFrame)")

        except Exception as e:
            print(f"🚨 Критическая ошибка в гильдии {guild_name}: {e}")
            import traceback
            traceback.print_exc()

    print(f"✅ Плановый парсинг завершен в {time.strftime('%H:%M:%S')}")

# Загружаем гильдии только при запуске скрипта напрямую
if __name__ == '__main__':
    GUILD_URLS = load_guilds_for_service()

    # Настраиваем расписание
    print("⏰ Настраиваем расписание...")
    schedule.every(2).minutes.do(scheduled_parsing)

    print(f"\n🚀 Сервис парсинга запущен!")
    print(f"📊 Мониторим {len(GUILD_URLS)} гильдий")
    print(f"⏰ Парсинг каждые 2 минуты")
    print(f"⏳ Ожидаем первого запуска...")

    # Бесконечный цикл
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n⏹️ Сервис парсинга остановлен")