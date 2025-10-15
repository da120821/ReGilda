# Это всего лишь таймер для запуска
# парсера чтобы если несколько человек запросили запрос
# (а его я блокаю(хз убрал я это в конечной версии) и оставляю только уполномоченным админам)
# бота нах не убило, а точнее бот переживет
# но вот пользователь умрет со старости
# когда его срочный запрос на обновление данных обработается( в порядке очереди ) которое в среднем около 3-5 мин



import schedule
import time
from database import db_manager
from parser import parse_table_for_service  # импортируем из старого парсера

GUILD_URLS = {
    "Филлиал": "https://remanga.org/guild/phillial/settings/donations",
    "Иггдрасиль": "https://remanga.org/guild/i-g-g-d-r-a-s-i-l--a1172e3f/settings/donations"
}


def scheduled_parsing():
    """Парсит все гильдии по расписанию"""
    print(f"🔄 Начало планового парсинга в {time.strftime('%H:%M:%S')}")

    for guild_name, url in GUILD_URLS.items():
        try:
            print(f"📊 Парсим {guild_name}...")
            df = parse_table_for_service(url)

            if not df.empty:
                db_manager.save_to_iggdrasil(df)
                print(f"✅ {guild_name}: {len(df)} записей")
            else:
                print(f"❌ {guild_name}: не удалось получить данные")

        except Exception as e:
            print(f"🚨 Ошибка в {guild_name}: {e}")


# Настраиваем расписание
schedule.every(10).minutes.do(scheduled_parsing)

print("🚀 Сервис парсинга запущен! Парсим каждые 10 минут...")

# Бесконечный цикл
while True:
    schedule.run_pending()
    time.sleep(60)  # проверяем каждую минуту