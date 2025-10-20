import re
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from database import db_manager
import pandas as pd
from parser import parse_table
import asyncio

GUILD_URLS = {}


def create_choice_keyboard():
    """Создает клавиатуру для выбора показа таблицы"""
    keyboard = [
        [InlineKeyboardButton("✅ Да, показать всю таблицу", callback_data="show_full")],
        [InlineKeyboardButton("❌ Нет, хватит", callback_data="show_partial")]
    ]
    return InlineKeyboardMarkup(keyboard)


def create_show_more_keyboard(guild_name: str):
    """Создает клавиатуру с кнопками показа всех данных"""
    keyboard = [
        [InlineKeyboardButton("📋 Показать всех бустеров", callback_data=f"show_all_{guild_name}")],
        [InlineKeyboardButton("📜 Показать историю бустов", callback_data="show_full")],
        [InlineKeyboardButton("🔄 Обновить данные", callback_data=f"refresh_{guild_name}")],
        [InlineKeyboardButton("❌ Закрыть", callback_data="close_table")]
    ]
    return InlineKeyboardMarkup(keyboard)


def create_simple_keyboard():
    """Создает простую клавиатуру только с кнопкой закрытия"""
    keyboard = [
        [InlineKeyboardButton("❌ Закрыть", callback_data="close_table")]
    ]
    return InlineKeyboardMarkup(keyboard)


def split_long_message(message: str, max_length: int = 4000):
    """Разбивает длинное сообщение на части, сохраняя HTML-разметку"""
    if len(message) <= max_length:
        return [message]

    parts = []
    current_part = ""
    open_tags = []

    i = 0
    while i < len(message):
        if len(current_part) >= max_length - 100:
            for tag in reversed(open_tags):
                current_part += f"</{tag}>"

            parts.append(current_part)
            current_part = ""
            for tag in open_tags:
                current_part += f"<{tag}>"

        if message[i] == '<' and i + 1 < len(message) and message[i + 1] != '/':
            tag_end = message.find('>', i)
            if tag_end != -1:
                tag_content = message[i:tag_end + 1]
                tag_name_match = re.match(r'<(\w+)', tag_content)
                if tag_name_match:
                    tag_name = tag_name_match.group(1)
                    open_tags.append(tag_name)
                current_part += tag_content
                i = tag_end + 1
                continue

        elif message[i] == '<' and i + 1 < len(message) and message[i + 1] == '/':
            tag_end = message.find('>', i)
            if tag_end != -1:
                tag_content = message[i:tag_end + 1]
                tag_name_match = re.match(r'</(\w+)', tag_content)
                if tag_name_match and open_tags:
                    tag_name = tag_name_match.group(1)
                    if tag_name in open_tags:
                        open_tags.remove(tag_name)
                current_part += tag_content
                i = tag_end + 1
                continue

        current_part += message[i]
        i += 1

    if current_part:
        for tag in reversed(open_tags):
            current_part += f"</{tag}>"
        parts.append(current_part)

    return parts


def format_stats_from_db(db_stats):
    """Форматирует статистику по данным из БД с HTML разметкой"""
    if not db_stats:
        return "❌ Нет данных для статистики"

    stats = (
        f"<b>📊 Статистика из БД:</b>\n"
        f"• Всего бустов: <code>{db_stats['total_transactions']}</code>\n"
        f"• Уникальных бустеров: <code>{db_stats['unique_users']}</code>\n"
        f"• Общая сумма: <code>{db_stats['total_amount']:,} ⚡</code>\n"
        f"• Последняя дата обновления: <code>{db_stats['last_update']}</code>"
    )
    return stats


def format_full_table(df, start_idx: int = 0, chunk_size: int = None):
    """Форматирует часть таблицы с HTML разметкой"""
    if df.empty:
        return "❌ Нет данных для отображения"

    if chunk_size:
        end_idx = min(start_idx + chunk_size, len(df))
        df_chunk = df.iloc[start_idx:end_idx]
        title = f"<b>📊 Таблица бустов (часть {start_idx // chunk_size + 1})</b>\n"
        title += f"<b>Записи {start_idx + 1}-{end_idx} из {len(df)}</b>\n\n"
    else:
        df_chunk = df
        title = f"<b>📊 Полная таблица бустов</b>\n"
        title += f"<b>Всего записей: {len(df)}</b>\n\n"

    table = title
    table += "<pre>"
    table += f"{'№':<3} {'Бустер':<25} {'Сумма':<12} {'Дата':<18}\n"
    table += "-" * 65 + "\n"

    for i, (_, row) in enumerate(df_chunk.iterrows(), start_idx + 1):
        user = str(row['user_name'])[:24] if pd.notna(row['user_name']) else "Неизвестный"
        amount = str(row['sum'])[:11] if pd.notna(row['sum']) else "0"
        date = str(row['date_buster'])[:17] if pd.notna(row['date_buster']) else "Неизвестная дата"

        table += f"{i:<3} {user:<25} {amount:<12} {date:<18}\n"

    table += "</pre>"
    return table


def format_top_donators_from_db(db_data, top_n=20, show_all=False):
    """Форматирует топ бустеров в виде таблицы"""
    if not db_data:
        return "❌ Нет данных о бустерах"

    if show_all:
        top_text = f"<b>🏆 Все бустеры ({len(db_data)})</b>\n\n"
    else:
        top_text = f"<b>🏆 Топ-{min(top_n, len(db_data))} бустеров</b>\n\n"

    top_text += "<pre>"
    top_text += f"{'№':<3} {'Бустер':<20} {'Сумма':<12} {'Бустов':<8}\n"
    top_text += "─" * 45 + "\n"

    display_data = db_data if show_all else db_data[:top_n]

    for i, (user_name, total_donated, donation_count) in enumerate(display_data, 1):
        user_display = user_name[:19]
        total_formatted = f"{total_donated:,} ⚡".replace(",", " ")

        top_text += f"{i:<3} {user_display:<20} {total_formatted:<12} {donation_count:<8}\n"

    top_text += "</pre>"

    if not show_all and len(db_data) > top_n:
        top_text += f"\n<i>... и еще {len(db_data) - top_n} бустеров</i>"

    return top_text


async def send_all_donators(update: Update, context: ContextTypes.DEFAULT_TYPE, guild_name: str):
    """Отправляет полный список всех бустеров"""
    query = update.callback_query
    await query.answer()

    donations_data = db_manager.get_all_donations_grouped(guild_name, limit=1000)

    if not donations_data:
        await query.message.reply_text("❌ Нет данных о бустерах")
        return

    all_donators_text = format_top_donators_from_db(donations_data, show_all=True)
    messages = split_long_message(all_donators_text, 3500)

    for i, message_part in enumerate(messages):
        if i == 0:
            keyboard = create_simple_keyboard()
            await query.message.reply_text(message_part, parse_mode='HTML', reply_markup=keyboard)
        else:
            await query.message.reply_text(message_part, parse_mode='HTML')

    await query.message.delete()


async def send_data_from_db(update: Update, context: ContextTypes.DEFAULT_TYPE, guild_name: str):
    """Отправляет данные из БД"""
    try:
        await update.message.reply_text(f"📊 Загружаю данные по гильдии {guild_name} из БД...")

        donations_data = db_manager.get_all_donations_grouped(guild_name)
        db_stats = db_manager.get_detailed_stats(guild_name)

        if not donations_data:
            await update.message.reply_text("❌ В базе данных пока нет записей")
            return

        # Отправляем статистику из БД (отдельным сообщением)
        stats_text = format_stats_from_db(db_stats)
        await update.message.reply_text(stats_text, parse_mode='HTML')

        # Создаем ОДНО сообщение с топом бустеров и кнопками
        top_text = format_top_donators_from_db(donations_data, 20, show_all=False)
        # Убираем строку с "... и еще X бустеров"
        top_text = top_text.split('\n<i>... и еще')[0]

        # Добавляем информацию о дате и кнопки в то же сообщение
        full_message = (
            f"{top_text}\n\n"
            f"📅 Данные актуальны на: {db_stats['last_update']}\n"
            "Выберите действие:"
        )

        show_more_keyboard = create_show_more_keyboard(guild_name)

        # Отправляем ВСЕ в одном сообщении
        await update.message.reply_text(
            full_message,
            parse_mode='HTML',
            reply_markup=show_more_keyboard
        )

        # Сохраняем данные в контексте
        context.user_data['guild_name'] = guild_name

    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка загрузки данных: {e}")


def format_top_donators_without_footer(db_data, top_n=20):
    """Форматирует топ бустеров БЕЗ текста '... и еще X бустеров'"""
    if not db_data:
        return "❌ Нет данных о бустерах"

    top_text = f"<b>🏆 Топ-{min(top_n, len(db_data))} бустеров</b>\n\n"

    top_text += "<pre>"
    top_text += f"{'№':<3} {'Бустер':<20} {'Сумма':<12} {'Бустов':<8}\n"
    top_text += "─" * 45 + "\n"

    display_data = db_data[:top_n]

    for i, (user_name, total_donated, donation_count) in enumerate(display_data, 1):
        user_display = user_name[:19]
        total_formatted = f"{total_donated:,} ⚡".replace(",", " ")

        top_text += f"{i:<3} {user_display:<20} {total_formatted:<12} {donation_count:<8}\n"

    top_text += "</pre>"

    # УБИРАЕМ строку с "... и еще X бустеров"
    return top_text


async def send_complete_data(update: Update, context: ContextTypes.DEFAULT_TYPE, df, web_page_url: str = None,
                             guild_name: str = None):
    """Отправляет все данные после парсинга"""
    if df.empty:
        await update.message.reply_text("❌ Не удалось получить данные таблицы")
        return

    # Получаем статистику по новым бустам
    new_stats = db_manager.get_new_donations_stats(df, guild_name)

    if new_stats and new_stats['new_donations_count'] > 0:
        new_stats_text = (
            f"<b>🆕 Новые бусты:</b>\n"
            f"• Новых бустов: <code>{new_stats['new_donations_count']}</code>\n"
            f"• Новых бустеров: <code>{new_stats['new_donations_users_count']}</code>\n"
            f"• Сумма новых бустов: <code>{new_stats['new_donations_amount']:,} ⚡</code>\n\n"
        )
        await update.message.reply_text(new_stats_text, parse_mode='HTML')
    else:
        await update.message.reply_text("ℹ️ Новых бустов не найдено", parse_mode='HTML')

    # Получаем обновленные данные из БД
    donations_data = db_manager.get_all_donations_grouped(guild_name)
    db_stats = db_manager.get_detailed_stats(guild_name)

    # Отправляем статистику
    stats_text = format_stats_from_db(db_stats)
    await update.message.reply_text(stats_text, parse_mode='HTML')

    # Отправляем топ бустеров из БД
    if donations_data:
        top_text = format_top_donators_from_db(donations_data, 20, show_all=False)
        await update.message.reply_text(top_text, parse_mode='HTML')
    else:
        await update.message.reply_text("❌ Не удалось получить данные бустеров")

    # ПРЕДЛАГАЕМ ПОКАЗАТЬ ВСЮ ТАБЛИЦУ
    choice_keyboard = create_choice_keyboard()

    await update.message.reply_text(
        "Хотите увидеть полную историю всех бустов?",
        reply_markup=choice_keyboard
    )

    # Сохраняем данные в контексте
    df_renamed = df.rename(columns={
        'user_name': 'Пользователь',
        'sum': 'Сумма',
        'date_buster': 'Дата'
    })

    context.user_data['guild_name'] = guild_name
    context.user_data['full_dataframe'] = df_renamed
    context.user_data['web_page_url'] = web_page_url


async def send_full_table(update: Update, context: ContextTypes.DEFAULT_TYPE, guild_name: str):
    """Отправляет полную историю бустов из БД"""
    query = update.callback_query
    await query.answer()

    all_donations = db_manager.get_all_donations(guild_name)

    if not all_donations:
        await query.message.reply_text("❌ В базе данных нет записей")
        return

    df = pd.DataFrame(all_donations)

    await query.message.reply_text("📊 Загружаю историю бустов из БД...")

    chunk_size = 25
    total_parts = (len(df) + chunk_size - 1) // chunk_size

    for part in range(total_parts):
        start_idx = part * chunk_size
        table_text = format_full_table(df, start_idx, chunk_size)

        if part == 0:
            keyboard = create_simple_keyboard()
            await query.message.reply_text(
                table_text,
                reply_markup=keyboard,
                parse_mode='HTML'
            )
        else:
            await query.message.reply_text(
                table_text,
                parse_mode='HTML'
            )

        await asyncio.sleep(0.5)


async def handle_table_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает выбор пользователя"""
    query = update.callback_query
    await query.answer()

    data = query.data
    guild_name = context.user_data.get('guild_name', 'Неизвестная гильдия')

    if data == "show_full":
        await send_full_table(update, context, guild_name)
    elif data == "show_partial":
        await query.message.delete()
        await query.message.reply_text("✅ Хорошо! Если понадобится полная таблица - просто запросите данные снова.")
    elif data == "close_table":
        await query.message.delete()


async def handle_show_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки 'Показать всех'"""
    query = update.callback_query
    await query.answer()

    data = query.data
    guild_name = data.replace('show_all_', '')

    await send_all_donators(update, context, guild_name)


async def show_guilds_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список доступных гильдий"""
    guilds_text = "📋 Доступные гильдии:\n\n"

    for i, (guild_name, url) in enumerate(GUILD_URLS.items(), 1):
        guilds_text += f"{i}. {guild_name}\n"

    guilds_text += f"\nВсего гильдий: {len(GUILD_URLS)}"

    await update.message.reply_text(guilds_text)


async def gettable(update: Update, context: ContextTypes.DEFAULT_TYPE, url: str, guild_name: str):
    """Функция для получения таблицы"""
    # Определяем откуда пришел запрос - из message или callback_query
    if hasattr(update, 'message') and update.message:
        message_func = update.message.reply_text
    elif hasattr(update, 'callback_query') and update.callback_query:
        message_func = update.callback_query.message.reply_text
    else:
        return  # Если ничего не нашли, выходим

    await message_func("⏳ Начинаю извлечение данных таблицы... это займет около 2 мин")

    try:

        # Используем функцию parse_table из parser.py
        df = await asyncio.to_thread(parse_table, url)

        if df.empty:
            await message_func("❌ Не удалось получить данные таблицы")
            return

        await message_func("✅ Таблица успешно получена!")

        web_page_url = url
        await send_complete_data(update, context, df, web_page_url, guild_name)

    except Exception as e:
        await message_func(f"❌ Произошла ошибка: {e}")