from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler
import asyncio
from parser import parse_table
from database import db_manager
from TableToBot import GUILD_URLS, send_data_from_db, send_complete_data, handle_table_choice, handle_show_all, gettable

# URL для веб-страниц гильдий
GUILD_URLS = {}

def load_guilds_from_db():
    """Загружает гильдии из БД при старте"""
    global GUILD_URLS
    GUILD_URLS = db_manager.load_all_guilds()
    print(f"📊 Загружено {len(GUILD_URLS)} гильдий из БД")

def create_guilds_keyboard():
    """Создает клавиатуру с кнопками гильдий"""
    buttons = []
    guild_names = list(GUILD_URLS.keys())

    for i in range(0, len(guild_names), 2):
        row = [KeyboardButton(guild_names[i])]
        if i + 1 < len(guild_names):
            row.append(KeyboardButton(guild_names[i + 1]))
        buttons.append(row)

    buttons.append([KeyboardButton("➕ Добавить гильдию")])
    buttons.append([KeyboardButton("⌫ Удалить гильдию")])

    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

async def add_new_guild(update: Update, context: ContextTypes.DEFAULT_TYPE, guild_name: str, url: str):
    """Добавляет новую гильдию в систему"""
    try:
        success = db_manager.save_guild(guild_name, url)
        if success:
            GUILD_URLS[guild_name] = url
            await update.message.reply_text(
                f"✅ Гильдия '{guild_name}' успешно добавлена!\n\n"
                f"📝 Название: {guild_name}\n"
                f"🔗 Ссылка: {url}"
            )
            return True
        else:
            await update.message.reply_text("❌ Ошибка при сохранении гильдии в БД")
            return False
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при добавлении гильдии: {e}")
        return False

async def show_guilds_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список доступных гильдий"""
    guilds_text = "📋 Доступные гильдии:\n\n"
    for i, (guild_name, url) in enumerate(GUILD_URLS.items(), 1):
        guilds_text += f"{i}. {guild_name}\n"
    guilds_text += f"\nВсего гильдий: {len(GUILD_URLS)}"
    await update.message.reply_text(guilds_text)

async def handle_add_guild(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки добавления гильдии"""
    await update.message.reply_text(
        "📝 Чтобы добавить новую гильдию, отправьте сообщение в формате:\n\n"
        "➤ <b>Название гильдии</b>\n"
        "➤ <b>Ссылка на донаты</b>\n\n"
        "Пример:\n"
        "<code>МояГильдия\n"
        "https://remanga.org/guild/moya-gildiya/settings/donations</code>\n\n"
        "Или просто отправьте ссылку, а название я придумаю сама! 😊",
        parse_mode='HTML'
    )
    context.user_data['awaiting_guild_data'] = True

async def handle_guild_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ввода данных гильдии"""
    if context.user_data.get('awaiting_guild_data'):
        user_input = update.message.text.strip()
        try:
            if '\n' in user_input:
                lines = user_input.split('\n')
                guild_name = lines[0].strip()
                url = lines[1].strip()
            else:
                url = user_input
                guild_name = url.split('/guild/')[-1].split('/')[0]
                guild_name = guild_name.replace('-', ' ').title()

            if not url.startswith('https://remanga.org/guild/') or not url.endswith('/settings/donations'):
                await update.message.reply_text(
                    "❌ Неверный формат ссылки!\n"
                    "Ссылка должна быть вида:\n"
                    "https://remanga.org/guild/НАЗВАНИЕ/settings/donations"
                )
                return

            success = await add_new_guild(update, context, guild_name, url)
            if success:
                markup = create_guilds_keyboard()
                await update.message.reply_text(
                    "🎉 Отлично! Теперь вы можете выбрать новую гильдию из списка:",
                    reply_markup=markup
                )
            context.user_data['awaiting_guild_data'] = False
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка: {e}\n\nПопробуйте еще раз!")
            context.user_data['awaiting_guild_data'] = False
    else:
        await handle_other_messages(update, context)

async def handle_guild_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ТОЛЬКО для кнопок гильдий"""
    text = update.message.text
    if text in GUILD_URLS:
        await send_data_from_db(update, context, text)
    elif text == "➕ Добавить гильдию":
        await handle_add_guild(update, context)
    elif text == "⌫ Удалить гильдию":
        await handle_delete_guild(update, context)
    else:
        await update.message.reply_text("Пожалуйста, используйте кнопки для выбора действия")

async def handle_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запускает парсинг по требованию"""
    query = update.callback_query
    await query.answer()
    data = query.data
    guild_name = data.replace('refresh_', '')
    url = GUILD_URLS[guild_name]
    await gettable(update, context, url, guild_name)

async def handle_pagination(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик пагинации для кнопки закрытия"""
    query = update.callback_query
    await query.answer()
    if query.data == "close_table":
        await query.message.delete()

async def handle_other_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик для всех остальных сообщений"""
    await update.message.reply_text("Извините, но я не хочу общаться на темы которые не заданны моим разработчиком.")
    await asyncio.sleep(0.3)
    await update.message.reply_text("Лучше используйте кнопки для выбора гильдии с которой будете работать")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    markup = create_guilds_keyboard()
    await asyncio.sleep(0.5)
    await update.message.reply_text("Привет! Рада вас видеть!")
    await asyncio.sleep(1.2)
    await update.message.reply_text(
        "Выберите гильдию с которой хотите начать работать.",
        reply_markup=markup
    )

async def handle_delete_guild(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки удаления гильдии"""
    if not GUILD_URLS:
        await update.message.reply_text("❌ В базе нет гильдий для удаления")
        return

    keyboard = []
    for guild_name in GUILD_URLS.keys():
        keyboard.append([InlineKeyboardButton(f"⌫ {guild_name}", callback_data=f"delete_{guild_name}")])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_delete")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "⌫ Выберите гильдию для удаления:",
        reply_markup=reply_markup
    )

async def handle_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback'ов для удаления гильдий"""
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "cancel_delete":
        await query.message.delete()
        await query.message.reply_text("❌ Удаление отменено")
        return

    if data.startswith("delete_"):
        guild_name = data.replace("delete_", "")
        if guild_name not in GUILD_URLS:
            await query.message.reply_text(f"❌ Гильдия '{guild_name}' не найдена")
            return

        confirm_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_{guild_name}")],
            [InlineKeyboardButton("❌ Нет, отменить", callback_data="cancel_delete")]
        ])

        await query.message.edit_text(
            f"⚠️ Вы уверены, что хотите удалить гильдию '{guild_name}'?\n\n"
            f"Это действие нельзя отменить!",
            reply_markup=confirm_keyboard
        )

    elif data.startswith("confirm_delete_"):
        guild_name = data.replace("confirm_delete_", "")
        success = db_manager.delete_guild(guild_name)
        if success:
            del GUILD_URLS[guild_name]
            await query.message.edit_text(
                f"✅ Гильдия '{guild_name}' успешно удалена!\n\n"
                f"Таблица донатов также была удалена из базы данных."
            )
            markup = create_guilds_keyboard()
            await query.message.reply_text("Клавиатура обновлена ✅", reply_markup=markup)
        else:
            await query.message.edit_text(
                f"❌ Ошибка при удалении гильдии '{guild_name}'\n"
                f"Попробуйте еще раз или проверьте логи."
            )

async def list_guilds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список гильдий"""
    await show_guilds_list(update, context)

if __name__ == '__main__':
    # Инициализируем БД и загружаем гильдии
    db_manager.setup_guilds_table()
    load_guilds_from_db()

    TOKEN = '8145185481:AAFBUZVvdQ0Hmo3ePKJF10fWT384jyduvjw'
    application = ApplicationBuilder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("guilds", list_guilds))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(r'^(.*)$'), handle_guild_buttons))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_guild_input))
    application.add_handler(CallbackQueryHandler(handle_show_all, pattern="^show_all_"))
    application.add_handler(CallbackQueryHandler(handle_pagination, pattern="^close_table$"))
    application.add_handler(CallbackQueryHandler(handle_table_choice, pattern="^(show_full|show_partial)$"))
    application.add_handler(CallbackQueryHandler(handle_refresh, pattern="^refresh_"))
    application.add_handler(CallbackQueryHandler(handle_delete_callback, pattern="^(delete_|confirm_delete_|cancel_delete)"))

    print("Бот запущен. Ожидаю команды...")
    application.run_polling()