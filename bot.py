import logging
import sqlite3
import os
import re
import asyncio
import psycopg2#
from psycopg2.extras import RealDictCursor#
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from telegram.ext import JobQueue

# ДОБАВЬ ПОСЛЕ ИМПОРТОВ:
DATABASE_URL = os.environ.get('DATABASE_URL')

# Глобальный словарь для отслеживания отправленных уведомлений
# Формат: {hash_id: message_id}
sent_notifications = {}

# === КОД ПРОВЕРКИ ПОЛЬЗОВАТЕЛЯ ===
ALLOWED_USER_IDS = {8428922739}  # ЗАМЕНИ на свои user_id

def is_user_allowed(user_id: int) -> bool:
    """Проверяем разрешен ли доступ пользователю"""
    return user_id in ALLOWED_USER_IDS

async def check_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Проверяем доступ пользователя"""
    user_id = update.effective_user.id
    if not is_user_allowed(user_id):
        if update.message:
            await update.message.reply_text(
                "❌ Доступ запрещен!\n"
                "Ваш ID не находится в списке разрешенных пользователей."
            )
        elif update.callback_query:
            await update.callback_query.message.reply_text(
                "❌ Доступ запрещен!\n"
                "Ваш ID не находится в списке разрешенных пользователей."
            )
        return False
    return True

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    if not await check_access(update, context):
        return
    
    await update.message.reply_text(
        "🤖 Добро пожаловать в менеджер библиотек!\nВыберите действие:",
        reply_markup=create_main_menu()
    )
# === КОНЕЦ КОДА ПРОВЕРКИ ===

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== БАЗА ДАННЫХ ====================

#def init_database():
#    """Создаем базу данных с нужными таблицами"""
#    conn = get_connection()
#    cursor = conn.cursor()
#    
#    # Таблица библиотек
#    cursor.execute('''
#        CREATE TABLE IF NOT EXISTS libraries (
#            id INTEGER PRIMARY KEY AUTOINCREMENT,
#            name TEXT UNIQUE NOT NULL,
#            description TEXT,
#            notifications_enabled BOOLEAN DEFAULT FALSE
#        )
#    ''')
#   
#    # Таблица хешей
#    cursor.execute('''
#        CREATE TABLE IF NOT EXISTS hashes (
#            id INTEGER PRIMARY KEY AUTOINCREMENT,
#            library_id INTEGER,
#            hash_text TEXT NOT NULL,
#            phone_number TEXT NOT NULL,
#            status TEXT DEFAULT '',
#            time_text TEXT DEFAULT '00:00',
#            FOREIGN KEY (library_id) REFERENCES libraries (id)
#        )
#    ''')
#    
#    # Проверяем и добавляем колонки если их нет
#    try:
#        cursor.execute("SELECT phone_type FROM hashes LIMIT 1")
#    except sqlite3.OperationalError:
#        cursor.execute("ALTER TABLE hashes ADD COLUMN phone_type TEXT DEFAULT ''")
#        print("✅ Добавлена колонка phone_type в таблицу hashes")
#    
#    try:
#        cursor.execute("SELECT missed_cycles FROM hashes LIMIT 1")
#    except sqlite3.OperationalError:
#        cursor.execute("ALTER TABLE hashes ADD COLUMN missed_cycles INTEGER DEFAULT 0")
#        print("✅ Добавлена колонка missed_cycles в таблицу hashes")
#    
#    try:
#        cursor.execute("SELECT next_notification FROM hashes LIMIT 1")
#    except sqlite3.OperationalError:
#        cursor.execute("ALTER TABLE hashes ADD COLUMN next_notification TEXT DEFAULT ''")
#        print("✅ Добавлена колонка next_notification в таблицу hashes")
#    
#    conn.commit()
#    conn.close()
#    print("✅ База данных создана!")
print(f"🔍 Проверяем DATABASE_URL...")
if DATABASE_URL:
    # Покажем только начало URL для безопасности
    db_info = DATABASE_URL.split('@')
    if len(db_info) > 1:
        print(f"🔍 Подключаемся к: {db_info[1]}")
    else:
        print(f"🔍 DATABASE_URL: {DATABASE_URL[:50]}...")
else:
    print("❌ DATABASE_URL не найден!")

def get_connection():
    DATABASE_URL = os.environ.get('DATABASE_URL')
    print(f"🔗 DATABASE_URL: {DATABASE_URL}")
    
    if DATABASE_URL:
        try:
            # Заменяем внутренний хост на внешний
            external_url = DATABASE_URL.replace('postgres.railway.internal', 'monorail.proxy.rlwy.net')
            print(f"🔗 Подключаемся по URL: {external_url.split('@')[1]}")
            return psycopg2.connect(external_url)
        except Exception as e:
            print(f"❌ Ошибка подключения: {e}")
    
    # Запасной вариант - SQLite
    print("🔗 Используем SQLite...")
    return sqlite3.connect('library.db')
    
def init_database():
    """Создаем базу данных с нужными таблицами"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Таблица библиотек
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS libraries (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            description TEXT,
            notifications_enabled BOOLEAN DEFAULT FALSE
        )
    ''')
    
    # Таблица хешей
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS hashes (
            id SERIAL PRIMARY KEY,
            library_id INTEGER REFERENCES libraries(id),
            hash_text TEXT NOT NULL,
            phone_number TEXT NOT NULL,
            status TEXT DEFAULT '',
            time_text TEXT DEFAULT '00:00',
            phone_type TEXT DEFAULT '',
            missed_cycles INTEGER DEFAULT 0,
            next_notification TEXT DEFAULT ''
        )
    ''')
    
    conn.commit()
    conn.close()
    print("✅ База данных создана!")

# ==================== ВАЛИДАЦИЯ ДАННЫХ ====================

def validate_library_name(name):
    """Проверяем название библиотеки (1-30 символов)"""
    return 1 <= len(name) <= 30

def validate_library_description(desc):
    """Проверяем описание библиотеки (1-50 символов)"""
    return 1 <= len(desc) <= 50

def validate_hash(hash_text):
    """Проверяем формат хеша: FXexpress******H (сохраняем оригинальный регистр)"""
    # Проверяем в верхнем регистре, но возвращаем True для любого валидного регистра
    hash_upper = hash_text.upper()
    if (hash_upper.startswith('FXEXPRESS') and 
        hash_upper.endswith('H') and 
        len(hash_upper) == 16):  # FXEXPRESS(6 цифр)H = 9 + 6 + 1 = 16
        middle = hash_upper[9:15]  # Берем 6 символов после FXEXPRESS
        return middle.isdigit()
    return False

def clean_phone_number(phone):
    """Очищаем номер телефона до формата 79020387257"""
    # Убираем все нецифровые символы
    cleaned = re.sub(r'\D', '', phone)
    # Если номер начинается с 8, заменяем на 7
    if cleaned.startswith('8') and len(cleaned) == 11:
        cleaned = '7' + cleaned[1:]
    # Если номер начинается с +7, убираем +
    elif cleaned.startswith('7') and len(cleaned) == 11:
        cleaned = cleaned
    elif len(cleaned) == 10:
        cleaned = '7' + cleaned
    return cleaned if len(cleaned) == 11 else None

def get_phone_suffix(phone):
    """Получаем последние 4 цифры в формате (72-57)"""
    if len(phone) >= 4:
        last_four = phone[-4:]
        return f"({last_four[:2]}-{last_four[2:]})"
    return ""

def validate_time(time_str):
    """Проверяем время в формате HH:MM"""
    try:
        hours, minutes = map(int, time_str.split(':'))
        return 0 <= hours <= 23 and 0 <= minutes <= 59
    except:
        return False

def format_time_from_digits(digits):
    """Форматируем 4 цифры в время HH:MM"""
    if len(digits) == 4 and digits.isdigit():
        hours = int(digits[:2])
        minutes = int(digits[2:])
        if 0 <= hours <= 23 and 0 <= minutes <= 59:
            return f"{hours:02d}:{minutes:02d}"
    return None

async def delete_message_with_countdown(update, context, message_id, text):
    """Удаляем сообщение с отсчетом времени"""
    try:
        # Показываем отсчет
        for i in range(3, 0, -1):
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=message_id,
                text=f"{text}\n\nУдаление через {i}..."
            )
            await asyncio.sleep(1)
        
        # Удаляем сообщение
        await context.bot.delete_message(
            chat_id=update.effective_chat.id,
            message_id=message_id
        )
    except Exception as e:
        print(f"Ошибка при удалении сообщения: {e}")

# ==================== КЛАВИАТУРЫ ====================

def create_main_menu():
    """Главное меню"""
    keyboard = [
        [InlineKeyboardButton("➕ Создать библиотеку", callback_data="create_library")],
        [InlineKeyboardButton("📚 Выбрать библиотеку", callback_data="select_library")],
        [InlineKeyboardButton("👤 Добавить Клиента", callback_data="add_client")],
        [InlineKeyboardButton("🗑️ Удалить Клиента", callback_data="delete_client")],
        [InlineKeyboardButton("🎯 Актуальные хеши", callback_data="actual_hashes")],
        [InlineKeyboardButton("🔔 Управление уведомлениями", callback_data="manage_notifications")],
        [InlineKeyboardButton("🔍 Поиск по хешу", callback_data="search_hash")],
        [InlineKeyboardButton("📞 Поиск по номеру", callback_data="search_number")]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_back_button(target="back_to_main"):
    """Создает кнопку Назад"""
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=target)]])

def create_library_menu(library_id):
    """Меню для конкретной библиотеки"""
    keyboard = [
        [InlineKeyboardButton("📋 Просмотр хешей", callback_data=f"view_hashes_{library_id}")],
        [InlineKeyboardButton("➕ Добавить хеш", callback_data=f"add_hash_{library_id}")],
        [InlineKeyboardButton("✏️ Переименовать", callback_data=f"rename_lib_{library_id}")],
        [InlineKeyboardButton("📝 Изменить описание", callback_data=f"change_desc_{library_id}")],
        [InlineKeyboardButton("🔍 Поиск в библиотеке", callback_data=f"search_in_lib_{library_id}")],
        [InlineKeyboardButton("🔙 К списку библиотек", callback_data="select_library")]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_hash_actions_menu(hash_id, library_id):
    """Меню действий с хешем"""
    keyboard = [
        [InlineKeyboardButton("🕐 Обновить время", callback_data=f"update_time_{hash_id}")],
        [InlineKeyboardButton("⏰ Указать время вручную", callback_data=f"set_time_manual_{hash_id}")],
        [InlineKeyboardButton("📝 Обновить статус", callback_data=f"update_status_{hash_id}")],
        [InlineKeyboardButton("🗑️ Удалить хеш", callback_data=f"delete_hash_{hash_id}")],
        [InlineKeyboardButton("🔙 Назад к библиотеке", callback_data=f"view_library_{library_id}")]
    ]
    return InlineKeyboardMarkup(keyboard)

# ==================== ОСНОВНЫЕ КОМАНДЫ ====================

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /menu"""
    if not await check_access(update, context):
        return
    
    await update.message.reply_text(
        "🤖 Главное меню:\nВыберите действие:",
        reply_markup=create_main_menu()
    )

# ==================== СОЗДАНИЕ БИБЛИОТЕКИ ====================

async def start_create_library(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинаем процесс создания библиотеки"""
    if not await check_access(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    
    context.user_data['creating_library'] = True
    context.user_data['step'] = 'waiting_name'
    context.user_data['last_bot_message'] = query.message.message_id
    
    await query.edit_message_text(
        "📝 Введите название для новой библиотеки (1-30 символов):",
        reply_markup=create_back_button()
    )

async def handle_library_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатываем введенное название библиотеки"""
    if not await check_access(update, context):
        return
        
    if context.user_data.get('step') != 'waiting_name':
        return
    
    library_name = update.message.text
    
    if not validate_library_name(library_name):
        error_msg = await update.message.reply_text(
            "❌ Название должно содержать от 1 до 30 символов."
        )
        # Запускаем отсчет удаления для сообщения об ошибке
        asyncio.create_task(delete_message_with_countdown(
            update, context, error_msg.message_id, "❌ Неверный формат названия"
        ))
        return
    
    await update.message.delete()
    
    context.user_data['library_name'] = library_name
    context.user_data['step'] = 'waiting_description'
    
    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=context.user_data.get('last_bot_message'),
        text="📝 Укажите описание библиотеки (1-50 символов):"
    )

async def handle_library_description(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатываем введенное описание библиотеки"""
    if not await check_access(update, context):
        return
        
    if context.user_data.get('step') != 'waiting_description':
        return
    
    description = update.message.text
    
    if not validate_library_description(description):
        error_msg = await update.message.reply_text(
            "❌ Описание должно содержать от 1 до 50 символов."
        )
        # Запускаем отсчет удаления для сообщения об ошибке
        asyncio.create_task(delete_message_with_countdown(
            update, context, error_msg.message_id, "❌ Неверный формат описания"
        ))
        return
    
    await update.message.delete()
    
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO libraries (name, description) VALUES (?, ?)",
            (context.user_data['library_name'], description)
        )
        conn.commit()
        conn.close()
        
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data.get('last_bot_message'),
            text=f"✅ Библиотека \"{context.user_data['library_name']}\" успешно создана!"
        )
        
        await asyncio.sleep(2)
        
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data.get('last_bot_message'),
            text="🤖 Выберите действие:",
            reply_markup=create_main_menu()
        )
        
    except sqlite3.IntegrityError:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data.get('last_bot_message'),
            text="❌ Библиотека с таким названием уже существует!",
            reply_markup=create_main_menu()
        )
    finally:
        conn.close()
    
    context.user_data.clear()

# ==================== ВЫБОР БИБЛИОТЕКИ ====================

async def show_libraries(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показываем список библиотек"""
    if not await check_access(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, description FROM libraries")
    libraries = cursor.fetchall()
    conn.close()
    
    if not libraries:
        await query.edit_message_text(
            "📭 Библиотек пока нет!\nСоздайте первую библиотеку.",
            reply_markup=create_back_button()
        )
        return
    
    text = "📚 Ваши библиотеки:\n\n"
    keyboard = []
    
    for lib_id, name, description in libraries:
        # Получаем количество хешей
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM hashes WHERE library_id = ?", (lib_id,))
        hash_count = cursor.fetchone()[0]
        conn.close()
        
        text += f"• {name} ({hash_count} хешей)\n"
        keyboard.append([InlineKeyboardButton(f"📖 {name}", callback_data=f"view_library_{lib_id}")])
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")])
    
    await query.edit_message_text(
        text=text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def view_library(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показываем информацию о библиотеке"""
    if not await check_access(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    
    library_id = int(query.data.split('_')[-1])
    
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name, description FROM libraries WHERE id = ?", (library_id,))
    library = cursor.fetchone()
    
    # Получаем хеши библиотеки - ИСПРАВЛЕННЫЙ ЗАПРОС
    cursor.execute("""
        SELECT hash_text, phone_number, status, time_text 
        FROM hashes 
        WHERE library_id = ? 
        ORDER BY id DESC
    """, (library_id,))
    hashes = cursor.fetchall()
    conn.close()
    
    if library:
        name, description = library
        text = f"📚 Библиотека: {name}\n"
        text += f"📝 Описание: {description}\n"
        text += f"🎯 Хешей: {len(hashes)}\n\n"
        
        if hashes:
            text += "Последние хеши:\n"
            for i, (hash_text, phone, status, time_text) in enumerate(hashes[:5], 1):
                phone_suffix = get_phone_suffix(phone)
                text += f"{i}. {hash_text}\n"
                text += f"   📞 {phone} {phone_suffix} [{time_text}]\n"
                if status:
                    text += f"   📋 {status}\n"
                text += "\n"
        else:
            text += "📭 В библиотеке пока нет хешей\n"
        
        await query.edit_message_text(
            text=text,
            reply_markup=create_library_menu(library_id)
        )

# ==================== ДОБАВЛЕНИЕ КЛИЕНТА ====================

async def start_add_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинаем процесс добавления клиента"""
    if not await check_access(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    
    # Сначала выбираем библиотеку
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name FROM libraries")
    libraries = cursor.fetchall()
    conn.close()
    
    if not libraries:
        await query.edit_message_text(
            "❌ Нет библиотек для добавления клиента!\nСначала создайте библиотеку.",
            reply_markup=create_back_button()
        )
        return
    
    text = "📚 Выберите библиотеку для добавления клиента:\n\n"
    keyboard = []
    
    for lib_id, name in libraries:
        text += f"• {name}\n"
        keyboard.append([InlineKeyboardButton(f"📖 {name}", callback_data=f"add_to_library_{lib_id}")])
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")])
    
    await query.edit_message_text(
        text=text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def start_add_to_library(update: Update, context: ContextTypes.DEFAULT_TYPE, library_id: int):
    """Начинаем добавление клиента в конкретную библиотеку"""
    if not await check_access(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    
    context.user_data['adding_client'] = True
    context.user_data['step'] = 'waiting_hash'
    context.user_data['library_id'] = library_id
    context.user_data['last_bot_message'] = query.message.message_id
    
    # Получаем название библиотеки
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM libraries WHERE id = ?", (library_id,))
    lib_name = cursor.fetchone()[0]
    conn.close()
    
    await query.edit_message_text(
        f"📚 Добавление в библиотеку: {lib_name}\n\n"
        "🔍 Введите хеш клиента (формат: FXexpress123456H):",
        reply_markup=create_back_button("add_client")
    )

async def handle_client_hash(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатываем хеш клиента"""
    if not await check_access(update, context):
        return
        
    if context.user_data.get('step') != 'waiting_hash':
        return
    
    hash_text = update.message.text.strip()
    
    # Удаляем сообщение пользователя сразу
    await update.message.delete()
    
    if not validate_hash(hash_text):
        error_msg = await update.message.reply_text(
            "❌ Неверный формат хеша!\nПравильный формат: FXexpress123456H"
        )
        # Запускаем отсчет удаления для сообщения об ошибке
        asyncio.create_task(delete_message_with_countdown(
            update, context, error_msg.message_id, "❌ Неверный формат хеша"
        ))
        return
    
    # Проверяем, существует ли хеш уже в базе данных (ищем в любом регистре)
    conn = get_connection()
    cursor = conn.cursor()
    
    # Ищем хеш в любом регистре - сравниваем в верхнем регистре
    cursor.execute("SELECT id FROM hashes WHERE UPPER(hash_text) = UPPER(?)", (hash_text,))
    existing_hash = cursor.fetchone()
    conn.close()
    
    if existing_hash:
        error_msg = await update.message.reply_text(
            f"❌ Хеш '{hash_text}' уже существует в базе данных!\nВведите другой хеш:"
        )
        # Запускаем отсчет удаления для сообщения об ошибке
        asyncio.create_task(delete_message_with_countdown(
            update, context, error_msg.message_id, "❌ Хеш уже существует"
        ))
        return
    
    # Сохраняем хеш в ОРИГИНАЛЬНОМ регистре как ввел пользователь
    context.user_data['hash_text'] = hash_text
    context.user_data['step'] = 'waiting_phone'
    
    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=context.user_data.get('last_bot_message'),
        text="📞 Введите номер телефона:"
    )

async def handle_client_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатываем номер телефона"""
    if not await check_access(update, context):
        return
        
    if context.user_data.get('step') != 'waiting_phone':
        return
    
    phone = update.message.text
    cleaned_phone = clean_phone_number(phone)
    
    # Удаляем сообщение пользователя сразу
    await update.message.delete()
    
    if not cleaned_phone:
        error_msg = await update.message.reply_text(
            "❌ Неверный формат номера!"
        )
        # Запускаем отсчет удаления для сообщения об ошибке
        asyncio.create_task(delete_message_with_countdown(
            update, context, error_msg.message_id, "❌ Неверный формат номера"
        ))
        return
    
    # Проверяем, существует ли номер телефона уже в базе данных
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, hash_text FROM hashes WHERE phone_number = ?", (cleaned_phone,))
    existing_phone = cursor.fetchone()
    conn.close()
    
    if existing_phone:
        phone_id, existing_hash = existing_phone
        error_msg = await update.message.reply_text(
            f"❌ Номер телефона '{cleaned_phone}' уже существует в базе данных!\n"
            f"Он привязан к хешу: {existing_hash}\n"
            f"Введите другой номер телефона:"
        )
        # Запускаем отсчет удаления для сообщения об ошибке
        asyncio.create_task(delete_message_with_countdown(
            update, context, error_msg.message_id, "❌ Номер телефона уже существует"
        ))
        return
    
    context.user_data['phone'] = cleaned_phone
    context.user_data['step'] = 'waiting_phone_type'
    
    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=context.user_data.get('last_bot_message'),
        text="📱 Введите тип телефона (до 20 символов, например: Alik):"
    )

async def handle_client_phone_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатываем тип телефона"""
    if not await check_access(update, context):
        return
        
    if context.user_data.get('step') != 'waiting_phone_type':
        return
    
    phone_type = update.message.text
    
    # Удаляем сообщение пользователя сразу
    await update.message.delete()
    
    if len(phone_type) > 20:
        error_msg = await update.message.reply_text(
            "❌ Тип телефона слишком длинный! Максимум 20 символов."
        )
        # Запускаем отсчет удаления для сообщения об ошибке
        asyncio.create_task(delete_message_with_countdown(
            update, context, error_msg.message_id, "❌ Слишком длинный тип телефона"
        ))
        return
    
    context.user_data['phone_type'] = phone_type
    context.user_data['step'] = 'waiting_status'
    
    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=context.user_data.get('last_bot_message'),
        text="📝 Введите статус (до 64 символов):"
    )

async def handle_client_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатываем статус и сохраняем клиента"""
    if not await check_access(update, context):
        return
        
    if context.user_data.get('step') != 'waiting_status':
        return
    
    status = update.message.text
    
    if len(status) > 64:
        error_msg = await update.message.reply_text(
            "❌ Статус слишком длинный! Максимум 64 символа."
        )
        # Запускаем отсчет удаления для сообщения об ошибке
        asyncio.create_task(delete_message_with_countdown(
            update, context, error_msg.message_id, "❌ Слишком длинный статус"
        ))
        return
    
    await update.message.delete()
    
    # Сохраняем клиента в базу с текущим временем
    conn = get_connection()
    cursor = conn.cursor()
    
    # Получаем текущее время
    current_time = datetime.now().strftime("%H:%M:%S")
    
    try:
        cursor.execute(
            "INSERT INTO hashes (library_id, hash_text, phone_number, phone_type, status, time_text) VALUES (?, ?, ?, ?, ?, ?)",
            (context.user_data['library_id'], context.user_data['hash_text'], context.user_data['phone'], context.user_data['phone_type'], status, current_time)
        )
        hash_id = cursor.lastrowid
        
        conn.commit()
        conn.close()
        
        # Получаем название библиотеки для сообщения
        cursor.execute("SELECT name FROM libraries WHERE id = ?", (context.user_data['library_id'],))
        lib_name = cursor.fetchone()[0]
        
        phone_suffix = get_phone_suffix(context.user_data['phone'])
        
        # Если уведомления включены, создаем индивидуальный job для этого хеша
        if context.application.bot_data.get('notifications_enabled', False):
            # Запускаем индивидуальный job для этого хеша через 1 час (3600 секунд)
            context.application.job_queue.run_once(
                send_hash_notification, 
                when=3480,  # через 1 час = 3600 секунд
                chat_id=update.effective_chat.id,
                data=hash_id,
                name=f"hash_notification_{hash_id}"
            )
        
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data.get('last_bot_message'),
            text=f"✅ Клиент успешно добавлен!\n\n"
                 f"Хеш: {context.user_data['hash_text']}\n"
                 f"Номер: {context.user_data['phone']} {phone_suffix}\n"
                 f"Тип: {context.user_data['phone_type']}\n"
                 f"Статус: {status}\n"
                 f"Время: {current_time}\n"
                 f"Следующее уведомление: через 1 час\n"
                 f"Библиотека: {lib_name}"
        )
        
        await asyncio.sleep(3)
        
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data.get('last_bot_message'),
            text="🤖 Выберите действие:",
            reply_markup=create_main_menu()
        )
        
    except sqlite3.IntegrityError:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data.get('last_bot_message'),
            text="❌ Хеш уже существует в этой библиотеке!",
            reply_markup=create_main_menu()
        )
    finally:
        conn.close()
    
    context.user_data.clear()

# ==================== УДАЛЕНИЕ КЛИЕНТА ====================

async def start_delete_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинаем процесс удаления клиента"""
    if not await check_access(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    
    context.user_data['deleting_client'] = True
    context.user_data['step'] = 'waiting_hash_for_delete'
    context.user_data['last_bot_message'] = query.message.message_id
    
    await query.edit_message_text(
        "🔍 Введите хеш для удаления:\n\nФормат: FXexpress123456H",
        reply_markup=create_back_button()
    )

async def handle_hash_for_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатываем хеш для удаления"""
    if not await check_access(update, context):
        return
        
    if context.user_data.get('step') != 'waiting_hash_for_delete':
        return
    
    hash_to_delete = update.message.text.strip()
    
    if not validate_hash(hash_to_delete):
        error_msg = await update.message.reply_text(
            "❌ Неверный формат хеша!"
        )
        # Запускаем отсчет удаления для сообщения об ошибке
        asyncio.create_task(delete_message_with_countdown(
            update, context, error_msg.message_id, "❌ Неверный формат хеша"
        ))
        return
    
    await update.message.delete()
    
    # Ищем хеш во всех библиотеках (используем оригинальный регистр как в базе)
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT h.hash_text, h.phone_number, h.status, l.name 
        FROM hashes h 
        JOIN libraries l ON h.library_id = l.id 
        WHERE h.hash_text = ?
    """, (hash_to_delete,))  # ← Ищем по оригинальному регистру
    
    found_hashes = cursor.fetchall()
    conn.close()
    
    if not found_hashes:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data.get('last_bot_message'),
            text=f"❌ **Хеш не найден!**\n\n`{hash_to_delete}`\n\nХеш не найден ни в одной библиотеке.",
            reply_markup=create_main_menu(),
            parse_mode='Markdown'
        )
        context.user_data.clear()
        return
    
    context.user_data['hash_to_delete'] = hash_to_delete
    context.user_data['found_hashes'] = found_hashes
    context.user_data['step'] = 'confirm_deletion'
    
    # Формируем информацию о найденных хешах
    text = "🎯 **Найденные данные для удаления:**\n\n"
    
    for i, (hash_text, phone, status, lib_name) in enumerate(found_hashes, 1):
        phone_suffix = get_phone_suffix(phone)
        text += f"**{i}. Хеш:** `{hash_text}`\n"
        if phone:
            text += f"   **Номер:** `{phone}` {phone_suffix}\n"
        if status:
            text += f"   **Статус:** {status}\n"
        text += f"   **Библиотека:** {lib_name}\n\n"
    
    text += "❓ **Вы уверены что хотите удалить данный хеш из всех библиотек?**"
    
    # Кнопки подтверждения
    keyboard = [
        [
            InlineKeyboardButton("✅ Да, удалить", callback_data="confirm_delete_yes"),
            InlineKeyboardButton("❌ Нет, отмена", callback_data="confirm_delete_no")
        ]
    ]
    
    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=context.user_data.get('last_bot_message'),
        text=text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_delete_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатываем подтверждение удаления"""
    if not await check_access(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    
    if query.data == "confirm_delete_yes":
        # УДАЛЯЕМ хеш из всех библиотек (по оригинальному регистру)
        hash_to_delete = context.user_data.get('hash_to_delete')
        found_hashes = context.user_data.get('found_hashes', [])
        
        conn = get_connection()
        cursor = conn.cursor()
        
        # Удаляем хеш из всех библиотек по оригинальному регистру
        cursor.execute("DELETE FROM hashes WHERE hash_text = ?", (hash_to_delete,))
        deleted_count = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        # Формируем отчет об удалении
        text = "✅ **Хеш успешно удален!**\n\n"
        text += f"**Удалено записей:** {deleted_count}\n"
        text += f"**Хеш:** `{hash_to_delete}`\n\n"
        
        if found_hashes:
            text += "**Из библиотек:**\n"
            for hash_text, phone, status, lib_name in found_hashes:
                text += f"• {lib_name}\n"
        
        await query.edit_message_text(
            text=text,
            reply_markup=create_main_menu(),
            parse_mode='Markdown'
        )
        
    else:  # confirm_delete_no
        # Отмена удаления
        await query.edit_message_text(
            text="❌ **Удаление отменено**\n\nХеш не был удален.",
            reply_markup=create_main_menu(),
            parse_mode='Markdown'
        )
    
    # Очищаем состояние
    context.user_data.clear()

# ==================== АКТУАЛЬНЫЕ ХЕШИ ====================

async def show_actual_hashes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показываем актуальные хеши (истекшие по времени)"""
    if not await check_access(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Получаем текущее время с секундами
    current_time = datetime.now().strftime("%H:%M:%S")
    
    # Ищем хеши с истекшим временем
    cursor.execute("""
        SELECT h.hash_text, h.phone_number, h.phone_type, h.status, h.time_text, l.name, h.missed_cycles 
        FROM hashes h 
        JOIN libraries l ON h.library_id = l.id 
        WHERE h.time_text < ? AND h.time_text != '00:00:00'
        ORDER BY h.time_text ASC
        LIMIT 20
    """, (current_time,))
    
    hashes = cursor.fetchall()
    conn.close()
    
    if not hashes:
        await query.edit_message_text(
            "✅ Нет хешей с истекшим временем",
            reply_markup=create_back_button()
        )
        return
    
    text = "🎯 Актуальные хеши (время отписи):\n\n"
    
    for i, (hash_text, phone, phone_type, status, time_text, lib_name, missed_cycles) in enumerate(hashes, 1):
        phone_suffix = get_phone_suffix(phone)
        text += f"{i}. {hash_text}\n"
        text += f"   📞 `{phone}` {phone_suffix} [{time_text}]\n"
        if status:
            text += f"   📋 Status: {status}\n"
        text += f"   💬 ВЦ: {lib_name}\n"
        if phone_type:
            text += f"   📱 Телефон: {phone_type}\n"
        if missed_cycles > 0:
            text += f"   🔴 Пропущено кругов: {missed_cycles}\n"
        text += "\n"
    
    await query.edit_message_text(
        text=text,
        reply_markup=create_back_button(),
        parse_mode='Markdown'
    )

# ==================== ПОИСК ====================

async def start_search_hash(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинаем поиск по хешу"""
    if not await check_access(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    
    context.user_data['searching_hash'] = True
    context.user_data['step'] = 'waiting_search_hash'
    context.user_data['last_bot_message'] = query.message.message_id
    
    await query.edit_message_text(
        "🔍 Введите хеш для поиска:",
        reply_markup=create_back_button()
    )

async def handle_search_hash(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатываем поиск по хешу"""
    if not await check_access(update, context):
        return
        
    if context.user_data.get('step') != 'waiting_search_hash':
        return
    
    search_hash = update.message.text.strip()
    
    await update.message.delete()
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT h.hash_text, h.phone_number, h.phone_type, h.status, h.time_text, l.name 
        FROM hashes h 
        JOIN libraries l ON h.library_id = l.id 
        WHERE h.hash_text LIKE ?
    """, (f'%{search_hash}%',))
    
    results = cursor.fetchall()
    conn.close()
    
    if not results:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data.get('last_bot_message'),
            text=f"❌ По запросу '{search_hash}' ничего не найдено",
            reply_markup=create_main_menu()
        )
    else:
        text = f"🔍 Результаты поиска по '{search_hash}':\n\n"
        
        for hash_text, phone, phone_type, status, time_text, lib_name in results:
            phone_suffix = get_phone_suffix(phone)
            text += f"• {hash_text}\n"
            text += f"   📞 `{phone}` {phone_suffix} [{time_text}]\n"
            if status:
                text += f"   📋 Status: {status}\n"
            text += f"   💬 ВЦ: {lib_name}\n"
            if phone_type:
                text += f"   📱 Телефон: {phone_type}\n"
            text += "\n"
        
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data.get('last_bot_message'),
            text=text,
            reply_markup=create_back_button(),
            parse_mode='Markdown'
        )
    
    context.user_data.clear()

async def start_search_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинаем поиск по номеру"""
    if not await check_access(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    
    context.user_data['searching_number'] = True
    context.user_data['step'] = 'waiting_search_number'
    context.user_data['last_bot_message'] = query.message.message_id
    
    await query.edit_message_text(
        "📞 Введите номер для поиска:",
        reply_markup=create_back_button()
    )

async def handle_search_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатываем поиск по номеру"""
    if not await check_access(update, context):
        return
        
    if context.user_data.get('step') != 'waiting_search_number':
        return
    
    search_number = update.message.text
    cleaned_number = clean_phone_number(search_number)
    
    await update.message.delete()
    
    if not cleaned_number:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data.get('last_bot_message'),
            text="❌ Неверный формат номера!",
            reply_markup=create_main_menu()
        )
        context.user_data.clear()
        return
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT h.hash_text, h.phone_number, h.phone_type, h.status, h.time_text, l.name 
        FROM hashes h 
        JOIN libraries l ON h.library_id = l.id 
        WHERE h.phone_number LIKE ?
    """, (f'%{cleaned_number}%',))
    
    results = cursor.fetchall()
    conn.close()
    
    if not results:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data.get('last_bot_message'),
            text=f"❌ По номеру '{cleaned_number}' ничего не найдено",
            reply_markup=create_main_menu()
        )
    else:
        text = f"🔍 Результаты поиска по номеру '{cleaned_number}':\n\n"
        
        for hash_text, phone, phone_type, status, time_text, lib_name in results:
            phone_suffix = get_phone_suffix(phone)
            text += f"• {hash_text}\n"
            text += f"   📞 `{phone}` {phone_suffix} [{time_text}]\n"
            if status:
                text += f"   📋 Status: {status}\n"
            text += f"   💬 ВЦ: {lib_name}\n"
            if phone_type:
                text += f"   📱 Телефон: {phone_type}\n"
            text += "\n"
        
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data.get('last_bot_message'),
            text=text,
            reply_markup=create_back_button(),
            parse_mode='Markdown'
        )
    
    context.user_data.clear()

# ==================== ФУНКЦИИ БИБЛИОТЕКИ ====================

async def view_library_hashes(update: Update, context: ContextTypes.DEFAULT_TYPE, library_id: int):
    """Показываем хеши конкретной библиотеки"""
    if not await check_access(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Получаем информацию о библиотеке
    cursor.execute("SELECT name FROM libraries WHERE id = ?", (library_id,))
    lib_name = cursor.fetchone()[0]
    
    # Получаем хеши библиотеки - ИСПРАВЛЕННЫЙ ЗАПРОС
    cursor.execute("""
        SELECT id, hash_text, phone_number, status, time_text 
        FROM hashes 
        WHERE library_id = ? 
        ORDER BY id DESC
    """, (library_id,))
    hashes = cursor.fetchall()
    conn.close()
    
    if not hashes:
        await query.edit_message_text(
            f"📭 В библиотеке '{lib_name}' нет хешей",
            reply_markup=create_library_menu(library_id)
        )
        return
    
    text = f"📚 Хеши библиотеки '{lib_name}':\n\n"
    
    for hash_id, hash_text, phone, status, time_text in hashes:
        phone_suffix = get_phone_suffix(phone)
        text += f"• {hash_text}\n"
        text += f"  📞 {phone} {phone_suffix} [{time_text}]\n"
        if status:
            text += f"  📋 {status}\n"
        text += f"  [ID: {hash_id}]\n\n"
    
    await query.edit_message_text(
        text=text,
        reply_markup=create_library_menu(library_id)
    )

async def manage_notifications(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Управление уведомлениями"""
    if not await check_access(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    
    # Проверяем статус уведомлений
    notifications_enabled = context.application.bot_data.get('notifications_enabled', False)
    
    if notifications_enabled:
        text = "🔔 Уведомления: ВКЛЮЧЕНЫ\n\nБот присылает уведомления каждую минуту для хешей с истекшим временем."
        button_text = "🔕 Выключить уведомления"
        callback_data = "disable_notifications"
    else:
        text = "🔕 Уведомления: ВЫКЛЮЧЕНЫ\n\nВключите уведомления для получения напоминаний."
        button_text = "🔔 Включить уведомления" 
        callback_data = "enable_notifications"
    
    keyboard = [
        [InlineKeyboardButton(button_text, callback_data=callback_data)],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
    ]
    
    await query.edit_message_text(
        text=text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def update_hash_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обновляем время хеша на текущее и перезапускаем индивидуальный таймер"""
    if not await check_access(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    
    # Получаем данные из callback_data (формат: complete_hash_123)
    hash_id = int(query.data.split('_')[-1])
    
    # Удаляем уведомление из отслеживания
    if hash_id in sent_notifications:
        del sent_notifications[hash_id]
    
    # Получаем текущее время
    current_time = datetime.now().strftime("%H:%M:%S")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Обновляем время хеша и обнуляем счетчик
    cursor.execute(
        "UPDATE hashes SET time_text = ?, missed_cycles = 0 WHERE id = ?",
        (current_time, hash_id)
    )
    
    # Получаем информацию о хеше для сообщения
    cursor.execute("""
        SELECT h.hash_text, h.phone_number, h.phone_type, h.status, l.name 
        FROM hashes h 
        JOIN libraries l ON h.library_id = l.id 
        WHERE h.id = ?
    """, (hash_id,))
    
    hash_info = cursor.fetchone()
    conn.commit()
    conn.close()
    
    if hash_info:
        hash_text, phone, phone_type, status, lib_name = hash_info
        phone_suffix = get_phone_suffix(phone)
        
        # Удаляем сообщение с уведомлением
        await query.message.delete()
        
        # Если уведомления включены, перезапускаем индивидуальный job для этого хеша
        if context.application.bot_data.get('notifications_enabled', False):
            # Останавливаем старый job если есть
            old_job_name = f"hash_notification_{hash_id}"
            current_jobs = context.application.job_queue.get_jobs_by_name(old_job_name)
            for job in current_jobs:
                job.schedule_removal()
            
            # Запускаем новый индивидуальный job для этого хеша через 1 час (3600 секунд)
            context.application.job_queue.run_once(
                send_hash_notification, 
                when=3480,  # через 1 час = 3600 секунд
                chat_id=query.message.chat_id,
                data=hash_id,
                name=f"hash_notification_{hash_id}"
            )
        
        # Отправляем подтверждение
        confirmation_msg = await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=f"✅ Время хеша обновлено!\n\n"
                 f"Хеш: {hash_text}\n"
                 f"Новое время: {current_time}\n"
                 f"Счетчик пропущенных кругов обнулен\n"
                 f"Следующее уведомление: через 1 час",
            parse_mode='Markdown'
        )
        
        # Удаляем подтверждение через 3 секунды
        await asyncio.sleep(3)
        await confirmation_msg.delete()

async def send_hash_notification(context: ContextTypes.DEFAULT_TYPE):
    """Отправляем уведомление для конкретного хеша и планируем следующее"""
    job = context.job
    hash_id = job.data
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Получаем информацию о хеше
    cursor.execute("""
        SELECT h.hash_text, h.phone_number, h.phone_type, h.status, h.time_text, l.name, h.missed_cycles 
        FROM hashes h 
        JOIN libraries l ON h.library_id = l.id 
        WHERE h.id = ?
    """, (hash_id,))
    
    hash_info = cursor.fetchone()
    
    if hash_info:
        hash_text, phone, phone_type, status, time_text, lib_name, missed_cycles = hash_info
        phone_suffix = get_phone_suffix(phone)
        
        message = f"🔔 Время отписи!\n\n"
        message += f"Хеш: {hash_text}\n"
        message += f"📞 `{phone}` {phone_suffix} [{time_text}]\n"
        if status:
            message += f"📋 Status: {status}\n"
        message += f"💬 ВЦ: {lib_name}\n"
        if phone_type:
            message += f"📱 Телефон: {phone_type}\n"
        
        # Добавляем информацию о пропущенных кругах только если они есть
        if missed_cycles > 0:
            message += f"🔴 Пропущен {missed_cycles} круг!\n"
        
        # Создаем клавиатуру с кнопкой "Завершить"
        keyboard = [
            [InlineKeyboardButton("✅ Завершить", callback_data=f"complete_hash_{hash_id}")]
        ]
        
        # Проверяем, есть ли уже сообщение для этого хеша
        if hash_id in sent_notifications:
            try:
                # Увеличиваем счетчик пропущенных кругов для этого хеша
                new_missed_cycles = missed_cycles + 1
                cursor.execute(
                    "UPDATE hashes SET missed_cycles = ? WHERE id = ?",
                    (new_missed_cycles, hash_id)
                )
                conn.commit()
                conn.close()
                
                # Обновляем сообщение с новым счетчиком
                if new_missed_cycles > 0:
                    message += f"🔴 Пропущен {new_missed_cycles} круг!\n"
                
                # Пытаемся отредактировать существующее сообщение
                await context.bot.edit_message_text(
                    chat_id=job.chat_id,
                    message_id=sent_notifications[hash_id],
                    text=message,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='Markdown'
                )
                print(f"✅ Отредактировано уведомление для хеша {hash_text}, кругов: {new_missed_cycles}")
            except Exception as e:
                # Если не удалось отредактировать (сообщение удалено и т.д.), отправляем новое
                print(f"❌ Не удалось отредактировать уведомление для {hash_text}: {e}")
                new_message = await context.bot.send_message(
                    chat_id=job.chat_id,
                    text=message,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='Markdown'
                )
                sent_notifications[hash_id] = new_message.message_id
        else:
            # Первое уведомление - НЕ увеличиваем счетчик
            sent_notifications[hash_id] = None  # Помечаем что уведомление создано
            
            # Отправляем новое уведомление и сохраняем ID сообщения
            new_message = await context.bot.send_message(
                chat_id=job.chat_id,
                text=message,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            sent_notifications[hash_id] = new_message.message_id
            print(f"✅ Отправлено первое уведомление для хеша {hash_text}")
        
        # Планируем следующее уведомление для этого хеша через 58 минут
        context.application.job_queue.run_once(
            send_hash_notification, 
            when=3480,  # 58 минут = 3480 секунд
            chat_id=job.chat_id,
            data=hash_id,
            name=f"hash_notification_{hash_id}"
        )
    
    conn.close()

async def send_notifications(context: ContextTypes.DEFAULT_TYPE):
    """Отправляем уведомления для истекших хешей (индивидуальные таймеры)"""
    
    # Проверяем включены ли уведомления
    if not context.application.bot_data.get('notifications_enabled', False):
        return
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Получаем текущее время с секундами
    current_time = datetime.now().strftime("%H:%M:%S")
    
    # Ищем хеши, у которых время следующего уведомления наступило
    cursor.execute("""
        SELECT h.id, h.hash_text, h.phone_number, h.phone_type, h.status, h.time_text, l.name, h.missed_cycles, h.next_notification 
        FROM hashes h 
        JOIN libraries l ON h.library_id = l.id 
        WHERE h.next_notification <= ? AND h.time_text != '00:00:00' AND h.next_notification != ''
        ORDER BY h.next_notification ASC
    """, (current_time,))
    
    ready_hashes = cursor.fetchall()
    
    for hash_id, hash_text, phone, phone_type, status, time_text, lib_name, missed_cycles, next_notification in ready_hashes:
        phone_suffix = get_phone_suffix(phone)
        message = f"🔔 Время отписи!\n\n"
        message += f"Хеш: {hash_text}\n"
        message += f"📞 `{phone}` {phone_suffix} [{time_text}]\n"
        if status:
            message += f"📋 Status: {status}\n"
        message += f"💬 ВЦ: {lib_name}\n"
        if phone_type:
            message += f"📱 Телефон: {phone_type}\n"
        
        # Добавляем информацию о пропущенных кругах
        if missed_cycles > 0:
            message += f"🔴 Пропущен {missed_cycles} круг!\n"
        
        # Создаем клавиатуру с кнопкой "Завершить"
        keyboard = [
            [InlineKeyboardButton("✅ Завершить", callback_data=f"complete_hash_{hash_id}")]
        ]
        
        # Проверяем, было ли уже отправлено уведомление для этого хеша
        if hash_id in sent_notifications:
            try:
                # Пытаемся отредактировать существующее сообщение
                await context.bot.edit_message_text(
                    chat_id=context.job.chat_id,
                    message_id=sent_notifications[hash_id],
                    text=message,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='Markdown'
                )
                print(f"✅ Отредактировано уведомление для хеша {hash_text}")
            except Exception as e:
                # Если не удалось отредактировать, отправляем новое
                print(f"❌ Не удалось отредактировать уведомление для {hash_text}: {e}")
                new_message = await context.bot.send_message(
                    chat_id=context.job.chat_id,
                    text=message,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='Markdown'
                )
                sent_notifications[hash_id] = new_message.message_id
        else:
            # Отправляем новое уведомление и сохраняем ID сообщения
            new_message = await context.bot.send_message(
                chat_id=context.job.chat_id,
                text=message,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            sent_notifications[hash_id] = new_message.message_id
            print(f"✅ Отправлено новое уведомление для хеша {hash_text}")
        
        # Увеличиваем счетчик пропущенных кругов
        cursor.execute(
            "UPDATE hashes SET missed_cycles = missed_cycles + 1 WHERE id = ?",
            (hash_id,)
        )
        
        # Устанавливаем следующее уведомление через 3480 (58 минут) секунд от текущего времени
        next_time = (datetime.now() + timedelta(seconds=3480)).strftime("%H:%M:%S")
        cursor.execute(
            "UPDATE hashes SET next_notification = ? WHERE id = ?",
            (next_time, hash_id)
        )
        print(f"✅ Установлено следующее уведомление для {hash_text} на {next_time}")
    
    conn.commit()
    conn.close()

async def start_add_hash_to_library(update: Update, context: ContextTypes.DEFAULT_TYPE, library_id: int):
    """Начинаем добавление хеша в библиотеку"""
    if not await check_access(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    
    context.user_data['adding_hash_to_library'] = True
    context.user_data['step'] = 'waiting_hash_for_library'
    context.user_data['library_id'] = library_id
    context.user_data['last_bot_message'] = query.message.message_id
    
    # Получаем название библиотеки
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM libraries WHERE id = ?", (library_id,))
    lib_name = cursor.fetchone()[0]
    conn.close()
    
    await query.edit_message_text(
        f"📚 Добавление хеша в библиотеку: {lib_name}\n\n"
        "🔍 Введите хеш (формат: FXexpress123456H):",
        reply_markup=create_back_button(f"view_library_{library_id}")
    )

async def handle_hash_for_library(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатываем хеш при добавлении через меню библиотеки"""
    if not await check_access(update, context):
        return
        
    if context.user_data.get('step') != 'waiting_hash_for_library':
        return
    
    hash_text = update.message.text.strip()
    
    # Удаляем сообщение пользователя сразу
    await update.message.delete()
    
    if not validate_hash(hash_text):
        error_msg = await update.message.reply_text(
            "❌ Неверный формат хеша!\nПравильный формат: FXexpress123456H"
        )
        asyncio.create_task(delete_message_with_countdown(
            update, context, error_msg.message_id, "❌ Неверный формат хеша"
        ))
        return
    
    # Проверяем, существует ли хеш уже в базе данных (ищем в любом регистре)
    conn = get_connection()
    cursor = conn.cursor()
    
    # Ищем хеш в любом регистре - сравниваем в верхнем регистре
    cursor.execute("SELECT id FROM hashes WHERE UPPER(hash_text) = UPPER(?)", (hash_text,))
    existing_hash = cursor.fetchone()
    conn.close()
    
    if existing_hash:
        error_msg = await update.message.reply_text(
            f"❌ Хеш '{hash_text}' уже существует в базе данных!\nВведите другой хеш:"
        )
        # Запускаем отсчет удаления для сообщения об ошибке
        asyncio.create_task(delete_message_with_countdown(
            update, context, error_msg.message_id, "❌ Хеш уже существует"
        ))
        return
    
    # Сохраняем хеш в ОРИГИНАЛЬНОМ регистре как ввел пользователь
    context.user_data['hash_text'] = hash_text
    context.user_data['step'] = 'waiting_phone'
    
    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=context.user_data.get('last_bot_message'),
        text="📞 Введите номер телефона:"
    )

# ==================== ОБРАБОТКА СООБЩЕНИЙ ====================

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Общий обработчик текстовых сообщений"""
    if not await check_access(update, context):
        return
        
    current_step = context.user_data.get('step')
    
    if current_step == 'waiting_name':
        await handle_library_name(update, context)
    elif current_step == 'waiting_description':
        await handle_library_description(update, context)
    elif current_step == 'waiting_hash':
        await handle_client_hash(update, context)
    elif current_step == 'waiting_phone':
        await handle_client_phone(update, context)
    elif current_step == 'waiting_phone_type':  # ← ДОБАВЬ ЭТУ СТРОЧКУ
        await handle_client_phone_type(update, context)  # ← И ЭТУ
    elif current_step == 'waiting_hash_for_library':  # ← ДОБАВЬ ЭТУ СТРОЧКУ
        await handle_hash_for_library(update, context)  # ← И ЭТУ
    elif current_step == 'waiting_status':
        await handle_client_status(update, context)
    elif current_step == 'waiting_hash_for_delete':
        await handle_hash_for_delete(update, context)
    elif current_step == 'waiting_search_hash':
        await handle_search_hash(update, context)
    elif current_step == 'waiting_search_number':
        await handle_search_number(update, context)
    elif current_step == 'waiting_hash_for_library':
        await handle_client_hash(update, context)
        
    elif data == "enable_notifications":
        context.bot_data['notifications_enabled'] = True
        # Запускаем задачу для уведомлений (для теста - каждую минуту)
        if hasattr(context, 'job_queue'):
            context.job_queue.run_repeating(send_notifications, interval=13, first=3, chat_id=query.message.chat_id)
        await query.edit_message_text(
            text="✅ Уведомления включены! Бот будет присылать уведомления каждую минуту.",
            reply_markup=create_back_button()
        )
    
    elif data == "disable_notifications":
        context.bot_data['notifications_enabled'] = False
        # Останавливаем все задачи уведомлений
        if hasattr(context, 'job_queue'):
            current_jobs = context.job_queue.get_jobs_by_name("notification_job")
            for job in current_jobs:
                job.schedule_removal()
        await query.edit_message_text(
            text="❌ Уведомления выключены!",
            reply_markup=create_back_button()
        )
        
    elif data == "disable_notifications":
        context.application.bot_data['notifications_enabled'] = False
        # Останавливаем все задачи уведомлений
        current_jobs = context.application.job_queue.get_jobs_by_name("notification_job")
        for job in current_jobs:
            job.schedule_removal()
        # Очищаем отслеживание уведомлений
        sent_notifications.clear()
        # Возвращаемся в меню уведомлений чтобы увидеть обновленный статус
        await manage_notifications(update, context)

# ==================== ОБРАБОТКА КНОПОК ====================

async def handle_button_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатываем все нажатия на кнопки"""
    if not await check_access(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data == "back_to_main":
        await query.edit_message_text(
            "🤖 Выберите действие:",
            reply_markup=create_main_menu()
        )
    
    elif data == "create_library":
        await start_create_library(update, context)
    
    elif data == "select_library":
        await show_libraries(update, context)
    
    elif data.startswith("view_library_"):
        await view_library(update, context)
    
    elif data == "add_client":
        await start_add_client(update, context)
    
    elif data.startswith("add_to_library_"):
        library_id = int(data.split('_')[-1])
        await start_add_to_library(update, context, library_id)
    
    elif data == "delete_client":
        await start_delete_client(update, context)
    
    elif data == "actual_hashes":
        await show_actual_hashes(update, context)
    
    elif data == "search_hash":
        await start_search_hash(update, context)
    
    elif data == "search_number":
        await start_search_number(update, context)
    
    elif data == "manage_notifications":
        await manage_notifications(update, context)
    
    elif data in ["confirm_delete_yes", "confirm_delete_no"]:
        await handle_delete_confirmation(update, context)
    
    # Обработка кнопок библиотеки
    elif data.startswith("view_hashes_"):
        library_id = int(data.split('_')[-1])
        await view_library_hashes(update, context, library_id)
    
    elif data.startswith("add_hash_"):
        library_id = int(data.split('_')[-1])
        await start_add_hash_to_library(update, context, library_id)
    
    # Заглушки для остальных функций
    elif any(data.startswith(prefix) for prefix in ["rename_lib_", "change_desc_", "search_in_lib_"]):
        await query.edit_message_text(
            "🛠 Функция в разработке!",
            reply_markup=create_main_menu()
        )
        
    elif data == "enable_notifications":
        context.application.bot_data['notifications_enabled'] = True
        
        # Запускаем индивидуальные jobs для всех существующих хешей
        conn = get_connection()
        cursor = conn.cursor()
        
        # Получаем текущее время
        current_time = datetime.now().strftime("%H:%M:%S")
        
        # Находим хеши, у которых время истекло более 60 минут назад
        cursor.execute("""
            SELECT id FROM hashes 
            WHERE time_text != '00:00:00' 
            AND time_text <= TIME(datetime('now', '-60 minutes'))
        """)
        expired_hashes = cursor.fetchall()
        conn.close()
        
        for (hash_id,) in expired_hashes:
            # Для просроченных хешей отправляем уведомление сразу
            context.application.job_queue.run_once(
                send_hash_notification, 
                when=1,  # через 1 секунду
                chat_id=query.message.chat_id,
                data=hash_id,
                name=f"hash_notification_{hash_id}"
            )
        
        # Для остальных хешей запускаем обычные таймеры
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM hashes WHERE time_text != '00:00:00'")
        all_hashes = cursor.fetchall()
        conn.close()
        
        expired_ids = [hash_id for (hash_id,) in expired_hashes]
        
        for (hash_id,) in all_hashes:
            if hash_id not in expired_ids:
                # Запускаем индивидуальный job для каждого хеша через 1 час
                context.application.job_queue.run_once(
                    send_hash_notification, 
                    when=3480,  # через 1 час = 3600 секунд
                    chat_id=query.message.chat_id,
                    data=hash_id,
                    name=f"hash_notification_{hash_id}"
                )
        
        await manage_notifications(update, context)
    
    elif data == "disable_notifications":
        context.application.bot_data['notifications_enabled'] = False
        
        # Останавливаем все индивидуальные jobs уведомлений
        current_jobs = context.application.job_queue.jobs()
        for job in current_jobs:
            if job.name and job.name.startswith("hash_notification_"):
                job.schedule_removal()
        
        # Очищаем отслеживание уведомлений
        sent_notifications.clear()
        
        await manage_notifications(update, context)
        
    elif data.startswith("complete_hash_"):
        await update_hash_time(update, context)

# ==================== ЗАПУСК БОТА ====================

def main():
    """Главная функция запуска бота"""
    print("🚀 Запускаю бота...")
    
    init_database()
    
#    token = "8250160966:AAEOa3o4MY2GGA47vLbSb7oLNJE8w3k5lv0"
    token = os.environ.get('BOT_TOKEN')
    
    # Создаем Application с JobQueue
    application = (
        Application.builder()
        .token(token)
        .build()
    )
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CallbackQueryHandler(handle_button_click))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Инициализируем данные бота
    application.bot_data['notifications_enabled'] = False
    
    print("✅ Бот запущен! Нажми Ctrl+C чтобы остановить")
    application.run_polling()

if __name__ == '__main__':
    main()