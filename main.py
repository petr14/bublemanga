import os
import time
import hmac
import hashlib
import urllib.parse
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request, session, redirect, url_for, Response, make_response
import threading
import sqlite3
import secrets
import json
import asyncio
import re
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice, WebAppInfo
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    PreCheckoutQueryHandler,
    filters
)
from functools import wraps
import logging
from flask_compress import Compress
from flask_socketio import SocketIO
from senkuro_api import SenkuroAPI


# Конфигурация логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Конфигурация
TELEGRAM_BOT_TOKEN = "7082209603:AAG97jX6MHgYOywy5hdDl03hduVMD6VBsW0"

# Список Telegram ID администраторов (заполни своим ID, узнать можно у @userinfobot)
ADMIN_TELEGRAM_IDS: list = [319026942,649144994]

COIN_PACKAGES = [
    {'id': 'coins_100',  'coins': 100,  'stars': 15,  'rub': 129,   'usd': '1.49', 'label': '100 шариков'},
    {'id': 'coins_300',  'coins': 300,  'stars': 40,  'rub': 329,   'usd': '3.99', 'label': '300 шариков'},
    {'id': 'coins_700',  'coins': 700,  'stars': 85,  'rub': 699,   'usd': '7.99', 'label': '700 шариков'},
    {'id': 'coins_1500', 'coins': 1500, 'stars': 175, 'rub': 1399,  'usd': '15.99', 'label': '1500 шариков'},
]

PREMIUM_PACKAGES = [
    {'id': 'premium_1m',  'days': 30,  'label': 'Premium на 1 месяц',  'rub': 199,  'usd': '2.49'},
    {'id': 'premium_3m',  'days': 90,  'label': 'Premium на 3 месяца', 'rub': 499,  'usd': '5.99'},
    {'id': 'premium_12m', 'days': 365, 'label': 'Premium на 1 год',    'rub': 1499, 'usd': '17.99'},
]

# ЮКасса (https://yookassa.ru → Настройки → Ключи API)
YOOKASSA_SHOP_ID  = os.environ.get('YOOKASSA_SHOP_ID', '')
YOOKASSA_SECRET   = os.environ.get('YOOKASSA_SECRET', '')

# Crypto Cloud (https://cryptocloud.plus → Проекты → API)
CRYPTOCLOUD_API_KEY    = os.environ.get('CRYPTOCLOUD_API_KEY', '')
CRYPTOCLOUD_SECRET_KEY = os.environ.get('CRYPTOCLOUD_SECRET_KEY', '')  # для верификации вебхука
CRYPTOCLOUD_SHOP_ID    = os.environ.get('CRYPTOCLOUD_SHOP_ID', '')

# Базовый URL сайта (для redirect после оплаты)
SITE_URL = 'http://bubblemanga.myftp.org'

app = Flask(__name__)

# Постоянный secret_key — читаем из файла, чтобы сессии не сбрасывались при рестарте
_SECRET_KEY_FILE = os.path.join(os.path.dirname(__file__), '.secret_key')
if os.path.exists(_SECRET_KEY_FILE):
    with open(_SECRET_KEY_FILE, 'r') as _f:
        app.secret_key = _f.read().strip()
else:
    app.secret_key = secrets.token_hex(32)
    with open(_SECRET_KEY_FILE, 'w') as _f:
        _f.write(app.secret_key)

app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=30)
app.config['COMPRESS_MIMETYPES'] = [
    'text/html', 'text/css', 'application/json',
    'application/javascript', 'text/javascript'
]
app.config['COMPRESS_LEVEL'] = 6
Compress(app)

socketio = SocketIO(
    app,
    async_mode='threading',
    cors_allowed_origins='*',
    logger=False,
    engineio_logger=False,
)

_TYPE_RU = {
    'MANGA': 'Манга', 'MANHWA': 'Манхва', 'MANHUA': 'Маньхуа',
    'OEL': 'Всемирная манга', 'NOVEL': 'Новелла', 'ONE_SHOT': 'Короткие истории',
    'DOUJINSHI': 'Додзинси', 'COMICS': 'Комикс',
}
_STATUS_RU = {
    'ONGOING': 'Выходит', 'FINISHED': 'Завершена', 'CANCELLED': 'Заброшена',
    'HIATUS': 'Пауза', 'ANNOUNCED': 'Анонс',
}
_RATING_RU = {
    'GENERAL': 'Для всех', 'SENSITIVE': '16+', 'QUESTIONABLE': '18+', 'EXPLICIT': 'Этти',
}

@app.template_filter('type_ru')
def filter_type_ru(v):
    return _TYPE_RU.get((v or '').upper(), v or '')

@app.template_filter('status_ru')
def filter_status_ru(v):
    return _STATUS_RU.get((v or '').upper(), v or '')

@app.template_filter('rating_ru')
def filter_rating_ru(v):
    return _RATING_RU.get((v or '').upper(), v or '')

telegram_app = None
_bot_loop = None  # event loop потока Telegram-бота (для run_coroutine_threadsafe)

# Клиент API Senkuro
api = SenkuroAPI()

# Словарь для отслеживания фоновой загрузки глав: manga_slug -> True
_manga_loading = {}

# ── In-memory кеш user_stats для /api/user/stats ─────────────────────────
_stats_cache: dict = {}          # {user_id: {'data': dict, 'expires': float}}
_stats_cache_lock = threading.Lock()

# ==================== БАЗА ДАННЫХ ====================

def init_db():
    """Инициализация базы данных"""
    conn = sqlite3.connect('manga.db', timeout=30)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    c = conn.cursor()
    
    # Таблица Манги
    c.execute('''CREATE TABLE IF NOT EXISTS manga (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        manga_id TEXT UNIQUE NOT NULL,
        manga_slug TEXT NOT NULL,
        manga_title TEXT NOT NULL,
        manga_type TEXT,
        manga_status TEXT,
        cover_url TEXT,
        last_chapter_id TEXT,
        last_chapter_number TEXT,
        last_chapter_volume TEXT,
        last_chapter_name TEXT,
        last_chapter_slug TEXT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        views INTEGER DEFAULT 0,
        rating TEXT DEFAULT 'GENERAL',
        branch_id TEXT,
        chapters_count INTEGER DEFAULT 0
    )''')

    # Добавляем недостающие колонки для полных данных манги (миграция)
    _manga_extra_cols = [
        ('description',        'TEXT DEFAULT ""'),
        ('score',              'REAL DEFAULT 0'),
        ('tags',               'TEXT DEFAULT "[]"'),
        ('original_name',      'TEXT DEFAULT ""'),
        ('translation_status', 'TEXT DEFAULT ""'),
        ('is_licensed',        'INTEGER DEFAULT 0'),
        ('formats',            'TEXT DEFAULT "[]"'),
    ]
    for col_name, col_def in _manga_extra_cols:
        try:
            c.execute(f'ALTER TABLE manga ADD COLUMN {col_name} {col_def}')
        except Exception:
            pass  # колонка уже существует

    c.execute('''CREATE TABLE IF NOT EXISTS chapters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        manga_id TEXT NOT NULL,
        chapter_id TEXT UNIQUE NOT NULL,
        chapter_slug TEXT NOT NULL,
        chapter_number TEXT,
        chapter_volume TEXT,
        chapter_name TEXT,
        chapter_url TEXT,
        pages_json TEXT,
        pages_count INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (manga_id) REFERENCES manga(manga_id)
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        telegram_id INTEGER UNIQUE NOT NULL,
        telegram_username TEXT,
        telegram_first_name TEXT,
        telegram_last_name TEXT,
        login_token TEXT UNIQUE,
        is_active BOOLEAN DEFAULT 1,
        notifications_enabled BOOLEAN DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_login TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS subscriptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        manga_id TEXT NOT NULL,
        subscribed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (manga_id) REFERENCES manga(manga_id),
        UNIQUE(user_id, manga_id)
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS reading_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        manga_id TEXT NOT NULL,
        chapter_id TEXT NOT NULL,
        page_number INTEGER DEFAULT 1,
        last_read TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (manga_id) REFERENCES manga(manga_id),
        FOREIGN KEY (chapter_id) REFERENCES chapters(chapter_id),
        UNIQUE(user_id, manga_id)
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS search_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        query TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )''')
    
    # Таблица для кеширования спотлайтов
    c.execute('''CREATE TABLE IF NOT EXISTS cache (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # ── Геймификация ───────────────────────────────────────────────────────

    # Статистика и валюта пользователя
    c.execute('''CREATE TABLE IF NOT EXISTS user_stats (
        user_id INTEGER PRIMARY KEY,
        xp INTEGER DEFAULT 0,
        coins INTEGER DEFAULT 0,
        level INTEGER DEFAULT 1,
        total_chapters_read INTEGER DEFAULT 0,
        total_pages_read INTEGER DEFAULT 0,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )''')

    # Каталог ачивок
    c.execute('''CREATE TABLE IF NOT EXISTS achievements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        key TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        icon TEXT DEFAULT '🏆',
        xp_reward INTEGER DEFAULT 0,
        condition_type TEXT NOT NULL,
        condition_value INTEGER NOT NULL
    )''')

    # Выданные ачивки
    c.execute('''CREATE TABLE IF NOT EXISTS user_achievements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        achievement_id INTEGER NOT NULL,
        unlocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (achievement_id) REFERENCES achievements(id),
        UNIQUE(user_id, achievement_id)
    )''')

    # Товары магазина
    c.execute('''CREATE TABLE IF NOT EXISTS shop_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT,
        type TEXT NOT NULL,
        preview_url TEXT,
        css_value TEXT,
        price INTEGER DEFAULT 0,
        is_upload INTEGER DEFAULT 0
    )''')

    # Покупки пользователей
    c.execute('''CREATE TABLE IF NOT EXISTS user_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        item_id INTEGER NOT NULL,
        purchased_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_equipped INTEGER DEFAULT 0,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (item_id) REFERENCES shop_items(id),
        UNIQUE(user_id, item_id)
    )''')

    # Профиль пользователя (оформление)
    c.execute('''CREATE TABLE IF NOT EXISTS user_profile (
        user_id INTEGER PRIMARY KEY,
        avatar_url TEXT,
        background_url TEXT,
        frame_item_id INTEGER,
        badge_item_id INTEGER,
        title_item_id INTEGER,
        bio TEXT DEFAULT '',
        FOREIGN KEY (user_id) REFERENCES users(id)
    )''')

    # Лог начисления XP (для антиспама)
    c.execute('''CREATE TABLE IF NOT EXISTS xp_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        reason TEXT NOT NULL,
        ref_id TEXT,
        amount INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    # Коллекции пользователей
    c.execute('''CREATE TABLE IF NOT EXISTS collections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        description TEXT DEFAULT '',
        cover_url TEXT,
        is_public INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS collection_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        collection_id INTEGER NOT NULL,
        manga_id TEXT NOT NULL,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE,
        UNIQUE(collection_id, manga_id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS collection_likes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        collection_id INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, collection_id)
    )''')

    # Очередь уведомлений для не-премиум пользователей (ежедневный дайджест)
    c.execute('''CREATE TABLE IF NOT EXISTS notification_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        manga_id TEXT NOT NULL,
        manga_title TEXT NOT NULL,
        manga_slug TEXT NOT NULL,
        chapter_number TEXT,
        chapter_volume TEXT,
        chapter_name TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, manga_id, chapter_number)
    )''')

    # Миграция: добавить колонку last_digest_date в users
    try:
        c.execute('ALTER TABLE users ADD COLUMN last_digest_date TEXT')
        conn.commit()
    except Exception:
        pass  # Колонка уже существует

    # Все прочитанные главы (одна запись на главу, в отличие от reading_history)
    c.execute('''CREATE TABLE IF NOT EXISTS chapters_read (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        chapter_id TEXT NOT NULL,
        manga_id TEXT NOT NULL,
        read_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id),
        UNIQUE(user_id, chapter_id)
    )''')

    # Миграции: новые колонки
    _migrations = [
        ("ALTER TABLE user_profile ADD COLUMN custom_name TEXT DEFAULT ''",),
        ("ALTER TABLE user_profile ADD COLUMN custom_avatar_url TEXT",),
        ("ALTER TABLE shop_items ADD COLUMN is_animated INTEGER DEFAULT 0",),
        ("ALTER TABLE users ADD COLUMN is_premium INTEGER DEFAULT 0",),
        ("ALTER TABLE users ADD COLUMN premium_expires_at TEXT DEFAULT NULL",),
        ("ALTER TABLE user_items ADD COLUMN is_premium_loan INTEGER DEFAULT 0",),
        ("ALTER TABLE achievements ADD COLUMN icon_url TEXT",),
        ("ALTER TABLE coin_purchases ADD COLUMN payment_method TEXT DEFAULT 'stars'",),
        ("ALTER TABLE user_profile ADD COLUMN name_change_count INTEGER DEFAULT 0",),
        ("ALTER TABLE user_profile ADD COLUMN name_change_month TEXT DEFAULT NULL",),
    ]
    for (sql,) in _migrations:
        try:
            c.execute(sql)
            conn.commit()
        except Exception:
            pass

    # Покупки монет за Stars
    c.execute('''CREATE TABLE IF NOT EXISTS coin_purchases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL REFERENCES users(id),
        package_id TEXT NOT NULL,
        stars_paid INTEGER NOT NULL,
        coins_received INTEGER NOT NULL,
        payment_id TEXT UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    # Покупки Premium подписки
    c.execute('''CREATE TABLE IF NOT EXISTS premium_purchases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL REFERENCES users(id),
        package_id TEXT NOT NULL,
        payment_id TEXT UNIQUE NOT NULL,
        payment_method TEXT DEFAULT 'yookassa',
        expires_at TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    # Задания (квесты)
    c.execute('''CREATE TABLE IF NOT EXISTS quests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        description TEXT DEFAULT '',
        icon TEXT DEFAULT '📋',
        icon_url TEXT,
        required_level INTEGER NOT NULL DEFAULT 1,
        condition_type TEXT NOT NULL,
        condition_value INTEGER NOT NULL DEFAULT 1,
        xp_reward INTEGER DEFAULT 0,
        coins_reward INTEGER DEFAULT 0,
        is_active INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    # Прогресс заданий пользователей
    c.execute('''CREATE TABLE IF NOT EXISTS user_quests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        quest_id INTEGER NOT NULL,
        progress INTEGER DEFAULT 0,
        completed_at TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (quest_id) REFERENCES quests(id),
        UNIQUE(user_id, quest_id)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_user_quests ON user_quests(user_id)')

    # Дедупликация квестов (на случай если уже накопились дубли от предыдущих запусков)
    c.execute('''
        DELETE FROM user_quests
        WHERE quest_id NOT IN (SELECT MIN(id) FROM quests GROUP BY title)
    ''')
    c.execute('''
        DELETE FROM quests
        WHERE id NOT IN (SELECT MIN(id) FROM quests GROUP BY title)
    ''')
    conn.commit()

    # UNIQUE индекс по title — предотвращает будущие дубли
    try:
        c.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_quests_title ON quests(title)')
        conn.commit()
    except Exception:
        pass

    # Seed: базовые задания по уровням
    QUESTS_SEED = [
        # (title, description, icon, required_level, condition_type, condition_value, xp_reward, coins_reward)
        ('Первый комментарий',   'Напиши свой первый комментарий к манге',    '💬', 1,  'comments_posted', 1,   50,  20),
        ('Начало пути',          'Прочитай 5 глав любой манги',               '📖', 1,  'chapters_read',   5,   75,  30),
        ('Подписчик',            'Подпишись на 1 мангу',                      '❤️', 2,  'subscriptions',   1,   100, 50),
        ('Активный читатель',    'Прочитай 20 глав',                          '📚', 2,  'chapters_read',   20,  150, 60),
        ('Болтун',               'Напиши 5 комментариев',                     '🗣️', 3,  'comments_posted', 5,   200, 80),
        ('Библиотека',           'Подпишись на 3 манги',                      '🗂️', 3,  'subscriptions',   3,   200, 80),
        ('Профи',                'Прочитай 100 глав',                         '🔥', 5,  'chapters_read',   100, 500, 200),
        ('Завсегдатай',          'Подпишись на 10 манг',                      '💎', 5,  'subscriptions',   10,  400, 150),
        ('Ветеран',              'Прочитай 500 глав',                         '🏆', 10, 'chapters_read',   500, 1000, 500),
        ('Великий комментатор',  'Напиши 50 комментариев',                    '👑', 10, 'comments_posted', 50,  800, 300),
    ]
    c.executemany(
        '''INSERT OR IGNORE INTO quests
           (title, description, icon, required_level, condition_type, condition_value, xp_reward, coins_reward)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
        QUESTS_SEED
    )

    # ── Индексы ────────────────────────────────────────────────────────────
    c.execute('CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_users_login_token ON users(login_token)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_manga_slug ON manga(manga_slug)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_subscriptions_user ON subscriptions(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_subscriptions_manga ON subscriptions(manga_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_search_user ON search_history(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_user_stats ON user_stats(xp DESC)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_xp_log ON xp_log(user_id, ref_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_chapters_read_user_manga ON chapters_read(user_id, manga_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_collections_user ON collections(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_collection_items ON collection_items(collection_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_collection_likes ON collection_likes(collection_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_collection_likes_user ON collection_likes(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_notification_queue_user ON notification_queue(user_id)')

    # Комментарии к манге
    c.execute('''CREATE TABLE IF NOT EXISTS comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        manga_slug TEXT NOT NULL,
        user_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_comments_manga ON comments(manga_slug, created_at DESC)')

    # ── Seed: ачивки ───────────────────────────────────────────────────────
    ACHIEVEMENTS = [
        ('first_chapter',  'Первый шаг',        'Прочитать первую главу',          '📖', 50,   'chapters_read', 1),
        ('reader_10',      'Читатель',           'Прочитать 10 глав',               '📚', 100,  'chapters_read', 10),
        ('reader_50',      'Книголюб',           'Прочитать 50 глав',               '🔖', 200,  'chapters_read', 50),
        ('reader_100',     'Книгочей',           'Прочитать 100 глав',              '🎓', 500,  'chapters_read', 100),
        ('reader_500',     'Запойный читатель',  'Прочитать 500 глав',              '🌟', 1000, 'chapters_read', 500),
        ('reader_1000',    'Маньяк чтения',      'Прочитать 1000 глав',             '👑', 2000, 'chapters_read', 1000),
        ('subscriber_1',   'Фанат',              'Подписаться на 1 мангу',          '❤️', 50,   'subscriptions', 1),
        ('subscriber_5',   'Следопыт',           'Подписаться на 5 манг',           '💫', 150,  'subscriptions', 5),
        ('subscriber_10',  'Коллекционер',       'Подписаться на 10 манг',          '💎', 300,  'subscriptions', 10),
        ('level_5',        'Опытный',            'Достичь 5 уровня',                '⚡', 0,    'level',         5),
        ('level_10',       'Бывалый',            'Достичь 10 уровня',               '🔥', 0,    'level',         10),
        ('level_20',       'Ветеран',            'Достичь 20 уровня',               '🏆', 0,    'level',         20),
        ('level_50',       'Легенда',            'Достичь 50 уровня',               '🌈', 0,    'level',         50),
    ]
    c.executemany(
        '''INSERT OR IGNORE INTO achievements
           (key, name, description, icon, xp_reward, condition_type, condition_value)
           VALUES (?, ?, ?, ?, ?, ?, ?)''',
        ACHIEVEMENTS
    )

    # ── Seed: товары магазина ──────────────────────────────────────────────
    SHOP_ITEMS = [
        # Рамки профиля
        ('Золотая рамка',    'Роскошная золотая рамка для аватара',   'frame',      None, 'border: 3px solid #FFD700; box-shadow: 0 0 12px #FFD700;',       500,  0),
        ('Неоновая рамка',   'Ярко-фиолетовая неоновая рамка',        'frame',      None, 'border: 3px solid #a855f7; box-shadow: 0 0 16px #a855f7;',       1000, 0),
        ('Радужная рамка',   'Переливающаяся RGB рамка',              'frame',      None, 'border: 3px solid transparent; background: linear-gradient(#141414,#141414) padding-box, linear-gradient(135deg,#f43f5e,#a855f7,#3b82f6) border-box;', 2000, 0),
        ('Аниме рамка',      'Рамка в стиле аниме с сакурой',         'frame',      None, 'border: 3px solid #ec4899; box-shadow: 0 0 12px #ec4899;',        750,  0),
        # Фоны профиля
        ('Ночной город',     'Тёмный городской пейзаж',               'background', None, 'background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);', 300,  0),
        ('Сакура',           'Нежно-розовый цветочный фон',           'background', None, 'background: linear-gradient(135deg, #f8b4d9, #f093fb, #f5576c);', 300,  0),
        ('Космос',           'Звёздное небо',                         'background', None, 'background: linear-gradient(135deg, #0d0d1a, #1a1a3e, #0d0d1a); background-size:400% 400%;', 500, 0),
        ('Океан',            'Глубокий океанский градиент',           'background', None, 'background: linear-gradient(135deg, #001f3f, #0074D9, #7FDBFF);', 400,  0),
        # Значки
        ('VIP',              'Эксклюзивный VIP значок',               'badge',      None, '👑 VIP',                                                          2000, 0),
        ('Отаку',            'Значок настоящего отаку',               'badge',      None, '🎌 Отаку',                                                        800,  0),
        ('Манга-гуру',       'Для тех, кто знает толк',               'badge',      None, '📖 Манга-гуру',                                                   1500, 0),
        # Слоты загрузки
        ('Загрузка аватара', 'Разблокировать загрузку своего аватара','avatar_slot', None, None,                                                              0,    1),
        ('Загрузка фона',    'Разблокировать загрузку своего фона',   'bg_slot',    None, None,                                                              500,  1),
    ]
    c.executemany(
        '''INSERT OR IGNORE INTO shop_items
           (name, description, type, preview_url, css_value, price, is_upload)
           VALUES (?, ?, ?, ?, ?, ?, ?)''',
        SHOP_ITEMS
    )

    # ── Миграция: Premium поля ─────────────────────────────────────────────
    try:
        c.execute('ALTER TABLE users ADD COLUMN is_premium INTEGER DEFAULT 0')
        conn.commit()
    except sqlite3.OperationalError:
        pass  # колонка уже существует
    try:
        c.execute('ALTER TABLE users ADD COLUMN premium_granted_at TIMESTAMP')
        conn.commit()
    except sqlite3.OperationalError:
        pass
    try:
        c.execute('ALTER TABLE users ADD COLUMN premium_expires_at TIMESTAMP')
        conn.commit()
    except sqlite3.OperationalError:
        pass
    try:
        c.execute('ALTER TABLE user_items ADD COLUMN is_premium_loan INTEGER DEFAULT 0')
        conn.commit()
    except sqlite3.OperationalError:
        pass
    try:
        c.execute('ALTER TABLE comments ADD COLUMN parent_id INTEGER REFERENCES comments(id)')
        conn.commit()
    except sqlite3.OperationalError:
        pass

    # ── Новые миграции: геймификация + социальные функции ─────────────────────

    # Стрик чтения
    for _sql in [
        'ALTER TABLE user_stats ADD COLUMN reading_streak INTEGER DEFAULT 0',
        'ALTER TABLE user_stats ADD COLUMN max_streak INTEGER DEFAULT 0',
        'ALTER TABLE user_stats ADD COLUMN last_read_date TEXT DEFAULT NULL',
        'ALTER TABLE users ADD COLUMN referral_code TEXT',
    ]:
        try:
            c.execute(_sql)
            conn.commit()
        except Exception:
            pass

    # UNIQUE индекс на referral_code
    try:
        c.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code)')
        conn.commit()
    except Exception:
        pass

    # Ежедневные задания (шаблоны)
    c.execute('''CREATE TABLE IF NOT EXISTS daily_quests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        description TEXT,
        icon TEXT DEFAULT '📅',
        condition_type TEXT NOT NULL,
        condition_value INTEGER NOT NULL,
        xp_reward INTEGER DEFAULT 0,
        coins_reward INTEGER DEFAULT 0,
        is_active INTEGER DEFAULT 1
    )''')

    # Прогресс пользователя по дневным заданиям
    c.execute('''CREATE TABLE IF NOT EXISTS user_daily_quests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        quest_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        progress INTEGER DEFAULT 0,
        completed_at TEXT DEFAULT NULL,
        UNIQUE(user_id, quest_id, date),
        FOREIGN KEY (user_id) REFERENCES users(id)
    )''')

    # Сезонные ивенты
    c.execute('''CREATE TABLE IF NOT EXISTS seasons (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT,
        icon TEXT DEFAULT '🌸',
        banner_url TEXT,
        starts_at TEXT NOT NULL,
        ends_at TEXT NOT NULL,
        is_active INTEGER DEFAULT 1
    )''')

    # Задания сезона
    c.execute('''CREATE TABLE IF NOT EXISTS season_quests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        season_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        icon TEXT DEFAULT '✨',
        condition_type TEXT NOT NULL,
        condition_value INTEGER NOT NULL,
        xp_reward INTEGER DEFAULT 0,
        coins_reward INTEGER DEFAULT 0,
        item_reward_id INTEGER DEFAULT NULL,
        FOREIGN KEY (season_id) REFERENCES seasons(id)
    )''')

    # Прогресс пользователя по сезонным заданиям
    c.execute('''CREATE TABLE IF NOT EXISTS user_season_quests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        season_quest_id INTEGER NOT NULL,
        progress INTEGER DEFAULT 0,
        completed_at TEXT DEFAULT NULL,
        UNIQUE(user_id, season_quest_id),
        FOREIGN KEY (user_id) REFERENCES users(id)
    )''')

    # Лайки на комментарии
    c.execute('''CREATE TABLE IF NOT EXISTS comment_likes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        comment_id INTEGER NOT NULL,
        UNIQUE(user_id, comment_id),
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (comment_id) REFERENCES comments(id)
    )''')

    # Список "Хочу прочитать"
    c.execute('''CREATE TABLE IF NOT EXISTS reading_wishlist (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        manga_id TEXT NOT NULL,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, manga_id),
        FOREIGN KEY (user_id) REFERENCES users(id)
    )''')

    # Рефералы
    c.execute('''CREATE TABLE IF NOT EXISTS referrals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referrer_id INTEGER NOT NULL,
        referred_id INTEGER NOT NULL UNIQUE,
        rewarded INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (referrer_id) REFERENCES users(id),
        FOREIGN KEY (referred_id) REFERENCES users(id)
    )''')

    # Трофеи "Коллекция недели"
    c.execute('''CREATE TABLE IF NOT EXISTS collection_trophies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        collection_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        iso_week TEXT NOT NULL,
        likes_count INTEGER NOT NULL,
        awarded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(iso_week)
    )''')

    # Новые индексы
    c.execute('CREATE INDEX IF NOT EXISTS idx_user_daily_quests ON user_daily_quests(user_id, date)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_user_season_quests ON user_season_quests(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_comment_likes_comment ON comment_likes(comment_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_wishlist_user ON reading_wishlist(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_collection_trophies_week ON collection_trophies(iso_week)')

    # Seed: ежедневные задания
    DAILY_QUESTS_SEED = [
        ('Читатель дня',   'Прочитай 3 главы сегодня',          '📖', 'chapters_today',  3,  50,  20),
        ('Комментатор',    'Оставь 1 комментарий сегодня',      '💬', 'comments_today',  1,  30,  10),
        ('Исследователь',  'Открой 2 разные манги сегодня',     '🔍', 'manga_today',     2,  30,  10),
    ]
    c.executemany(
        '''INSERT OR IGNORE INTO daily_quests
           (title, description, icon, condition_type, condition_value, xp_reward, coins_reward)
           VALUES (?, ?, ?, ?, ?, ?, ?)''',
        DAILY_QUESTS_SEED
    )

    # Seed: начальный сезон Spring 2026
    c.execute('''INSERT OR IGNORE INTO seasons (id, name, description, icon, starts_at, ends_at, is_active)
                 VALUES (1, 'Весна 2026', 'Сезон цветения — читай, комментируй и побеждай!',
                         '🌸', '2026-03-01', '2026-04-30', 1)''')

    # Эксклюзивный предмет сезона
    c.execute('''INSERT OR IGNORE INTO shop_items (id, name, description, type, preview_url, css_value, price, is_upload)
                 VALUES (100, 'Рамка «Весна»', 'Сезонная рамка Spring 2026',
                         'frame', NULL,
                         'border: 3px solid #ec4899; box-shadow: 0 0 14px #f9a8d4;',
                         0, 0)''')

    # Задания сезона
    SEASON_QUESTS_SEED = [
        (1, 'Весенний читатель',    'Прочитай 50 глав за сезон',    '📚', 'chapters_read',   50,  200, 100, None),
        (1, 'Болтун сезона',        'Оставь 10 комментариев',       '💬', 'comments_posted', 10,  150, 50,  None),
        (1, 'Коллекционер весны',   'Прочитай 100 глав за сезон',   '🌸', 'chapters_read',   100, 500, 200, 100),
    ]
    for sq in SEASON_QUESTS_SEED:
        c.execute('''INSERT OR IGNORE INTO season_quests
                     (season_id, title, description, icon, condition_type, condition_value,
                      xp_reward, coins_reward, item_reward_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', sq)

    # ── Уведомления на сайте ──────────────────────────────────────────────────
    c.execute('''CREATE TABLE IF NOT EXISTS site_notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        type TEXT NOT NULL,
        title TEXT NOT NULL,
        body TEXT,
        url TEXT,
        ref_id TEXT,
        is_read INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_site_notifications ON site_notifications(user_id, is_read)')

    # Предпочитаемый час дайджеста (МСК)
    try:
        c.execute('ALTER TABLE users ADD COLUMN digest_hour INTEGER DEFAULT 22')
        conn.commit()
    except Exception:
        pass

    # ── Новые миграции: монетизация ───────────────────────────────────────────
    for _sql in [
        'ALTER TABLE shop_items ADD COLUMN duration_days INTEGER DEFAULT NULL',
        'ALTER TABLE user_items ADD COLUMN expires_at TIMESTAMP DEFAULT NULL',
    ]:
        try:
            c.execute(_sql)
            conn.commit()
        except Exception:
            pass

    # Подарочный Premium
    c.execute('''CREATE TABLE IF NOT EXISTS premium_gifts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sender_id INTEGER NOT NULL,
        recipient_id INTEGER NOT NULL,
        days INTEGER NOT NULL,
        stars_paid INTEGER NOT NULL,
        payment_id TEXT UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (sender_id) REFERENCES users(id),
        FOREIGN KEY (recipient_id) REFERENCES users(id)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_premium_gifts_recipient ON premium_gifts(recipient_id)')

    # Подписка на кураторов
    c.execute('''CREATE TABLE IF NOT EXISTS curator_follows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        follower_id INTEGER NOT NULL,
        author_id INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(follower_id, author_id),
        FOREIGN KEY (follower_id) REFERENCES users(id),
        FOREIGN KEY (author_id) REFERENCES users(id)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_curator_follows ON curator_follows(author_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_curator_follows_follower ON curator_follows(follower_id)')

    conn.commit()
    conn.close()
    print("✅ База данных инициализирована")

def get_db():
    conn = sqlite3.connect('manga.db', timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    return conn


def create_site_notification(user_id, notif_type, title, body=None, url=None, ref_id=None, conn=None):
    """Создать уведомление на сайте. Если conn передан — не коммитит и не закрывает."""
    _close = conn is None
    if conn is None:
        conn = get_db()
    try:
        conn.execute(
            'INSERT INTO site_notifications (user_id, type, title, body, url, ref_id) VALUES (?,?,?,?,?,?)',
            (user_id, notif_type, title, body, url, ref_id)
        )
        if _close:
            conn.commit()
    except Exception as _e:
        logger.warning(f"create_site_notification error: {_e}")
    finally:
        if _close:
            conn.close()

# ── Контекст-процессор: данные пользователя для хедера ──────────────────────
@app.context_processor
def inject_g_user():
    user_id = session.get('user_id')
    if not user_id:
        return {'g_user': None}
    conn = None
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('''
            SELECT u.id, u.telegram_username, u.telegram_first_name,
                   up.custom_name, up.avatar_url, up.custom_avatar_url
            FROM users u
            LEFT JOIN user_profile up ON up.user_id = u.id
            WHERE u.id = ?
        ''', (user_id,))
        row = c.fetchone()
        if not row:
            return {'g_user': {'id': user_id, 'display_name': session.get('username', f'#{user_id}'), 'avatar_url': None}}
        display_name = (
            (row['custom_name'] or '').strip() or
            (row['telegram_first_name'] or '').strip() or
            (row['telegram_username'] or '').strip() or
            f'#{user_id}'
        )
        avatar = row['custom_avatar_url'] or row['avatar_url'] or None
        return {'g_user': {'id': row['id'], 'display_name': display_name, 'avatar_url': avatar}}
    except Exception as e:
        logger.warning(f'inject_g_user error: {e}')
        return {'g_user': {'id': user_id, 'display_name': session.get('username', f'#{user_id}'), 'avatar_url': None}}
    finally:
        if conn:
            conn.close()

# ==================== ГЕЙМИФИКАЦИЯ: XP / УРОВНИ / АЧИВКИ ====================

import math
from werkzeug.utils import secure_filename

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'static', 'uploads')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'webp'}


def _allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def get_level_from_xp(xp):
    """Вычислить уровень по количеству XP (формула: floor(sqrt(xp/100)) + 1)"""
    return max(1, int(math.floor(math.sqrt(max(0, xp) / 100))) + 1)


def get_xp_for_level(level):
    """XP, необходимый для достижения указанного уровня"""
    return (level - 1) ** 2 * 100


def get_or_create_user_stats(user_id, conn=None):
    """Получить или создать запись статистики пользователя"""
    close = conn is None
    if conn is None:
        conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM user_stats WHERE user_id = ?', (user_id,))
    stats = c.fetchone()
    if not stats:
        c.execute('INSERT OR IGNORE INTO user_stats (user_id) VALUES (?)', (user_id,))
        conn.commit()
        c.execute('SELECT * FROM user_stats WHERE user_id = ?', (user_id,))
        stats = c.fetchone()
    if close:
        conn.close()
    return dict(stats) if stats else None


def award_xp(user_id, amount, reason, ref_id=None):
    """
    Начислить XP и монеты пользователю.

    Args:
        user_id: ID пользователя
        amount: количество XP
        reason: причина (строка для лога)
        ref_id: ID связанного объекта (chapter_id и т.п.) для антиспама

    Returns:
        dict: {'xp': new_xp, 'level': new_level, 'leveled_up': bool, 'achievements': [...]}
    """
    if not user_id or amount <= 0:
        return None

    conn = get_db()
    c = conn.cursor()

    # Антиспам: не начислять XP дважды за один и тот же ref_id
    if ref_id:
        c.execute(
            'SELECT id FROM xp_log WHERE user_id = ? AND ref_id = ? AND reason = ? '
            'AND created_at > datetime("now", "-1 hour")',
            (user_id, str(ref_id), reason)
        )
        if c.fetchone():
            conn.close()
            return None

    # Создаём запись статистики если нет
    c.execute('INSERT OR IGNORE INTO user_stats (user_id) VALUES (?)', (user_id,))

    # Текущий XP
    c.execute('SELECT xp, level FROM user_stats WHERE user_id = ?', (user_id,))
    row = c.fetchone()
    old_xp = row['xp'] if row else 0
    old_level = row['level'] if row else 1

    new_xp = old_xp + amount
    new_level = get_level_from_xp(new_xp)

    # Обновляем статистику
    c.execute(
        '''UPDATE user_stats SET xp = ?, coins = coins + ?, level = ? WHERE user_id = ?''',
        (new_xp, amount, new_level, user_id)
    )

    # Лог начисления
    c.execute(
        'INSERT INTO xp_log (user_id, reason, ref_id, amount) VALUES (?, ?, ?, ?)',
        (user_id, reason, str(ref_id) if ref_id else None, amount)
    )

    conn.commit()

    # Уведомление о новом уровне
    if new_level > old_level:
        create_site_notification(user_id, 'level_up', f'Уровень {new_level}!',
                                 f'Поздравляем с {new_level} уровнем!',
                                 f'/profile/{user_id}', conn=conn)

    # Проверяем ачивки и квесты
    new_achievements = check_achievements(user_id, conn)
    check_quests(user_id, conn)

    conn.close()

    # Инвалидируем кеш статистики
    with _stats_cache_lock:
        _stats_cache.pop(user_id, None)

    return {
        'xp': new_xp,
        'level': new_level,
        'leveled_up': new_level > old_level,
        'achievements': new_achievements
    }


def check_achievements(user_id, conn=None):
    """
    Проверить и выдать новые ачивки пользователю.

    Returns:
        list[dict]: список только что выданных ачивок
    """
    close = conn is None
    if conn is None:
        conn = get_db()
    c = conn.cursor()

    # Получаем текущую статистику
    c.execute('SELECT * FROM user_stats WHERE user_id = ?', (user_id,))
    stats = c.fetchone()
    if not stats:
        if close:
            conn.close()
        return []

    # Количество подписок
    c.execute('SELECT COUNT(*) as cnt FROM subscriptions WHERE user_id = ?', (user_id,))
    sub_count = c.fetchone()['cnt']

    stat_values = {
        'chapters_read': stats['total_chapters_read'],
        'subscriptions': sub_count,
        'level': stats['level'],
    }

    # Все ачивки которых у пользователя ещё нет
    c.execute(
        '''SELECT a.* FROM achievements a
           WHERE a.id NOT IN (
               SELECT achievement_id FROM user_achievements WHERE user_id = ?
           )''',
        (user_id,)
    )
    pending = c.fetchall()

    unlocked = []
    for ach in pending:
        val = stat_values.get(ach['condition_type'], 0)
        if val >= ach['condition_value']:
            c.execute(
                'INSERT OR IGNORE INTO user_achievements (user_id, achievement_id) VALUES (?, ?)',
                (user_id, ach['id'])
            )
            # Бонус XP за ачивку (без рекурсии и без антиспама)
            if ach['xp_reward'] > 0:
                c.execute(
                    'UPDATE user_stats SET xp = xp + ?, coins = coins + ? WHERE user_id = ?',
                    (ach['xp_reward'], ach['xp_reward'], user_id)
                )
            unlocked.append(dict(ach))
            create_site_notification(user_id, 'achievement', f'Достижение: {ach["name"]}',
                                     ach.get('description'),
                                     f'/profile/{user_id}', conn=conn)

    conn.commit()
    if close:
        conn.close()
    return unlocked


def check_quests(user_id, conn=None):
    """
    Проверить и обновить прогресс заданий пользователя.
    Возвращает список только что завершённых заданий.
    """
    close = conn is None
    if conn is None:
        conn = get_db()
    c = conn.cursor()

    c.execute('SELECT * FROM user_stats WHERE user_id = ?', (user_id,))
    stats = c.fetchone()
    if not stats:
        if close:
            conn.close()
        return []

    c.execute('SELECT COUNT(*) as cnt FROM comments WHERE user_id = ?', (user_id,))
    comments_cnt = c.fetchone()['cnt']
    c.execute('SELECT COUNT(*) as cnt FROM subscriptions WHERE user_id = ?', (user_id,))
    subs_cnt = c.fetchone()['cnt']

    stat_values = {
        'chapters_read':   stats['total_chapters_read'],
        'subscriptions':   subs_cnt,
        'level':           stats['level'],
        'comments_posted': comments_cnt,
    }
    current_level = stats['level']

    # Активные квесты доступные пользователю (required_level <= current_level)
    c.execute(
        'SELECT * FROM quests WHERE is_active = 1 AND required_level <= ?',
        (current_level,)
    )
    all_quests = c.fetchall()

    just_completed = []
    for q in all_quests:
        current_progress = stat_values.get(q['condition_type'], 0)
        c.execute(
            'SELECT progress, completed_at FROM user_quests WHERE user_id = ? AND quest_id = ?',
            (user_id, q['id'])
        )
        uq = c.fetchone()
        already_done = uq and uq['completed_at'] is not None

        if already_done:
            continue

        # Upsert progress
        c.execute(
            '''INSERT INTO user_quests (user_id, quest_id, progress)
               VALUES (?, ?, ?)
               ON CONFLICT(user_id, quest_id) DO UPDATE SET progress = excluded.progress''',
            (user_id, q['id'], min(current_progress, q['condition_value']))
        )

        if current_progress >= q['condition_value']:
            c.execute(
                '''UPDATE user_quests SET completed_at = datetime("now"), progress = ?
                   WHERE user_id = ? AND quest_id = ?''',
                (q['condition_value'], user_id, q['id'])
            )
            # Выдаём награды
            if q['xp_reward'] > 0 or q['coins_reward'] > 0:
                c.execute(
                    'UPDATE user_stats SET xp = xp + ?, coins = coins + ? WHERE user_id = ?',
                    (q['xp_reward'], q['coins_reward'], user_id)
                )
                c.execute(
                    'INSERT INTO xp_log (user_id, reason, ref_id, amount) VALUES (?, ?, ?, ?)',
                    (user_id, f'quest:{q["id"]}', str(q['id']), q['xp_reward'])
                )
            just_completed.append(dict(q))
            create_site_notification(user_id, 'quest', f'Задание выполнено: {q["title"]}',
                                     f'+{q["xp_reward"]} XP',
                                     f'/profile/{user_id}', conn=conn)

    conn.commit()
    if close:
        conn.close()
    return just_completed


def get_user_full_profile(user_id):
    """Получить полный профиль пользователя для страницы профиля"""
    conn = get_db()
    c = conn.cursor()

    c.execute('SELECT * FROM users WHERE id = ?', (user_id,))
    user = c.fetchone()
    if not user:
        conn.close()
        return None

    # Статистика
    c.execute('SELECT * FROM user_stats WHERE user_id = ?', (user_id,))
    stats = c.fetchone()
    if not stats:
        c.execute('INSERT OR IGNORE INTO user_stats (user_id) VALUES (?)', (user_id,))
        conn.commit()
        c.execute('SELECT * FROM user_stats WHERE user_id = ?', (user_id,))
        stats = c.fetchone()

    # Профиль оформления
    c.execute('SELECT * FROM user_profile WHERE user_id = ?', (user_id,))
    profile = c.fetchone()
    if not profile:
        c.execute('INSERT OR IGNORE INTO user_profile (user_id) VALUES (?)', (user_id,))
        conn.commit()
        c.execute('SELECT * FROM user_profile WHERE user_id = ?', (user_id,))
        profile = c.fetchone()

    # Ачивки
    c.execute(
        '''SELECT a.*, ua.unlocked_at FROM achievements a
           JOIN user_achievements ua ON a.id = ua.achievement_id
           WHERE ua.user_id = ?
           ORDER BY ua.unlocked_at DESC''',
        (user_id,)
    )
    achievements = [dict(row) for row in c.fetchall()]

    # Купленные и надетые товары
    c.execute(
        '''SELECT si.*, ui.is_equipped, ui.purchased_at FROM shop_items si
           JOIN user_items ui ON si.id = ui.item_id
           WHERE ui.user_id = ?''',
        (user_id,)
    )
    items = [dict(row) for row in c.fetchall()]

    # История чтения (последние 10)
    c.execute(
        '''SELECT m.manga_title, m.manga_slug, m.cover_url,
                  c.chapter_number, c.chapter_slug, rh.last_read
           FROM reading_history rh
           JOIN manga m ON rh.manga_id = m.manga_id
           JOIN chapters c ON rh.chapter_id = c.chapter_id
           WHERE rh.user_id = ?
           ORDER BY rh.last_read DESC
           LIMIT 10''',
        (user_id,)
    )
    history = [dict(row) for row in c.fetchall()]

    # Вишлист
    c.execute(
        '''SELECT m.manga_id, m.manga_slug, m.manga_title, m.cover_url, m.manga_type, m.manga_status, rw.added_at
           FROM reading_wishlist rw
           JOIN manga m ON rw.manga_id = m.manga_id
           WHERE rw.user_id = ?
           ORDER BY rw.added_at DESC''',
        (user_id,)
    )
    wishlist = [dict(row) for row in c.fetchall()]

    # Количество подписчиков (как куратора)
    c.execute('SELECT COUNT(*) FROM curator_follows WHERE author_id=?', (user_id,))
    followers_count = c.fetchone()[0]

    conn.close()

    xp = stats['xp'] if stats else 0
    level = stats['level'] if stats else 1
    xp_current_level = get_xp_for_level(level)
    xp_next_level = get_xp_for_level(level + 1)
    xp_progress = xp - xp_current_level
    xp_needed = xp_next_level - xp_current_level
    progress_pct = min(100, int(xp_progress / max(1, xp_needed) * 100))

    stats_dict = dict(stats) if stats else {}
    return {
        'user': dict(user),
        'stats': stats_dict,
        'profile': dict(profile) if profile else {},
        'achievements': achievements,
        'items': items,
        'history': history,
        'wishlist': wishlist,
        'level': level,
        'xp': xp,
        'coins': stats_dict.get('coins', 0),
        'reading_streak': stats_dict.get('reading_streak', 0),
        'max_streak': stats_dict.get('max_streak', 0),
        'digest_hour': dict(user).get('digest_hour', 22),
        'xp_progress_pct': progress_pct,
        'xp_for_next': xp_needed - xp_progress,
        'display_name': (
            (dict(profile).get('custom_name') or '').strip() or
            dict(user).get('telegram_first_name') or
            dict(user).get('telegram_username') or
            f"Пользователь #{user_id}"
        ),
        'followers_count': followers_count,
    }

# ==================== ГЕЙМИФИКАЦИЯ: СТРИК / DAILY QUESTS / СЕЗОН ====================

from datetime import date as _date

def update_reading_streak(user_id, conn):
    """Обновить стрик чтения пользователя. Вызывать при каждом прочтении главы."""
    today = _date.today().isoformat()
    row = conn.execute(
        'SELECT reading_streak, max_streak, last_read_date FROM user_stats WHERE user_id=?',
        (user_id,)
    ).fetchone()
    if not row:
        return
    last = row['last_read_date']
    if last == today:
        return  # уже читал сегодня — стрик не меняем
    yesterday = (_date.today() - timedelta(1)).isoformat()
    new_streak = (row['reading_streak'] + 1) if last == yesterday else 1
    new_max = max(row['max_streak'] or 0, new_streak)
    conn.execute(
        'UPDATE user_stats SET reading_streak=?, max_streak=?, last_read_date=? WHERE user_id=?',
        (new_streak, new_max, today, user_id)
    )
    conn.commit()
    # Бонус XP за 7- и 30-дневный стрик (вызываем award_xp отдельно, без рекурсии)
    if new_streak in (7, 30):
        bonus = 50 if new_streak == 7 else 200
        conn.execute(
            'UPDATE user_stats SET xp=xp+?, coins=coins+? WHERE user_id=?',
            (bonus, bonus, user_id)
        )
        conn.execute(
            'INSERT INTO xp_log (user_id, reason, ref_id, amount) VALUES (?, ?, ?, ?)',
            (user_id, f'streak_{new_streak}', None, bonus)
        )
        conn.commit()


def get_or_create_daily_quests(user_id, conn):
    """Создать записи user_daily_quests на сегодня для всех активных дневных заданий."""
    today = _date.today().isoformat()
    quests = conn.execute('SELECT id FROM daily_quests WHERE is_active=1').fetchall()
    for q in quests:
        conn.execute(
            'INSERT OR IGNORE INTO user_daily_quests (user_id, quest_id, date) VALUES (?, ?, ?)',
            (user_id, q['id'], today)
        )
    conn.commit()


def update_daily_quest_progress(user_id, condition_type, conn):
    """Инкрементировать прогресс дневных заданий по condition_type. Выдать награду при выполнении."""
    today = _date.today().isoformat()
    get_or_create_daily_quests(user_id, conn)
    rows = conn.execute(
        '''SELECT udq.id, udq.progress, udq.completed_at, dq.condition_value, dq.xp_reward, dq.coins_reward
           FROM user_daily_quests udq
           JOIN daily_quests dq ON udq.quest_id = dq.id
           WHERE udq.user_id=? AND udq.date=? AND dq.condition_type=? AND dq.is_active=1''',
        (user_id, today, condition_type)
    ).fetchall()
    for row in rows:
        if row['completed_at']:
            continue
        new_progress = row['progress'] + 1
        if new_progress >= row['condition_value']:
            conn.execute(
                'UPDATE user_daily_quests SET progress=?, completed_at=? WHERE id=?',
                (row['condition_value'], today, row['id'])
            )
            if row['xp_reward'] > 0 or row['coins_reward'] > 0:
                conn.execute(
                    'UPDATE user_stats SET xp=xp+?, coins=coins+? WHERE user_id=?',
                    (row['xp_reward'], row['coins_reward'], user_id)
                )
                conn.execute(
                    'INSERT INTO xp_log (user_id, reason, ref_id, amount) VALUES (?, ?, ?, ?)',
                    (user_id, f'daily_quest:{row["id"]}', str(row['id']), row['xp_reward'])
                )
        else:
            conn.execute('UPDATE user_daily_quests SET progress=? WHERE id=?', (new_progress, row['id']))
    conn.commit()


def get_active_season(conn):
    """Вернуть активный сезон или None."""
    now_iso = datetime.utcnow().isoformat()
    return conn.execute(
        'SELECT * FROM seasons WHERE is_active=1 AND ends_at >= ? ORDER BY id DESC LIMIT 1',
        (now_iso[:10],)
    ).fetchone()


def update_season_quest_progress(user_id, condition_type, amount, conn):
    """Обновить накопительный прогресс по квестам активного сезона."""
    season = get_active_season(conn)
    if not season:
        return
    quests = conn.execute(
        'SELECT * FROM season_quests WHERE season_id=? AND condition_type=?',
        (season['id'], condition_type)
    ).fetchall()
    for q in quests:
        conn.execute(
            'INSERT OR IGNORE INTO user_season_quests (user_id, season_quest_id) VALUES (?, ?)',
            (user_id, q['id'])
        )
        row = conn.execute(
            'SELECT id, progress, completed_at FROM user_season_quests WHERE user_id=? AND season_quest_id=?',
            (user_id, q['id'])
        ).fetchone()
        if row['completed_at']:
            continue
        new_progress = row['progress'] + amount
        if new_progress >= q['condition_value']:
            conn.execute(
                'UPDATE user_season_quests SET progress=?, completed_at=? WHERE id=?',
                (q['condition_value'], datetime.utcnow().isoformat(), row['id'])
            )
            if q['xp_reward'] > 0 or q['coins_reward'] > 0:
                conn.execute(
                    'UPDATE user_stats SET xp=xp+?, coins=coins+? WHERE user_id=?',
                    (q['xp_reward'], q['coins_reward'], user_id)
                )
                conn.execute(
                    'INSERT INTO xp_log (user_id, reason, ref_id, amount) VALUES (?, ?, ?, ?)',
                    (user_id, f'season_quest:{q["id"]}', str(q['id']), q['xp_reward'])
                )
            # Выдать предметную награду
            if q['item_reward_id']:
                conn.execute(
                    'INSERT OR IGNORE INTO user_items (user_id, item_id) VALUES (?, ?)',
                    (user_id, q['item_reward_id'])
                )
        else:
            conn.execute(
                'UPDATE user_season_quests SET progress=? WHERE id=?',
                (new_progress, row['id'])
            )
    conn.commit()


def award_weekly_collection_trophy():
    """Выдать трофей коллекции недели. Вызывается по понедельникам из background_checker."""
    try:
        conn = get_db()
        # ISO-неделя
        from datetime import date as _d
        today = _d.today()
        iso_week = f"{today.isocalendar()[0]}-W{today.isocalendar()[1]:02d}"
        # Уже выдавали на этой неделе?
        if conn.execute('SELECT 1 FROM collection_trophies WHERE iso_week=?', (iso_week,)).fetchone():
            conn.close()
            return
        # Коллекция с максимальным числом лайков
        row = conn.execute(
            '''SELECT cl.collection_id, COUNT(*) as cnt, c.user_id
               FROM collection_likes cl
               JOIN collections c ON cl.collection_id = c.id
               GROUP BY cl.collection_id
               ORDER BY cnt DESC LIMIT 1'''
        ).fetchone()
        if not row:
            conn.close()
            return
        conn.execute(
            'INSERT OR IGNORE INTO collection_trophies (collection_id, user_id, iso_week, likes_count) VALUES (?,?,?,?)',
            (row['collection_id'], row['user_id'], iso_week, row['cnt'])
        )
        conn.execute(
            'UPDATE user_stats SET xp=xp+500, coins=coins+300 WHERE user_id=?',
            (row['user_id'],)
        )
        conn.execute(
            'INSERT INTO xp_log (user_id, reason, ref_id, amount) VALUES (?, ?, ?, ?)',
            (row['user_id'], 'weekly_trophy', iso_week, 500)
        )
        conn.commit()
        logger.info(f"🏆 Трофей коллекции недели {iso_week} выдан user_id={row['user_id']}")
        conn.close()
    except Exception as e:
        logger.error(f"award_weekly_collection_trophy error: {e}")


# ==================== ПОХОЖАЯ МАНГА ====================

def get_similar_manga(manga_id, user_id, limit=3):
    """Найти похожие манги по пересечению тегов (JSON-колонка). Исключает прочитанные."""
    import json as _json2
    conn = get_db()
    try:
        src = conn.execute('SELECT tags FROM manga WHERE manga_id = ?', (manga_id,)).fetchone()
        if not src:
            return []
        try:
            src_set = set(_json2.loads(src['tags'] or '[]'))
        except Exception:
            return []
        if not src_set:
            return []

        # Манги, которые пользователь уже читал или подписан
        read_ids = {r['manga_id'] for r in conn.execute(
            '''SELECT DISTINCT manga_id FROM chapters_read WHERE user_id = ?
               UNION SELECT manga_id FROM subscriptions WHERE user_id = ?''',
            (user_id, user_id)
        ).fetchall()}
        read_ids.add(manga_id)

        rows = conn.execute(
            'SELECT manga_id, manga_title, manga_slug, cover_url, tags FROM manga WHERE manga_id != ?',
            (manga_id,)
        ).fetchall()

        scored = []
        for row in rows:
            if row['manga_id'] in read_ids:
                continue
            try:
                other_tags = set(_json2.loads(row['tags'] or '[]'))
            except Exception:
                other_tags = set()
            overlap = len(src_set & other_tags)
            if overlap > 0:
                scored.append((overlap, row))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [
            {'manga_id': r['manga_id'], 'title': r['manga_title'],
             'slug': r['manga_slug'], 'cover': r['cover_url']}
            for _, r in scored[:limit]
        ]
    finally:
        conn.close()


def _suggest_similar_manga(user_id, manga_id, manga_title):
    """Отправить пользователю рекомендации похожих манг через Telegram + site notification."""
    similar = get_similar_manga(manga_id, user_id, limit=3)
    if not similar:
        return

    # Сайт-уведомление
    body = 'Пока жди: ' + ', '.join(s['title'] for s in similar)
    create_site_notification(user_id, 'similar_manga',
                             f'Похожие на "{manga_title}"', body, '/catalog')

    # Telegram уведомление
    conn = get_db()
    row = conn.execute('SELECT telegram_id, notifications_enabled FROM users WHERE id=?', (user_id,)).fetchone()
    conn.close()
    if not row or not row['telegram_id'] or row['notifications_enabled'] == 0:
        return

    lines = [f'📖 Ты дочитал всё доступное в <b>{manga_title}</b>!\n']
    lines.append('Пока ждёшь новых глав, попробуй похожее:\n')
    for s in similar:
        url = f"{SITE_URL}/manga/{s['slug']}"
        lines.append(f"• <a href='{url}'>{s['title']}</a>")
    message = '\n'.join(lines)

    async def _send():
        global telegram_app
        if telegram_app:
            try:
                await telegram_app.bot.send_message(
                    chat_id=row['telegram_id'], text=message,
                    parse_mode='HTML', disable_web_page_preview=True
                )
            except Exception as e:
                logger.warning(f"_suggest_similar_manga tg error: {e}")

    if _bot_loop and _bot_loop.is_running():
        asyncio.run_coroutine_threadsafe(_send(), _bot_loop)


# ==================== ФУНКЦИИ ДЛЯ ПОЛУЧЕНИЯ СПОТЛАЙТОВ ====================

def get_experimental_spotlights_api(after=None, website_mode="SENKURO"):
    """
    Получить экспериментальные спотлайты (блоки главной страницы)

    Args:
        after: курсор для пагинации (None, "2", "4")
        website_mode: режим сайта (SENKURO по умолчанию)

    Returns:
        dict: данные спотлайтов с пагинацией
    """
    return api.fetch_spotlights(after=after, website_mode=website_mode)

def parse_spotlight_data(spotlight_edge):
    """
    Парсинг данных из спотлайта
    
    Args:
        spotlight_edge: элемент из edges
        
    Returns:
        dict: структурированные данные спотлайта
    """
    node = spotlight_edge.get("node") or {}
    spotlight_id = node.get("id")
    titles = node.get("titles") or []

    # Получаем русское и английское название спотлайта
    ru_title = next((t["content"] for t in titles if t.get("lang") == "RU"), None)
    en_title = next((t["content"] for t in titles if t.get("lang") == "EN"), None)

    # Парсим манги в спотлайте
    manga_nodes = node.get("nodes") or []
    parsed_mangas = []

    for manga in manga_nodes:
        # Название манги
        manga_titles = manga.get("titles") or []
        manga_ru_title = next((t["content"] for t in manga_titles if t.get("lang") == "RU"), None)
        manga_en_title = next((t["content"] for t in manga_titles if t.get("lang") == "EN"), None)

        # Оригинальное название
        original_name = (manga.get("originalName") or {}).get("content", "")

        # Обложка
        cover = manga.get("cover") or {}
        original = cover.get("original") or {}
        preview = cover.get("preview") or {}
        cover_url = original.get("url") or preview.get("url", "")
        
        # Тип тега для отображения
        manga_type = manga.get("mangaType", "")
        tag_class = ""
        if manga_type == "MANGA":
            tag_class = "tag--manga"
        elif manga_type == "MANHWA":
            tag_class = "tag--manhwa"
        elif manga_type == "MANHUA":
            tag_class = "tag--manhua"
        
        parsed_manga = {
            'id': manga.get('id'),
            'slug': manga.get('slug'),
            'title': manga_ru_title or manga_en_title or original_name,
            'original_name': original_name,
            'type': manga_type,
            'status': manga.get('mangaStatus'),
            'rating': manga.get('mangaRating'),
            'formats': manga.get('mangaFormats', []),
            'cover_url': cover_url,
            'blurhash': cover.get('blurhash'),
            'tag_class': tag_class,
            'viewer_bookmark': manga.get('viewerBookmark')
        }
        
        # Сохраняем в БД для кеширования
        save_manga_from_spotlight(parsed_manga)
        parsed_mangas.append(parsed_manga)
    
    result = {
        'id': spotlight_id,
        'ru_title': ru_title,
        'en_title': en_title,
        'title': ru_title or en_title or f"Блок {spotlight_id}",
        'mangas': parsed_mangas
    }
    logger.info(
        f"[parse_spotlight] id={spotlight_id} ru={ru_title!r} en={en_title!r} "
        f"mangas={len(parsed_mangas)}"
    )
    return result

def save_manga_from_spotlight(manga_data):
    """Сохранить мангу из спотлайта в БД"""
    if not manga_data or not manga_data.get('id'):
        return
    
    conn = get_db()
    c = conn.cursor()
    
    try:
        # Проверяем, существует ли манга
        c.execute('SELECT manga_id FROM manga WHERE manga_id = ?', (manga_data['id'],))
        existing = c.fetchone()
        
        if existing:
            # Обновляем существующую запись
            c.execute('''UPDATE manga SET 
                        manga_slug = ?, manga_title = ?, manga_type = ?, manga_status = ?,
                        rating = ?, cover_url = ?, last_updated = ?
                        WHERE manga_id = ?''',
                      (manga_data['slug'], manga_data['title'], manga_data['type'],
                       manga_data['status'], manga_data['rating'], manga_data['cover_url'],
                       datetime.now(), manga_data['id']))
        else:
            # Создаем новую запись
            c.execute('''INSERT INTO manga 
                        (manga_id, manga_slug, manga_title, manga_type, 
                         manga_status, rating, cover_url, last_updated) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                      (manga_data['id'], manga_data['slug'], manga_data['title'],
                       manga_data['type'], manga_data['status'], manga_data['rating'],
                       manga_data['cover_url'], datetime.now()))
        
        conn.commit()
        logger.debug(f"✅ Сохранена манга из спотлайта: {manga_data['title']}")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения манги из спотлайта: {e}")
    finally:
        conn.close()

def get_all_experimental_spotlights():
    """
    Получить ВСЕ экспериментальные спотлайты с пагинацией
    
    Returns:
        list: список всех спотлайтов
    """
    all_spotlights = []
    after = None
    has_next_page = True
    request_count = 0
    
    logger.info("🔄 Начинаем загрузку всех экспериментальных спотлайтов")
    
    while has_next_page and request_count < 5:  # Ограничиваем 5 запросами максимум
        request_count += 1
        
        # Получаем данные страницы
        data = get_experimental_spotlights_api(after=after)
        
        # Получаем информацию о пагинации
        page_info = data.get("pageInfo", {})
        has_next_page = page_info.get("hasNextPage", False)
        after = page_info.get("endCursor")
        
        # Парсим спотлайты на текущей странице
        edges = data.get("edges", [])
        logger.info(f"📄 Запрос {request_count}: получено {len(edges)} спотлайтов, hasNextPage: {has_next_page}, endCursor: {after}")
        
        for edge in edges:
            spotlight_data = parse_spotlight_data(edge)
            if spotlight_data:
                all_spotlights.append(spotlight_data)
        
        # Небольшая задержка между запросами
        if has_next_page:
            time.sleep(0.0001)
    
    logger.info(f"✅ Всего загружено {len(all_spotlights)} спотлайтов")
    return all_spotlights

def get_cached_spotlights(ttl_seconds=3600):
    """
    Получить закешированные спотлайты
    
    Args:
        ttl_seconds: время жизни кеша в секундах (по умолчанию 1 час)
    
    Returns:
        dict: закешированные спотлайты
    """
    cache_key = "spotlights_cache"
    
    conn = get_db()
    c = conn.cursor()
    
    try:
        # Проверяем кеш
        c.execute('SELECT value, updated_at FROM cache WHERE key = ?', (cache_key,))
        cache_row = c.fetchone()
        
        if cache_row:
            cache_data = json.loads(cache_row['value'])
            cache_time = datetime.fromisoformat(cache_row['updated_at'])
            current_time = datetime.now()
            
            # Проверяем свежесть кеша
            if (current_time - cache_time).total_seconds() < ttl_seconds:
                logger.info(f"📦 Используем закешированные спотлайты (возраст: {(current_time - cache_time).total_seconds():.0f} сек)")
                conn.close()
                return cache_data
        
        # Кеш устарел или отсутствует, загружаем свежие данные
        logger.info("📄 Загружаем свежие спотлайты...")
        all_spotlights = get_all_experimental_spotlights()
        
        # Группируем спотлайты по типам
        spotlights_by_type = {
            'last_manga': None,        # Последние манги
            'popular_new': None,       # Популярные новинки манги
            'top_manhwa': None,        # Топ мхнва
            'top_manhua': None,        # Топ манхуа
            'top_manga': None,         # Топ манг
            'most_read': None,         # Самое читаемое
            'latest_updates': None,    # Последние обновления
            'genres': None             # Жанры/теги
        }
        
        # Соопостав спотлайты по названиям
        for spotlight in all_spotlights:
            title = (spotlight.get('title') or '').lower()
            ru_title = (spotlight.get('ru_title') or '').lower()

            if any(keyword in title or keyword in ru_title for keyword in ['последние манги', 'last manga']):
                spotlights_by_type['last_manga'] = spotlight
                logger.info(f"[categorize] id={spotlight.get('id')} → last_manga (title={spotlight.get('title')!r})")
            elif any(keyword in title or keyword in ru_title for keyword in ['популярные новинки', 'new popular']):
                spotlights_by_type['popular_new'] = spotlight
                logger.info(f"[categorize] id={spotlight.get('id')} → popular_new (title={spotlight.get('title')!r})")
            elif any(keyword in title or keyword in ru_title for keyword in ['топ манхв', 'top manhwa']):
                spotlights_by_type['top_manhwa'] = spotlight
                logger.info(f"[categorize] id={spotlight.get('id')} → top_manhwa (title={spotlight.get('title')!r})")
            elif any(keyword in title or keyword in ru_title for keyword in ['топ манхуа', 'топ маньхуа', 'top manhua']):
                spotlights_by_type['top_manhua'] = spotlight
                logger.info(f"[categorize] id={spotlight.get('id')} → top_manhua (title={spotlight.get('title')!r})")
            elif any(keyword in title or keyword in ru_title for keyword in ['топ манг', 'top manga']):
                spotlights_by_type['top_manga'] = spotlight
                logger.info(f"[categorize] id={spotlight.get('id')} → top_manga (title={spotlight.get('title')!r})")
            elif any(keyword in title or keyword in ru_title for keyword in ['самое читаемое', 'most read']):
                spotlights_by_type['most_read'] = spotlight
                logger.info(f"[categorize] id={spotlight.get('id')} → most_read (title={spotlight.get('title')!r})")
            elif any(keyword in title or keyword in ru_title for keyword in ['последние обновления', 'latest updates']):
                spotlights_by_type['latest_updates'] = spotlight
                logger.info(f"[categorize] id={spotlight.get('id')} → latest_updates (title={spotlight.get('title')!r})")
            elif any(keyword in title or keyword in ru_title for keyword in ['лейблы', 'labels', 'жанры', 'genres']):
                spotlights_by_type['genres'] = spotlight
                logger.info(f"[categorize] id={spotlight.get('id')} → genres (title={spotlight.get('title')!r})")
            else:
                logger.warning(f"[categorize] id={spotlight.get('id')} → НЕ РАСПОЗНАН (title={spotlight.get('title')!r}, ru={spotlight.get('ru_title')!r})")
        
        # Получаем "Самое читаемое" отдельно (если не нашли в спотлайтах)
        if spotlights_by_type['most_read'] is None or not spotlights_by_type['most_read']['mangas']:
            most_read_manga = get_most_read_manga(limit=12)
            spotlights_by_type['most_read'] = most_read_manga
        
        result = {
            'spotlights': spotlights_by_type,
            'all_spotlights': all_spotlights,
            'cached_at': datetime.now().isoformat()
        }
        
        # Сохраняем в кеш
        c.execute('INSERT OR REPLACE INTO cache (key, value, updated_at) VALUES (?, ?, ?)', 
                 (cache_key, json.dumps(result), datetime.now().isoformat()))
        conn.commit()
        
        logger.info(f"✅ Сохранено в кеш: {len(all_spotlights)} спотлайтов")
        return result
        
    except Exception as e:
        logger.error(f"❌ Ошибка кеширования спотлайтов: {e}")
        # В случае ошибки возвращаем пустые данные
        return {
            'spotlights': {},
            'all_spotlights': [],
            'cached_at': datetime.now().isoformat()
        }
    finally:
        conn.close()

def get_most_read_manga(period="WEEK", limit=12):
    """
    Получить "Самое читаемое"
    
    Args:
        period: период (DAY, WEEK, MONTH)
        limit: количество манг
    
    Returns:
        dict: спотлайт с самыми читаемыми мангами
    """
    # Используем существующую функцию для популярных манг
    popular_manga = get_popular_manga_from_api(period=period, limit=limit)
    
    # Преобразуем в формат спотлайта
    most_read_spotlight = {
        'id': 'most_read',
        'title': 'Самое читаемое',
        'mangas': []
    }
    
    for manga in popular_manga:
        manga_data = {
            'id': manga.get('manga_id'),
            'slug': manga.get('manga_slug'),
            'title': manga.get('manga_title'),
            'cover_url': manga.get('cover_url'),
            'score': manga.get('score', 0),
            'type': 'MANGA',  # По умолчанию
            'tag_class': 'tag--manga'
        }
        most_read_spotlight['mangas'].append(manga_data)
    
    return most_read_spotlight

# ==================== ФУНКЦИИ ПОИСКА ====================

def search_manga_api(query, limit=50):
    """Поиск манги через API с кешированием результатов в БД"""
    results = api.search(query, max_results=limit)

    # Кешируем результаты в БД
    for manga in results:
        save_manga_search_result(manga)

    return results

def save_manga_search_result(manga_data):
    """Сохранить результат поиска в БД"""
    conn = get_db()
    c = conn.cursor()
    
    try:
        c.execute('''INSERT OR REPLACE INTO manga 
                     (manga_id, manga_slug, manga_title, manga_type, 
                      manga_status, cover_url, rating, last_updated) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (manga_data['manga_id'], manga_data['manga_slug'], 
                   manga_data['manga_title'], manga_data['manga_type'],
                   manga_data['manga_status'], manga_data['cover_url'],
                   manga_data.get('rating', 'GENERAL'), datetime.now()))
        conn.commit()
    except Exception as e:
        print(f"❌ Ошибка сохранения манги: {e}")
    finally:
        conn.close()

def save_search_history(user_id, query):
    """Сохранить историю поиска"""
    if not user_id:
        return
    
    conn = get_db()
    c = conn.cursor()
    
    # Удаляем старые записи если их больше 50
    c.execute('DELETE FROM search_history WHERE id IN '
              '(SELECT id FROM search_history WHERE user_id = ? ORDER BY created_at DESC LIMIT -1 OFFSET 50)',
              (user_id,))
    
    c.execute('INSERT INTO search_history (user_id, query) VALUES (?, ?)',
              (user_id, query))
    conn.commit()
    conn.close()

def get_search_suggestions(query, limit=100):
    """Получить предложения для автодополнения"""
    conn = get_db()
    c = conn.cursor()
    c.execute('''SELECT DISTINCT query FROM search_history 
                 WHERE query LIKE ? 
                 ORDER BY created_at DESC 
                 LIMIT ?''',
              (f'{query}%', limit))
    suggestions = [row[0] for row in c.fetchall()]
    conn.close()
    return suggestions

# ==================== ПОЛЬЗОВАТЕЛИ ====================

def get_or_create_user_by_telegram(telegram_id, username=None, first_name=None, last_name=None):
    """Получить или создать пользователя по Telegram ID"""
    conn = get_db()
    c = conn.cursor()
    
    c.execute('SELECT * FROM users WHERE telegram_id = ?', (telegram_id,))
    user = c.fetchone()
    
    if user:
        # Обновляем данные если они изменились
        if (user['telegram_username'] != username or 
            user['telegram_first_name'] != first_name or
            user['telegram_last_name'] != last_name):
            c.execute('''UPDATE users SET 
                        telegram_username = ?, 
                        telegram_first_name = ?,
                        telegram_last_name = ?,
                        last_login = ?
                        WHERE id = ?''',
                      (username, first_name, last_name, datetime.now(), user['id']))
            conn.commit()
        
        conn.close()
        return dict(user)
    
    # Создаем нового пользователя
    login_token = secrets.token_urlsafe(32)
    referral_code = secrets.token_urlsafe(6).upper()
    c.execute('''INSERT INTO users
                 (telegram_id, telegram_username, telegram_first_name, telegram_last_name, login_token, referral_code)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (telegram_id, username, first_name, last_name, login_token, referral_code))
    conn.commit()
    
    user_id = c.lastrowid
    c.execute('SELECT * FROM users WHERE id = ?', (user_id,))
    user = c.fetchone()
    conn.close()
    
    return dict(user) if user else None

def get_user_by_token(token):
    """Получить пользователя по токену"""
    conn = get_db()
    c = conn.cursor()
    c.execute('''SELECT * FROM users WHERE login_token = ? AND is_active = 1''', (token,))
    user = c.fetchone()
    conn.close()
    return dict(user) if user else None

def update_user_token(user_id):
    """Обновить токен пользователя"""
    conn = get_db()
    c = conn.cursor()
    new_token = secrets.token_urlsafe(32)
    c.execute('UPDATE users SET login_token = ? WHERE id = ?', (new_token, user_id))
    conn.commit()
    conn.close()
    return new_token

def get_user_by_telegram_id(telegram_id):
    """Получить пользователя по Telegram ID"""
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM users WHERE telegram_id = ?', (telegram_id,))
    user = c.fetchone()
    conn.close()
    return dict(user) if user else None

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
# ==================== ФУНКЦИИ ДЛЯ ПОЛУЧЕНИЯ ДЕТАЛЕЙ МАНГИ ====================

def get_manga_details_api(manga_slug):
    """Получить детальную информацию о манге через API и закешировать в БД"""
    manga_data = api.fetch_manga(manga_slug)
    if not manga_data:
        return None

    # Кешируем в БД
    save_manga_details_to_db(manga_data)

    logger.info(f"✅ Получена манга {manga_slug}")
    logger.info(f"   🆔 manga_id: {manga_data['manga_id']}")
    logger.info(f"   🌿 branch_id: {manga_data['branch_id']}")
    logger.info(f"   📚 Глав заявлено: {manga_data['chapters_count']}")

    return manga_data

def save_manga_details_to_db(manga_data):
    """Сохранить детали манги в БД (все поля включая описание, теги, оценку)"""
    import json as _json
    conn = get_db()
    c = conn.cursor()

    try:
        tags_json    = _json.dumps(manga_data.get('tags', []),    ensure_ascii=False)
        formats_json = _json.dumps(manga_data.get('formats', []), ensure_ascii=False)

        c.execute(
            '''INSERT INTO manga
                   (manga_id, manga_slug, manga_title, original_name,
                    manga_type, manga_status, rating, views, score,
                    cover_url, branch_id, chapters_count,
                    description, tags, formats, is_licensed, translation_status,
                    last_updated)
               VALUES
                   (:manga_id, :manga_slug, :manga_title, :original_name,
                    :manga_type, :manga_status, :rating, :views, :score,
                    :cover_url, :branch_id, :chapters_count,
                    :description, :tags, :formats, :is_licensed, :translation_status,
                    :now)
               ON CONFLICT(manga_id) DO UPDATE SET
                   manga_slug         = excluded.manga_slug,
                   manga_title        = excluded.manga_title,
                   original_name      = excluded.original_name,
                   manga_type         = excluded.manga_type,
                   manga_status       = excluded.manga_status,
                   rating             = excluded.rating,
                   views              = excluded.views,
                   score              = excluded.score,
                   cover_url          = excluded.cover_url,
                   branch_id          = excluded.branch_id,
                   chapters_count     = excluded.chapters_count,
                   description        = excluded.description,
                   tags               = excluded.tags,
                   formats            = excluded.formats,
                   is_licensed        = excluded.is_licensed,
                   translation_status = excluded.translation_status,
                   last_updated       = excluded.last_updated''',
            {
                'manga_id':          manga_data['manga_id'],
                'manga_slug':        manga_data['manga_slug'],
                'manga_title':       manga_data['manga_title'],
                'original_name':     manga_data.get('original_name', ''),
                'manga_type':        manga_data.get('manga_type'),
                'manga_status':      manga_data.get('manga_status'),
                'rating':            manga_data.get('rating'),
                'views':             manga_data.get('views', 0),
                'score':             manga_data.get('score', 0),
                'cover_url':         manga_data.get('cover_url', ''),
                'branch_id':         manga_data.get('branch_id'),
                'chapters_count':    manga_data.get('chapters_count', 0),
                'description':       manga_data.get('description', ''),
                'tags':              tags_json,
                'formats':           formats_json,
                'is_licensed':       1 if manga_data.get('is_licensed') else 0,
                'translation_status': manga_data.get('translation_status', ''),
                'now':               datetime.now().isoformat(),
            }
        )
        conn.commit()
        logger.info(f"✅ Сохранена манга в БД: {manga_data['manga_title']}")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения деталей манги: {e}")
    finally:
        conn.close()

def get_manga_chapters_api(manga_slug, limit=10000):
    """Получить ВСЕ главы манги через API с пагинацией"""
    # Сначала получаем детали манги чтобы узнать ID ветки
    manga_details = get_manga_details_api(manga_slug)
    if not manga_details:
        logger.error(f"❌ Не удалось получить детали манги {manga_slug}")
        return []
    
    # Получаем ID манги и ветки
    manga_id = manga_details['manga_id']
    branch_id = manga_details.get('branch_id', manga_id)
    
    logger.info(f"🔄 Загрузка ВСЕХ глав для {manga_slug}, manga_id: {manga_id}, branch_id: {branch_id}")
    
    chapters = []
    after = None
    has_next_page = True
    page_num = 0
    max_pages = 50  # Максимум 50 страниц (5000 глав) на всякий случай

    while has_next_page and page_num < max_pages:
        page_num += 1

        chapters_connection = api.fetch_manga_chapters_page(branch_id, after)

        if not chapters_connection:
            logger.warning(f"⚠️ Пустой ответ для глав манги {manga_slug} (страница {page_num})")
            break

        # Пагинация
        page_info = chapters_connection.get("pageInfo", {})
        has_next_page = page_info.get("hasNextPage", False)
        after = page_info.get("endCursor")

        edges = chapters_connection.get("edges", [])
        logger.info(
            f"📄 Страница {page_num}: получено {len(edges)} глав "
            f"(всего {len(chapters) + len(edges)}), "
            f"hasNextPage={has_next_page}, endCursor={after}"
        )

        for edge in edges:
            node = edge.get("node") or {}
            if not node:
                continue
            chapters.append({
                'chapter_id': node.get('id'),
                'chapter_slug': node.get('slug'),
                'chapter_number': node.get('number'),
                'chapter_volume': node.get('volume'),
                'chapter_name': node.get('name'),
                'created_at': node.get('createdAt'),
                'manga_id': manga_id,
                'manga_slug': manga_slug
            })

        # Прерываем при достижении лимита
        if limit and len(chapters) >= limit:
            logger.info(f"✅ Достигнут лимит {limit} глав")
            break

        # Небольшая пауза между запросами
        if has_next_page:
            time.sleep(0.00001)
    
    # Кешируем главы в БД
    if chapters:
        save_chapters_to_db(chapters, manga_id)
        logger.info(f"✅ Получено и сохранено {len(chapters)} глав для {manga_slug}")
        
        # Обновляем счетчик глав в таблице manga
        update_manga_chapters_count(manga_id, len(chapters))
    else:
        logger.warning(f"⚠️ Главы не найдены для {manga_slug}")
    
    return chapters[:limit] if limit else chapters

def update_manga_chapters_count(manga_id, chapters_count):
    """Обновить количество глав в таблице manga"""
    conn = get_db()
    c = conn.cursor()
    
    try:
        c.execute('UPDATE manga SET chapters_count = ? WHERE manga_id = ?', 
                 (chapters_count, manga_id))
        conn.commit()
        logger.info(f"📊 Обновлен счетчик глав: {manga_id} -> {chapters_count} глав")
    except Exception as e:
        logger.error(f"❌ Ошибка обновления счетчика глав: {e}")
    finally:
        conn.close()

def save_chapters_to_db(chapters, manga_id):
    """Сохранить главы в БД с улучшенной обработкой"""
    if not chapters:
        return
    
    conn = get_db()
    c = conn.cursor()
    
    try:
        saved_count = 0
        updated_count = 0
        errors = 0
        
        for chapter in chapters:
            try:
                # Проверяем, существует ли глава
                c.execute('SELECT chapter_id, chapter_number FROM chapters WHERE chapter_id = ?', 
                         (chapter['chapter_id'],))
                existing = c.fetchone()
                
                if not existing:
                    # Создаем URL для чтения
                    chapter_url = f"/read/{chapter['manga_slug']}/{chapter['chapter_slug']}"
                    
                    # Сохраняем новую главу
                    c.execute('''INSERT INTO chapters 
                                (manga_id, chapter_id, chapter_slug, chapter_number,
                                 chapter_volume, chapter_name, chapter_url, created_at) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                              (manga_id, chapter['chapter_id'], chapter['chapter_slug'],
                               chapter['chapter_number'], chapter['chapter_volume'],
                               chapter['chapter_name'], chapter_url,
                               chapter['created_at']))
                    saved_count += 1
                else:
                    # Проверяем, нужно ли обновить номер главы (на случай изменений)
                    existing_number = existing['chapter_number']
                    new_number = chapter['chapter_number']
                    
                    if existing_number != new_number:
                        c.execute('UPDATE chapters SET chapter_number = ? WHERE chapter_id = ?',
                                 (new_number, chapter['chapter_id']))
                        updated_count += 1
                        
            except Exception as e:
                errors += 1
                logger.error(f"❌ Ошибка сохранения главы {chapter.get('chapter_id', 'unknown')}: {e}")
        
        conn.commit()
        logger.info(f"✅ Сохранено {saved_count} новых глав, обновлено {updated_count}, ошибок: {errors}")
        
        # Создаем индекс для быстрого поиска глав по номеру
        c.execute('CREATE INDEX IF NOT EXISTS idx_chapters_manga_number ON chapters(manga_id, chapter_number)')
        conn.commit()
        
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения глав: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

def get_manga_details_with_chapters(manga_slug, limit=50):
    """Получить детали манги и её главы"""
    # Получаем детали манги
    manga_details = get_manga_details_api(manga_slug)
    if not manga_details:
        return None, []
    
    # Получаем главы манги
    chapters = get_manga_chapters_api(manga_slug, limit)
    
    return manga_details, chapters
    
def get_popular_manga_from_api(period="MONTH", limit=12):
    """Получить популярные манги из API"""
    return api.fetch_popular_manga(period=period, limit=limit)


def get_cached_recent_chapters(ttl_seconds=300):
    """
    Получить последние главы с кешированием в БД.

    Args:
        ttl_seconds: время жизни кеша в секундах (по умолчанию 5 минут)

    Returns:
        list: список последних глав
    """
    cache_key = 'recent_chapters_cache'
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('SELECT value, updated_at FROM cache WHERE key = ?', (cache_key,))
        row = c.fetchone()
        if row:
            age = (datetime.now() - datetime.fromisoformat(row['updated_at'])).total_seconds()
            if age < ttl_seconds:
                logger.info(f"📦 Используем кеш последних глав (возраст: {age:.0f} сек)")
                return json.loads(row['value'])

        logger.info("📄 Загружаем свежие последние главы из API...")
        data = get_recent_chapters_from_api(21)
        c.execute(
            'INSERT OR REPLACE INTO cache (key, value, updated_at) VALUES (?, ?, ?)',
            (cache_key, json.dumps(data), datetime.now().isoformat())
        )
        conn.commit()
        return data
    except Exception as e:
        logger.error(f"❌ Ошибка кеширования последних глав: {e}")
        return []
    finally:
        conn.close()

def get_recent_chapters(limit=20):
    """Получить последние главы из БД"""
    conn = get_db()
    c = conn.cursor()
    c.execute('''SELECT c.*, m.manga_title, m.manga_slug, m.cover_url 
                 FROM chapters c 
                 JOIN manga m ON c.manga_id = m.manga_id 
                 ORDER BY c.created_at DESC 
                 LIMIT ?''', (limit,))
    chapters = c.fetchall()
    conn.close()
    return [dict(ch) for ch in chapters]

def get_recent_chapters_from_api(limit=21):
    """
    Получить последние главы напрямую из API.
    Возвращает все главы из lastMangaChapters (обычно 21 глава).
    """
    try:
        edges = api.fetch_main_page()
        if not edges:
            logger.error("❌ API не вернул данные для последних глав")
            return get_recent_chapters(limit)
        
        logger.info(f"📚 Получено {len(edges)} последних глав из API")
        
        recent_chapters = []
        
        for edge in edges[:limit]:
            node = edge.get("node") or {}
            if not node:
                continue
            manga_id = node.get("id")
            manga_slug = node.get("slug")

            # Получаем название манги
            titles = node.get("titles") or []
            ru_title = next((t["content"] for t in titles if t.get("lang") == "RU"), None)
            en_title = next((t["content"] for t in titles if t.get("lang") == "EN"), None)
            manga_title = ru_title or en_title or manga_slug

            # Получаем обложку
            cover = node.get("cover") or {}
            cover_url = (cover.get("original") or {}).get("url", "") or \
                        (cover.get("preview") or {}).get("url", "")
            
            # Получаем последние главы этой манги
            last_chapters = node.get("lastChapters", [])
            
            if not last_chapters:
                continue
            
            # Берем самую последнюю главу
            latest_chapter = last_chapters[0]
            
            chapter_data = {
                'manga_id': manga_id,
                'manga_slug': manga_slug,
                'manga_title': manga_title,
                'cover_url': cover_url,
                'chapter_id': latest_chapter.get('id'),
                'chapter_slug': latest_chapter.get('slug'),
                'chapter_number': latest_chapter.get('number'),
                'chapter_volume': latest_chapter.get('volume'),
                'chapter_name': latest_chapter.get('name'),
                'created_at': latest_chapter.get('createdAt'),
                'chapter_url': f"{SITE_URL}/read/{manga_slug}/{latest_chapter.get('slug')}"
            }
            
            recent_chapters.append(chapter_data)
            
            # Сохраняем мангу и главу в БД для кеширования
            save_manga_and_chapter_to_db(manga_id, manga_slug, manga_title, cover_url, latest_chapter)
        
        logger.info(f"✅ Обработано {len(recent_chapters)} последних глав")
        return recent_chapters
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения последних глав из API: {e}")
        import traceback
        traceback.print_exc()
        # В случае ошибки возвращаем данные из БД
        return get_recent_chapters(limit)

def save_manga_and_chapter_to_db(manga_id, manga_slug, manga_title, cover_url, chapter_info):
    """Сохранить мангу и главу в БД для кеширования"""
    conn = get_db()
    c = conn.cursor()
    
    try:
        # Сохраняем мангу
        c.execute('''INSERT OR REPLACE INTO manga 
                     (manga_id, manga_slug, manga_title, cover_url, 
                      last_chapter_id, last_chapter_number, last_chapter_volume,
                      last_chapter_name, last_chapter_slug, last_updated) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (manga_id, manga_slug, manga_title, cover_url,
                   chapter_info.get('id'), chapter_info.get('number'), 
                   chapter_info.get('volume'), chapter_info.get('name'), 
                   chapter_info.get('slug'), datetime.now()))
        
        # Проверяем, существует ли глава
        c.execute('SELECT chapter_id FROM chapters WHERE chapter_id = ?', 
                 (chapter_info.get('id'),))
        existing = c.fetchone()
        
        if not existing:
            # Сохраняем главу
            c.execute('''INSERT INTO chapters 
                         (manga_id, chapter_id, chapter_slug, chapter_number, 
                          chapter_volume, chapter_name, created_at) 
                         VALUES (?, ?, ?, ?, ?, ?, ?)''',
                      (manga_id, chapter_info.get('id'), chapter_info.get('slug'),
                       chapter_info.get('number'), chapter_info.get('volume'),
                       chapter_info.get('name'), chapter_info.get('createdAt') or datetime.now()))
        
        conn.commit()
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения манги/главы в БД: {e}")
    finally:
        conn.close()

def get_user_subscriptions(user_id, limit=12):
    conn = get_db()
    c = conn.cursor()
    c.execute('''SELECT m.* FROM manga m
                 JOIN subscriptions s ON m.manga_id = s.manga_id
                 WHERE s.user_id = ?
                 ORDER BY m.last_updated DESC
                 LIMIT ?''', (user_id, limit))
    manga = c.fetchall()
    conn.close()
    return [dict(m) for m in manga]

def get_user_reading(user_id, limit=12):
    conn = get_db()
    c = conn.cursor()
    c.execute('''SELECT m.*, MAX(rh.last_read) as last_read_time
                 FROM manga m
                 JOIN reading_history rh ON m.manga_id = rh.manga_id
                 WHERE rh.user_id = ?
                 GROUP BY m.manga_id
                 ORDER BY last_read_time DESC
                 LIMIT ?''', (user_id, limit))
    manga = c.fetchall()
    conn.close()
    return [dict(m) for m in manga]

def toggle_subscription(user_id, manga_id):
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id FROM subscriptions WHERE user_id = ? AND manga_id = ?', 
              (user_id, manga_id))
    existing = c.fetchone()
    
    if existing:
        c.execute('DELETE FROM subscriptions WHERE user_id = ? AND manga_id = ?', 
                  (user_id, manga_id))
        subscribed = False
    else:
        c.execute('INSERT INTO subscriptions (user_id, manga_id) VALUES (?, ?)', 
                  (user_id, manga_id))
        subscribed = True
    
    conn.commit()
    conn.close()
    return subscribed

def is_subscribed(user_id, manga_id):
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id FROM subscriptions WHERE user_id = ? AND manga_id = ?', 
              (user_id, manga_id))
    result = c.fetchone()
    conn.close()
    return result is not None

def increment_manga_views(manga_id):
    conn = get_db()
    c = conn.cursor()
    c.execute('UPDATE manga SET views = views + 1 WHERE manga_id = ?', (manga_id,))
    conn.commit()
    conn.close()

# ==================== ПРОВЕРКА НОВЫХ ГЛАВ ====================

last_known_chapters = {}

async def send_telegram_notification(user_id, manga_title, chapter_info, chapter_url):
    """Отправка уведомления пользователю через Telegram"""
    global telegram_app
    
    message = f"🆕 <b>Новая глава!</b>\n\n"
    message += f"📖 <b>{manga_title}</b>\n"
    message += f"Глава: {chapter_info.get('chapter_number')}"
    if chapter_info.get('chapter_volume'):
        message += f" (Том {chapter_info.get('chapter_volume')})"
    if chapter_info.get('chapter_name'):
        message += f"\n{chapter_info.get('chapter_name')}"
    message += f"\n\n🔗 <a href='{chapter_url}'>Читать на сайте</a>"
    
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('SELECT telegram_id FROM users WHERE id = ?', (user_id,))
        result = c.fetchone()
        conn.close()
        
        if result:
            telegram_id = result[0]
            await telegram_app.bot.send_message(
                chat_id=telegram_id,
                text=message,
                parse_mode='HTML'
            )
    except Exception as e:
        print(f"❌ Ошибка отправки уведомления: {e}")

def get_chapter_pages(chapter_slug):
    """Получить страницы главы через API"""
    logger.info(f"Загрузка страниц для главы: {chapter_slug}")
    return api.fetch_chapter_pages(chapter_slug)

def save_chapter_to_db(chapter_data):
    """Сохранить главу в БД"""
    conn = get_db()
    c = conn.cursor()
    pages_json = json.dumps(chapter_data['pages'])
    pages_count = len(chapter_data['pages'])
    
    try:
        c.execute('''INSERT OR REPLACE INTO chapters 
                     (manga_id, chapter_id, chapter_slug, chapter_number, chapter_volume, 
                      chapter_name, chapter_url, pages_json, pages_count) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (chapter_data['manga_id'], chapter_data['chapter_id'], 
                   chapter_data['chapter_slug'], chapter_data['chapter_number'], 
                   chapter_data['chapter_volume'], chapter_data['chapter_name'], 
                   chapter_data['chapter_url'], pages_json, pages_count))
        conn.commit()
    except Exception as e:
        print(f"❌ Ошибка сохранения главы: {e}")
    finally:
        conn.close()

def process_new_chapter(manga_title, manga_slug, manga_id, chapter_info, cover_url):
    """Обработка новой главы"""
    chapter_slug = chapter_info.get("slug")
    chapter_number = chapter_info.get("number")
    chapter_volume = chapter_info.get("volume")
    chapter_name = chapter_info.get("name")
    chapter_id = chapter_info.get("id")
    chapter_url = f"{SITE_URL}/read/{manga_slug}/{chapter_slug}"

    pages = get_chapter_pages(chapter_slug)
    if not pages:
        return

    page_urls = [p.get("image", {}).get("compress", {}).get("url", "") 
                 for p in pages if p.get("image", {}).get("compress", {}).get("url")]

    # Сохранить мангу в БД
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('''INSERT OR REPLACE INTO manga 
                     (manga_id, manga_slug, manga_title, cover_url, 
                      last_chapter_id, last_chapter_number, last_chapter_volume,
                      last_chapter_name, last_chapter_slug, last_updated) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (manga_id, manga_slug, manga_title, cover_url,
                   chapter_id, chapter_number, chapter_volume,
                   chapter_name, chapter_slug, datetime.now()))
        conn.commit()
    except Exception as e:
        print(f"❌ Ошибка сохранения манги: {e}")
    finally:
        conn.close()

    # Сохранить главу
    chapter_data = {
        'manga_id': manga_id,
        'chapter_id': chapter_id,
        'chapter_slug': chapter_slug,
        'chapter_number': chapter_number,
        'chapter_volume': chapter_volume,
        'chapter_name': chapter_name,
        'chapter_url': chapter_url,
        'pages': page_urls
    }
    save_chapter_to_db(chapter_data)

    # Уведомить подписанных пользователей
    conn = get_db()
    c = conn.cursor()
    c.execute(
        '''SELECT s.user_id, u.is_premium, u.notifications_enabled
           FROM subscriptions s
           JOIN users u ON s.user_id = u.id
           WHERE s.manga_id = ? AND u.is_active = 1''',
        (manga_id,)
    )
    subscribers = c.fetchall()
    conn.close()

    for sub in subscribers:
        uid = sub['user_id']
        if sub['notifications_enabled'] == 0:
            continue
        # Уведомление на сайте для всех подписчиков
        create_site_notification(uid, 'new_chapter', f'Новая глава: {manga_title}',
                                 f'Глава {chapter_number}' if chapter_number else None,
                                 chapter_url, ref_id=str(chapter_id))
        if sub['is_premium']:
            # Премиум: мгновенное уведомление со ссылкой на чтение
            coro = send_telegram_notification(uid, manga_title, chapter_data, chapter_url)
            if _bot_loop and _bot_loop.is_running():
                asyncio.run_coroutine_threadsafe(coro, _bot_loop)
            else:
                asyncio.run(coro)
        else:
            # Не премиум: добавить в очередь ежедневного дайджеста
            try:
                conn2 = get_db()
                c2 = conn2.cursor()
                c2.execute(
                    '''INSERT OR IGNORE INTO notification_queue
                       (user_id, manga_id, manga_title, manga_slug, chapter_number, chapter_volume, chapter_name)
                       VALUES (?, ?, ?, ?, ?, ?, ?)''',
                    (uid, manga_id, manga_title, manga_slug,
                     str(chapter_number) if chapter_number else None,
                     str(chapter_volume) if chapter_volume else None,
                     chapter_name)
                )
                conn2.commit()
                conn2.close()
            except Exception as e:
                print(f"❌ Ошибка добавления в очередь: {e}")

def check_new_chapters():
    """Проверка новых глав и обновление БД всеми 21 главой из API"""
    try:
        edges = api.fetch_main_page()
        if not edges:
            logger.error("❌ API не вернул данные при проверке новых глав")
            return

        logger.info(f"\n[{datetime.now().strftime('%H:%M:%S')}] 🔄 Проверка... Получено {len(edges)} глав из API")

        # Обрабатываем ВСЕ полученные главы (обычно 21)
        for edge in edges:
            node = edge.get("node") or {}
            if not node:
                continue
            manga_id = node.get("id")
            manga_slug = node.get("slug")

            titles = node.get("titles") or []
            ru_title = next((t["content"] for t in titles if t.get("lang") == "RU"), None)
            en_title = next((t["content"] for t in titles if t.get("lang") == "EN"), None)
            manga_title = ru_title or en_title or manga_slug

            cover = node.get("cover") or {}
            cover_url = (cover.get("original") or {}).get("url", "") or \
                        (cover.get("preview") or {}).get("url", "")
            
            last_chapters = node.get("lastChapters", [])
            
            if not last_chapters:
                continue

            latest_chapter = last_chapters[0]
            chapter_id = latest_chapter.get("id")

            # Сохраняем мангу и главу в БД для отображения в последних обновлениях
            save_manga_and_chapter_to_db(manga_id, manga_slug, manga_title, cover_url, latest_chapter)

            # Проверяем, новая ли это глава для уведомлений
            if manga_id not in last_known_chapters:
                last_known_chapters[manga_id] = chapter_id
                logger.info(f"📝 Зарегистрирована манга: {manga_title}")
            elif last_known_chapters[manga_id] != chapter_id:
                logger.info(f"🆕 Новая глава обнаружена: {manga_title} - Глава {latest_chapter.get('number')}")
                process_new_chapter(manga_title, manga_slug, manga_id, latest_chapter, cover_url)
                last_known_chapters[manga_id] = chapter_id

        logger.info(f"✅ Проверка завершена. Обработано {len(edges)} глав")

        # Пушим свежие главы всем подключённым клиентам
        try:
            fresh = get_recent_chapters_from_api(21)
            socketio.emit('new_chapters', fresh, namespace='/')
        except Exception as _se:
            logger.warning(f"⚠️ SocketIO emit error: {_se}")

    except Exception as e:
        logger.error(f"❌ Ошибка в check_new_chapters: {e}")
        import traceback
        traceback.print_exc()

def _revoke_premium_loans(c, user_id):
    """Удалить все временно активированные Premium-предметы пользователя"""
    col_map = {'frame': 'frame_item_id', 'badge': 'badge_item_id', 'title': 'title_item_id'}
    c.execute(
        '''SELECT ui.item_id, si.type FROM user_items ui
           JOIN shop_items si ON ui.item_id = si.id
           WHERE ui.user_id = ? AND ui.is_premium_loan = 1 AND ui.is_equipped = 1''',
        (user_id,)
    )
    for row in c.fetchall():
        col = col_map.get(row['type'])
        if col:
            c.execute(f'UPDATE user_profile SET {col} = NULL WHERE user_id = ?', (user_id,))
    c.execute('DELETE FROM user_items WHERE user_id = ? AND is_premium_loan = 1', (user_id,))


async def send_daily_digest(hour=22):
    """Отправить ежедневный дайджест новых глав непремиум-пользователям (для заданного часа MSK)"""
    global telegram_app
    if not telegram_app:
        return
    today = (datetime.utcnow() + timedelta(hours=3)).strftime('%Y-%m-%d')
    try:
        conn = get_db()
        c = conn.cursor()
        # Пользователи с очередью, не получавшие дайджест сегодня, и чей preferred час совпадает
        c.execute(
            '''SELECT DISTINCT nq.user_id, u.telegram_id
               FROM notification_queue nq
               JOIN users u ON nq.user_id = u.id
               WHERE (u.last_digest_date IS NULL OR u.last_digest_date < ?)
                 AND COALESCE(u.digest_hour, 22) = ?
                 AND u.is_active = 1 AND u.notifications_enabled = 1''',
            (today, hour)
        )
        users = c.fetchall()
        conn.close()
    except Exception as e:
        print(f"❌ Ошибка получения очереди дайджестов: {e}")
        return

    for row in users:
        user_id = row['user_id']
        telegram_id = row['telegram_id']
        try:
            conn2 = get_db()
            c2 = conn2.cursor()
            c2.execute(
                '''SELECT manga_title, manga_slug, chapter_number, chapter_volume, chapter_name
                   FROM notification_queue WHERE user_id = ?
                   ORDER BY created_at ASC''',
                (user_id,)
            )
            chapters = c2.fetchall()
            if not chapters:
                conn2.close()
                continue

            message = "📚 <b>Новые главы из твоих подписок:</b>\n\n"
            for ch in chapters:
                message += f"📖 <b>{ch['manga_title']}</b>"
                if ch['chapter_number']:
                    message += f" — Глава {ch['chapter_number']}"
                if ch['chapter_volume']:
                    message += f" (Том {ch['chapter_volume']})"
                if ch['chapter_name']:
                    message += f"\n    <i>{ch['chapter_name']}</i>"
                message += "\n"
            message += "\n💎 <i>Оформи Premium — получай мгновенные уведомления с прямыми ссылками на чтение!</i>"

            await telegram_app.bot.send_message(
                chat_id=telegram_id,
                text=message,
                parse_mode='HTML'
            )
            # Обновить дату дайджеста и очистить очередь
            c2.execute('UPDATE users SET last_digest_date = ? WHERE id = ?', (today, user_id))
            c2.execute('DELETE FROM notification_queue WHERE user_id = ?', (user_id,))
            conn2.commit()
        except Exception as e:
            print(f"❌ Ошибка отправки дайджеста пользователю {user_id}: {e}")
        finally:
            try:
                conn2.close()
            except Exception:
                pass


def background_checker():
    """Фоновый процесс проверки"""
    logger.info("🤖 Фоновый мониторинг запущен!")
    check_new_chapters()
    _last_digest_hour_key = None  # "YYYY-MM-DD-HH"

    while True:
        try:
            time.sleep(60)  # Проверка каждую минуту
            check_new_chapters()

            # Ежедневный дайджест: запускаем каждый час для пользователей с matching digest_hour
            now_msk = datetime.utcnow() + timedelta(hours=3)
            hour_key = now_msk.strftime('%Y-%m-%d-%H')
            if _last_digest_hour_key != hour_key:
                _last_digest_hour_key = hour_key
                coro = send_daily_digest(hour=now_msk.hour)
                if _bot_loop and _bot_loop.is_running():
                    asyncio.run_coroutine_threadsafe(coro, _bot_loop)
                else:
                    try:
                        asyncio.run(coro)
                    except Exception:
                        pass

            # Трофей коллекции недели — по понедельникам в полночь
            if datetime.utcnow().weekday() == 0 and datetime.utcnow().hour == 0:
                award_weekly_collection_trophy()

            # Проверяем истёкшие Premium подписки
            try:
                now_iso = datetime.utcnow().isoformat()
                conn_bg = get_db()
                c_bg = conn_bg.cursor()
                c_bg.execute(
                    'SELECT id FROM users WHERE is_premium=1 AND premium_expires_at IS NOT NULL AND premium_expires_at < ?',
                    (now_iso,)
                )
                expired = c_bg.fetchall()
                for row in expired:
                    c_bg.execute('UPDATE users SET is_premium=0, premium_expires_at=NULL WHERE id=?', (row['id'],))
                    _revoke_premium_loans(c_bg, row['id'])
                if expired:
                    conn_bg.commit()
                    logger.info(f"⏰ Premium истёк у {len(expired)} пользователей")
                conn_bg.close()
            except Exception as e_prem:
                logger.error(f"❌ Ошибка проверки Premium: {e_prem}")

            # Удаляем истёкшие временные предметы
            try:
                conn_tmp = get_db()
                now_iso = datetime.utcnow().isoformat()
                exp_rows = conn_tmp.execute(
                    '''SELECT ui.id, ui.user_id, ui.item_id, si.type, si.name
                       FROM user_items ui JOIN shop_items si ON ui.item_id = si.id
                       WHERE ui.expires_at IS NOT NULL AND ui.expires_at < ?''',
                    (now_iso,)
                ).fetchall()
                col_map = {'frame': 'frame_item_id', 'badge': 'badge_item_id', 'title': 'title_item_id'}
                for row in exp_rows:
                    col = col_map.get(row['type'])
                    if col:
                        conn_tmp.execute(
                            f'UPDATE user_profile SET {col}=NULL WHERE user_id=? AND {col}=?',
                            (row['user_id'], row['item_id'])
                        )
                    conn_tmp.execute('DELETE FROM user_items WHERE id=?', (row['id'],))
                if exp_rows:
                    conn_tmp.commit()
                conn_tmp.close()
                for row in exp_rows:
                    create_site_notification(
                        row['user_id'], 'item_expired',
                        f'Предмет истёк: {row["name"]}',
                        'Временный предмет удалён из инвентаря', '/shop'
                    )
            except Exception as e_tmp:
                logger.error(f"❌ Ошибка очистки временных предметов: {e_tmp}")
        except Exception as e:
            logger.error(f"❌ Ошибка в background_checker: {e}")
            time.sleep(60)
# ==================== TELEGRAM BOT ====================

async def premium_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /premium <user_id> — выдать/снять Premium (только для администраторов)"""
    if update.effective_user.id not in ADMIN_TELEGRAM_IDS:
        await update.message.reply_text("❌ У вас нет доступа к этой команде.")
        return

    args = context.args
    if not args:
        await update.message.reply_text(
            "Использование: /premium <user_id> [days]\n"
            "Пример: /premium 42 30\n"
            "По умолчанию 30 дней. Повторная команда снимает Premium."
        )
        return

    target = args[0].lstrip('@')
    days = 30
    if len(args) > 1:
        try:
            days = int(args[1])
        except ValueError:
            await update.message.reply_text("❌ Неверное значение дней")
            return

    conn = get_db()
    c = conn.cursor()

    if target.isdigit():
        c.execute('SELECT id, telegram_first_name, telegram_username, is_premium FROM users WHERE id = ?', (int(target),))
    else:
        c.execute('SELECT id, telegram_first_name, telegram_username, is_premium FROM users WHERE telegram_username = ?', (target,))

    user = c.fetchone()
    if not user:
        conn.close()
        await update.message.reply_text("❌ Пользователь не найден")
        return

    now = datetime.utcnow().isoformat()
    name = user['telegram_first_name'] or user['telegram_username'] or f"ID {user['id']}"

    if user['is_premium']:
        # Снять Premium
        c.execute('UPDATE users SET is_premium=0, premium_expires_at=NULL WHERE id=?', (user['id'],))
        _revoke_premium_loans(c, user['id'])
        conn.commit()
        conn.close()
        await update.message.reply_text(f"❌ Premium снят для {name} (ID: {user['id']})")
    else:
        # Выдать Premium на days дней
        expires = (datetime.utcnow() + timedelta(days=days)).isoformat()
        c.execute(
            'UPDATE users SET is_premium=1, premium_granted_at=?, premium_expires_at=? WHERE id=?',
            (now, expires, user['id'])
        )
        conn.commit()
        conn.close()
        await update.message.reply_text(
            f"✅ Premium выдан для {name} (ID: {user['id']}) на {days} дней\n"
            f"Истекает: {expires[:10]}"
        )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start - регистрация/вход"""
    telegram_id = update.effective_user.id
    username = update.effective_user.username
    first_name = update.effective_user.first_name
    last_name = update.effective_user.last_name

    # Регистрируем или получаем пользователя
    user = get_or_create_user_by_telegram(telegram_id, username, first_name, last_name)

    if not user:
        await update.message.reply_text("❌ Ошибка регистрации. Попробуйте позже.")
        return

    # ?start=buy — сразу показываем покупку монет
    if context.args and context.args[0] == 'buy':
        await buy_command(update, context)
        return

    # ?start=<referral_code> — реферальная программа
    if context.args and len(context.args[0]) >= 8:
        ref_code = context.args[0].upper()
        try:
            ref_conn = get_db()
            referrer = ref_conn.execute('SELECT id FROM users WHERE referral_code=?', (ref_code,)).fetchone()
            if referrer and referrer['id'] != user['id']:
                existing = ref_conn.execute(
                    'SELECT id FROM referrals WHERE referred_id=?', (user['id'],)
                ).fetchone()
                if not existing:
                    ref_conn.execute(
                        'INSERT OR IGNORE INTO referrals (referrer_id, referred_id, rewarded) VALUES (?,?,1)',
                        (referrer['id'], user['id'])
                    )
                    ref_conn.execute(
                        'UPDATE user_stats SET xp=xp+100, coins=coins+100 WHERE user_id=?',
                        (referrer['id'],)
                    )
                    ref_conn.execute(
                        'INSERT INTO xp_log (user_id, reason, ref_id, amount) VALUES (?,?,?,?)',
                        (referrer['id'], 'referral', str(user['id']), 100)
                    )
                    ref_conn.commit()
            ref_conn.close()
        except Exception as _re:
            logger.warning(f"Referral processing error: {_re}")

    login_url = f"{SITE_URL}/login/{user['login_token']}"
    webapp_url = SITE_URL

    open_btn = (
        InlineKeyboardButton("📱 Открыть приложение", web_app=WebAppInfo(url=webapp_url))
        if webapp_url.startswith("https://")
        else InlineKeyboardButton("📱 Открыть приложение", url=webapp_url)
    )
    keyboard = [
        [open_btn],
        [InlineKeyboardButton("📝 Войти на сайте", url=login_url)],
        [InlineKeyboardButton("🔍 Поиск манги", callback_data="search_manga")],
        [InlineKeyboardButton("⭐ Мои подписки", callback_data="my_subscriptions")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message = f"👋 Привет, {first_name or username}!\n\n"
    message += "🤖 Добро пожаловать в Manga Reader Bot!\n\n"
    message += "✅ Вы успешно зарегистрированы!\n"
    message += f"🆔 Ваш ID: {user['id']}\n\n"
    message += "Нажмите кнопку ниже, чтобы открыть сайт и начать читать мангу."

    await update.message.reply_text(message, reply_markup=reply_markup)

async def search_manga_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /search - поиск манги"""
    telegram_id = update.effective_user.id
    user = get_user_by_telegram_id(telegram_id)

    if not user:
        await update.effective_message.reply_text("❌ Сначала зарегистрируйтесь через /start")
        return

    context.user_data['waiting_for_search'] = True
    await update.effective_message.reply_text("🔍 Введите название манги для поиска:")

async def handle_search_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка сообщения для поиска"""
    text = update.message.text or ''

    # Ввод получателя для подарка
    if context.user_data.get('waiting_for_gift_username'):
        context.user_data['waiting_for_gift_username'] = False
        username = text.lstrip('@').strip()
        if not username:
            await update.message.reply_text("❌ Укажите username получателя")
            return
        recipient_id, recipient_name = await _resolve_recipient(username)
        if not recipient_id:
            await update.message.reply_text(f"❌ Пользователь @{username} не найден на BubbleManga")
            return
        keyboard = [
            [InlineKeyboardButton("🎁 1 месяц — 50 ⭐", callback_data=f"gift_pkg:{username}:30")],
            [InlineKeyboardButton("🎁 3 месяца — 130 ⭐", callback_data=f"gift_pkg:{username}:90")],
            [InlineKeyboardButton("🎁 1 год — 450 ⭐", callback_data=f"gift_pkg:{username}:365")],
        ]
        await update.message.reply_text(
            f"🎁 Подарить Premium пользователю *{recipient_name}*\n\nВыберите период:",
            parse_mode='MarkdownV2',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if not context.user_data.get('waiting_for_search'):
        return
    
    telegram_id = update.effective_user.id
    user = get_user_by_telegram_id(telegram_id)
    query = update.message.text
    
    if not user:
        await update.message.reply_text("❌ Ошибка пользователя")
        return
    
    if len(query) < 2:
        await update.message.reply_text("❌ Введите минимум 2 символа")
        return
    
    # Сохраняем историю поиска
    save_search_history(user['id'], query)
    
    await update.message.reply_text(f"📎 Ищу мангу по запросу: {query}...")
    
    # Ищем мангу
    results = search_manga_api(query, 5)
    
    if not results:
        await update.message.reply_text("❌ Ничего не найдено")
        context.user_data['waiting_for_search'] = False
        return
    
    # Отправляем результаты
    message = f"📚 Найдено манг: {len(results)}\n\n"
    
    keyboard = []
    for i, manga in enumerate(results[:10], 1):
        message += f"{i}. {manga['manga_title']}\n"
        
        # Создаем кнопки для подписки
        keyboard.append([
            InlineKeyboardButton(
                f"{i}. {manga['manga_title'][:20]}...",
                callback_data=f"subscribe_{manga['manga_id']}"
            )
        ])
    
    # Добавляем кнопку для открытия полного поиска на сайте
    keyboard.append([
        InlineKeyboardButton(
            "🌐 Открыть все результаты на сайте",
            url=f"{SITE_URL}/search?q={query}"
        )
    ])
    
    keyboard.append([
        InlineKeyboardButton("◀️ Назад", callback_data="back_to_start")
    ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(message, reply_markup=reply_markup)
    context.user_data['waiting_for_search'] = False

async def subscribe_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка подписки на мангу"""
    query = update.callback_query
    await query.answer()
    
    if not query.data.startswith('subscribe_'):
        return
    
    manga_id = query.data.replace('subscribe_', '')
    telegram_id = update.effective_user.id
    user = get_user_by_telegram_id(telegram_id)
    
    if not user:
        await query.edit_message_text("❌ Ошибка пользователя")
        return
    
    # Получаем информацию о манге
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT manga_title FROM manga WHERE manga_id = ?', (manga_id,))
    manga = c.fetchone()
    conn.close()
    
    if not manga:
        await query.edit_message_text("❌ Манга не найдена")
        return
    
    subscribed = toggle_subscription(user['id'], manga_id)
    
    if subscribed:
        message = f"✅ Вы подписались на: {manga['manga_title']}"
    else:
        message = f"❌ Вы отписались от: {manga['manga_title']}"
    
    await query.edit_message_text(message)

async def my_subscriptions_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать подписки пользователя"""
    query = update.callback_query
    await query.answer()
    
    telegram_id = update.effective_user.id
    user = get_user_by_telegram_id(telegram_id)
    
    if not user:
        await query.edit_message_text("❌ Сначала зарегистрируйтесь через /start")
        return
    
    subscriptions = get_user_subscriptions(user['id'], 10)
    
    if not subscriptions:
        await query.edit_message_text("🔭 У вас пока нет подписок.\n\nИспользуйте /search для поиска манги.")
        return
    
    message = f"⭐ Ваши подписки ({len(subscriptions)}):\n\n"
    
    keyboard = []
    for i, manga in enumerate(subscriptions, 1):
        message += f"{i}. {manga['manga_title']}\n"
        
        keyboard.append([
            InlineKeyboardButton(
                f"❌ Отписаться от {manga['manga_title'][:15]}...",
                callback_data=f"unsubscribe_{manga['manga_id']}"
            )
        ])
    
    keyboard.append([
        InlineKeyboardButton("🌐 Открыть на сайте",
                           url=f"{SITE_URL}/login/{user['login_token']}")
    ])
    keyboard.append([
        InlineKeyboardButton("◀️ Назад", callback_data="back_to_start")
    ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(message, reply_markup=reply_markup)

async def unsubscribe_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка отписки от манги"""
    query = update.callback_query
    await query.answer()
    
    if not query.data.startswith('unsubscribe_'):
        return
    
    manga_id = query.data.replace('unsubscribe_', '')
    telegram_id = update.effective_user.id
    user = get_user_by_telegram_id(telegram_id)
    
    if not user:
        await query.edit_message_text("❌ Ошибка пользователя")
        return
    
    subscribed = toggle_subscription(user['id'], manga_id)
    
    # Возвращаемся к списку подписок
    await my_subscriptions_callback(update, context)

async def back_to_start_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Вернуться к стартовому меню"""
    query = update.callback_query
    await query.answer()
    
    telegram_id = update.effective_user.id
    user = get_user_by_telegram_id(telegram_id)
    
    if not user:
        await query.edit_message_text("❌ Ошибка пользователя")
        return
    
    login_url = f"{SITE_URL}/login/{user['login_token']}"
    webapp_url = SITE_URL

    keyboard = [
        [InlineKeyboardButton("🌐 Открыть сайт", url=webapp_url)],
        [InlineKeyboardButton("📝 Войти на сайте", url=login_url)],
        [InlineKeyboardButton("🔍 Поиск манги", callback_data="search_manga")],
        [InlineKeyboardButton("⭐ Мои подписки", callback_data="my_subscriptions")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    message = f"👋 С возвращением!\n\n"
    message += "Выберите действие:"
    
    await query.edit_message_text(message, reply_markup=reply_markup)

async def buy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /buy — показывает inline кнопки с пакетами монет"""
    keyboard = [
        [InlineKeyboardButton(f"💰 {p['coins']} монет — {p['stars']} ⭐", callback_data=f"buy_coins:{p['id']}")]
        for p in COIN_PACKAGES
    ]
    text = (
        "⭐ *Купить монеты за Telegram Stars*\n\n"
        "Монеты используются в магазине BubbleManga:\n"
        "🖼 Аватары, рамки, фоны профиля\n"
        "🏷 Значки и другие украшения\n\n"
        "💡 *Как это работает?*\n"
        "1\\. Выберите пакет ниже\n"
        "2\\. Нажмите кнопку *Оплатить* в инвойсе\n"
        "3\\. Монеты зачислятся мгновенно\\!\n\n"
        "Выберите пакет:"
    )
    await update.effective_message.reply_text(
        text,
        parse_mode='MarkdownV2',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def pre_checkout_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обязательный ответ на pre_checkout_query"""
    await update.pre_checkout_query.answer(ok=True)


async def _handle_gift_premium_payment(update, payment, payload, payment_id):
    """Обрабатывает платёж подарочного Premium. Payload: gift_premium:{rid}:{days}:{sid}"""
    try:
        _, recipient_id_str, days_str, sender_id_str = payload.split(':', 3)
        recipient_id = int(recipient_id_str)
        days = int(days_str)
        sender_id = int(sender_id_str)
    except (ValueError, TypeError):
        await update.message.reply_text("❌ Ошибка формата подарка.")
        return

    # Idempotency
    conn = get_db()
    c = conn.cursor()
    existing = c.execute('SELECT id FROM premium_gifts WHERE payment_id=?', (payment_id,)).fetchone()
    conn.close()
    if existing:
        await update.message.reply_text("✅ Подарок уже был обработан.")
        return

    # Записываем подарок
    conn = get_db()
    c = conn.cursor()
    c.execute(
        'INSERT OR IGNORE INTO premium_gifts (sender_id, recipient_id, days, stars_paid, payment_id) VALUES (?,?,?,?,?)',
        (sender_id, recipient_id, days, payment.total_amount, payment_id)
    )
    conn.commit()
    conn.close()

    # Определяем package_id по дням
    pkg_map = {30: 'premium_1m', 90: 'premium_3m', 365: 'premium_12m'}
    pkg_id = pkg_map.get(days, 'premium_1m')
    label_map = {30: '1 месяц', 90: '3 месяца', 365: '1 год'}
    label = label_map.get(days, f'{days} дней')

    # Выдаём Premium получателю
    _grant_premium(recipient_id, pkg_id, f'gift_{payment_id}', 'stars_gift')

    # XP отправителю
    award_xp(sender_id, 50, 'gift_premium', ref_id=payment_id)

    # Уведомление получателю на сайте
    create_site_notification(
        recipient_id, 'gift_premium',
        'Вам подарили Premium!',
        f'на {label}', '/shop'
    )

    # Telegram уведомление получателю
    try:
        conn = get_db()
        c = conn.cursor()
        rec_row = c.execute('SELECT telegram_id FROM users WHERE id=?', (recipient_id,)).fetchone()
        conn.close()
        if rec_row and rec_row['telegram_id'] and _bot_loop and _bot_loop.is_running():
            async def _notify():
                try:
                    await context.bot.send_message(
                        chat_id=rec_row['telegram_id'],
                        text=f"🎁 Вам подарили Premium на {label}!\nПриятного чтения на BubbleManga!"
                    )
                except Exception:
                    pass
            asyncio.run_coroutine_threadsafe(_notify(), _bot_loop)
    except Exception:
        pass

    await update.message.reply_text(f"🎁 Подарок отправлен! Premium на {label} зачислен получателю.")


async def _resolve_recipient(username):
    """Ищет пользователя по telegram_username. Возвращает (user_id, display_name) или (None, None)."""
    conn = get_db()
    c = conn.cursor()
    uname = username.lstrip('@')
    row = c.execute(
        'SELECT id, telegram_first_name, telegram_username FROM users WHERE telegram_username=?', (uname,)
    ).fetchone()
    conn.close()
    if not row:
        return None, None
    display = row['telegram_first_name'] or row['telegram_username'] or f'ID {row["id"]}'
    return row['id'], display


async def _send_gift_invoice(msg_or_query, context, sender_id, recipient_id, recipient_name, days):
    """Отправляет Stars invoice для подарочного Premium."""
    label_map = {30: '1 месяц', 90: '3 месяца', 365: '1 год'}
    stars_map = {30: 50, 90: 130, 365: 450}
    label = label_map.get(days, f'{days} дней')
    stars = stars_map.get(days, 50)
    payload = f'gift_premium:{recipient_id}:{days}:{sender_id}'
    chat_id = msg_or_query.chat_id if hasattr(msg_or_query, 'chat_id') else msg_or_query.message.chat_id
    await context.bot.send_invoice(
        chat_id=chat_id,
        title=f'Premium на {label} для {recipient_name}',
        description=f'Подарок Premium BubbleManga на {label}',
        payload=payload,
        currency='XTR',
        provider_token='',
        prices=[LabeledPrice(label=f'Premium {label}', amount=stars)],
    )


async def gift_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /gift [@username] [30|90|365]"""
    telegram_id = update.effective_user.id
    conn = get_db()
    c = conn.cursor()
    sender_row = c.execute('SELECT id FROM users WHERE telegram_id=?', (telegram_id,)).fetchone()
    conn.close()
    if not sender_row:
        await update.message.reply_text("❌ Сначала войдите на сайт BubbleManga через /start")
        return
    sender_id = sender_row['id']

    args = context.args or []

    if not args:
        context.user_data['waiting_for_gift_username'] = True
        await update.message.reply_text(
            "🎁 *Подарить Premium*\n\nВведите @username получателя:",
            parse_mode='MarkdownV2'
        )
        return

    username = args[0].lstrip('@')
    recipient_id, recipient_name = await _resolve_recipient(username)
    if not recipient_id:
        await update.message.reply_text(f"❌ Пользователь @{username} не найден на BubbleManga")
        return

    if len(args) >= 2:
        try:
            days = int(args[1])
            if days not in (30, 90, 365):
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Укажите период: 30, 90 или 365 дней")
            return
        await _send_gift_invoice(update.message, context, sender_id, recipient_id, recipient_name, days)
    else:
        # Показать кнопки выбора периода
        keyboard = [
            [InlineKeyboardButton(f"🎁 1 месяц — 50 ⭐", callback_data=f"gift_pkg:{username}:30")],
            [InlineKeyboardButton(f"🎁 3 месяца — 130 ⭐", callback_data=f"gift_pkg:{username}:90")],
            [InlineKeyboardButton(f"🎁 1 год — 450 ⭐", callback_data=f"gift_pkg:{username}:365")],
        ]
        await update.message.reply_text(
            f"🎁 Подарить Premium пользователю *{recipient_name}*\n\nВыберите период:",
            parse_mode='MarkdownV2',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )


async def successful_payment_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начисляет монеты после успешной оплаты Stars"""
    payment = update.message.successful_payment
    payload = payment.invoice_payload  # format: "{package_id}:{user_id}"
    payment_id = payment.telegram_payment_charge_id

    # Подарочный Premium обрабатываем отдельно
    if payload.startswith('gift_premium:'):
        await _handle_gift_premium_payment(update, payment, payload, payment_id)
        return

    try:
        package_id, user_id_str = payload.rsplit(':', 1)
        user_id = int(user_id_str)
    except (ValueError, AttributeError):
        await update.message.reply_text("Ошибка обработки платежа. Обратитесь к администратору.")
        return

    pkg = next((p for p in COIN_PACKAGES if p['id'] == package_id), None)
    if not pkg:
        await update.message.reply_text("Пакет не найден. Обратитесь к администратору.")
        return

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute(
            'INSERT OR IGNORE INTO coin_purchases (user_id, package_id, stars_paid, coins_received, payment_id) VALUES (?, ?, ?, ?, ?)',
            (user_id, package_id, payment.total_amount, pkg['coins'], payment_id)
        )
        if c.rowcount > 0:
            c.execute('UPDATE user_stats SET coins = coins + ? WHERE user_id = ?', (pkg['coins'], user_id))
        conn.commit()
    finally:
        conn.close()

    await update.message.reply_text(
        f"✅ Оплата прошла успешно!\n\n💰 Начислено {pkg['coins']} монет.\nСпасибо за поддержку!"
    )


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback кнопок"""
    query = update.callback_query
    await query.answer()

    if query.data == "my_subscriptions":
        await my_subscriptions_callback(update, context)
    elif query.data == "search_manga":
        await search_manga_command(update, context)
    elif query.data.startswith("subscribe_"):
        await subscribe_callback(update, context)
    elif query.data.startswith("unsubscribe_"):
        await unsubscribe_callback(update, context)
    elif query.data == "back_to_start":
        await back_to_start_callback(update, context)
    elif query.data.startswith("buy_coins:"):
        pkg_id = query.data[len("buy_coins:"):]
        pkg = next((p for p in COIN_PACKAGES if p['id'] == pkg_id), None)
        if not pkg:
            await query.answer("Пакет не найден", show_alert=True)
            return
        telegram_id = query.from_user.id
        conn = get_db()
        c = conn.cursor()
        c.execute('SELECT id FROM users WHERE telegram_id = ?', (telegram_id,))
        row = c.fetchone()
        conn.close()
        if not row:
            await query.answer("Сначала войдите на сайт BubbleManga", show_alert=True)
            return
        user_id = row['id']
        payload = f"{pkg['id']}:{user_id}"
        await context.bot.send_invoice(
            chat_id=query.message.chat_id,
            title=pkg['label'],
            description=f"{pkg['coins']} монет для BubbleManga",
            payload=payload,
            currency="XTR",
            provider_token="",
            prices=[LabeledPrice(label=pkg['label'], amount=pkg['stars'])],
        )
    elif query.data.startswith("gift_pkg:"):
        _, username, days_str = query.data.split(":", 2)
        try:
            days = int(days_str)
        except ValueError:
            await query.answer("Ошибка", show_alert=True)
            return
        telegram_id = query.from_user.id
        conn = get_db()
        c = conn.cursor()
        sender_row = c.execute('SELECT id FROM users WHERE telegram_id=?', (telegram_id,)).fetchone()
        conn.close()
        if not sender_row:
            await query.answer("Сначала войдите на сайт BubbleManga", show_alert=True)
            return
        sender_id = sender_row['id']
        recipient_id, recipient_name = await _resolve_recipient(username)
        if not recipient_id:
            await query.answer(f"Пользователь @{username} не найден", show_alert=True)
            return
        await _send_gift_invoice(query.message, context, sender_id, recipient_id, recipient_name, days)


def run_telegram_bot():
    """Запуск Telegram бота"""
    global telegram_app

    def start_bot():
        """Запуск бота в отдельном потоке с явным созданием event loop"""
        global _bot_loop, telegram_app
        try:
            # Явно создаем новый event loop для этого потока
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            _bot_loop = loop  # сохраняем для run_coroutine_threadsafe
            
            # Теперь создаем приложение
            telegram_app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
            
            # Команды
            telegram_app.add_handler(CommandHandler("start", start_command))
            telegram_app.add_handler(CommandHandler("search", search_manga_command))
            telegram_app.add_handler(CommandHandler("premium", premium_command))
            telegram_app.add_handler(CommandHandler("buy", buy_command))
            telegram_app.add_handler(CommandHandler("gift", gift_command))

            # Callback кнопки
            telegram_app.add_handler(CallbackQueryHandler(handle_callback))

            # Payments
            telegram_app.add_handler(PreCheckoutQueryHandler(pre_checkout_handler))
            telegram_app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_handler))

            # Сообщения
            telegram_app.add_handler(MessageHandler(
                filters.TEXT & ~filters.COMMAND,
                handle_search_message
            ))
            
            print("🤖 Telegram бот запущен!")
            
            # Запускаем polling с явным указанием loop
            loop.run_until_complete(telegram_app.initialize())
            loop.run_until_complete(telegram_app.start())
            
            # Запускаем updater
            loop.run_until_complete(telegram_app.updater.start_polling(
                drop_pending_updates=True
            ))
            
            # Запускаем основной loop
            print("🤖 Бот запущен и работает...")
            loop.run_forever()
            
        except Exception as e:
            print(f"❌ Ошибка запуска Telegram бота: {e}")
            import traceback
            traceback.print_exc()
    
    # Запускаем бот в отдельном потоке
    bot_thread = threading.Thread(target=start_bot, daemon=True, name="TelegramBot")
    bot_thread.start()
    return bot_thread

# ==================== FLASK ROUTES ====================

@app.route('/')
def index():
    user_id = session.get('user_id')

    reading = []
    subscriptions = []

    if user_id:
        reading = get_user_reading(user_id, 12)
        subscriptions = get_user_subscriptions(user_id, 12)

    # Жанры/теги для секции "Все лейблы"
    genres = [
        {'icon': '⚡', 'name': 'Система'},
        {'icon': '❤️', 'name': 'Романтика'},
        {'icon': '🌀', 'name': 'Исекай'},
        {'icon': '👊', 'name': 'Боевик'},
        {'icon': '🤣', 'name': 'Комедия'},
        {'icon': '🎭', 'name': 'Драма'},
        {'icon': '🔮', 'name': 'Фэнтези'},
        {'icon': '👻', 'name': 'Ужасы'},
        {'icon': '🔎', 'name': 'Детектив'},
        {'icon': '💼', 'name': 'Повседневность'},
        {'icon': '🎓', 'name': 'Школа'},
        {'icon': '👑', 'name': 'Царей'}
    ]

    return render_template('index.html',
                          reading=reading,
                          subscriptions=subscriptions,
                          user_id=user_id,
                          genres=genres)


@app.route('/api/auth/webapp', methods=['POST'])
def api_auth_webapp():
    """Аутентификация через Telegram WebApp initData (HMAC-SHA256)."""
    body = request.get_json(silent=True) or {}
    init_data_raw = (body.get('initData') or '').strip()
    if not init_data_raw:
        return jsonify({'error': 'no initData'}), 400

    try:
        parsed = dict(urllib.parse.parse_qsl(init_data_raw, keep_blank_values=True))
        hash_from_tg = parsed.pop('hash', None)
        if not hash_from_tg:
            return jsonify({'error': 'no hash'}), 400

        check_str = '\n'.join(f'{k}={v}' for k, v in sorted(parsed.items()))
        secret = hmac.new(b'WebAppData', TELEGRAM_BOT_TOKEN.encode(), hashlib.sha256).digest()
        expected = hmac.new(secret, check_str.encode(), hashlib.sha256).hexdigest()

        if not hmac.compare_digest(expected, hash_from_tg):
            return jsonify({'error': 'invalid hash'}), 403
    except Exception as ex:
        logger.warning(f"⚠️ webapp auth parse error: {ex}")
        return jsonify({'error': 'parse error'}), 400

    try:
        tg_user = json.loads(parsed.get('user', '{}'))
    except Exception:
        return jsonify({'error': 'bad user json'}), 400

    tg_id = tg_user.get('id')
    if not tg_id:
        return jsonify({'error': 'no user id'}), 400

    user = get_or_create_user_by_telegram(
        tg_id,
        tg_user.get('username'),
        tg_user.get('first_name'),
        tg_user.get('last_name'),
    )
    if not user:
        return jsonify({'error': 'db error'}), 500

    already = session.get('user_id') == user['id']
    session['user_id'] = user['id']
    session['username'] = (
        user.get('telegram_username') or
        user.get('telegram_first_name') or
        f"User_{user['id']}"
    )
    session.permanent = True
    return jsonify({'ok': True, '_already': already, 'user_id': user['id'], 'username': session['username']})


@app.route('/api/home/recent')
def api_home_recent():
    data = get_recent_chapters_from_api(21)
    resp = make_response(jsonify(data))
    resp.headers['Cache-Control'] = 'public, max-age=300'
    return resp


@app.route('/api/home/spotlights')
def api_home_spotlights():
    spotlights_data = get_cached_spotlights(ttl_seconds=1800)
    resp = make_response(jsonify(spotlights_data.get('spotlights', {})))
    resp.headers['Cache-Control'] = 'public, max-age=1800'
    return resp


@app.route('/api/home/popular')
def api_home_popular():
    period = request.args.get('period', 'MONTH').upper()
    if period not in ('DAY', 'WEEK', 'MONTH'):
        period = 'MONTH'
    raw = get_popular_manga_from_api(period, 12)
    # Нормализуем поля под формат, который ожидает buildSliderItems на фронте
    data = [
        {
            'id':        m.get('manga_id'),
            'slug':      m.get('manga_slug'),
            'title':     m.get('manga_title'),
            'cover_url': m.get('cover_url'),
            'score':     m.get('score', 0),
            'type':      'MANGA',
        }
        for m in raw
    ]
    resp = make_response(jsonify(data))
    resp.headers['Cache-Control'] = 'public, max-age=600'
    return resp


@app.route('/sw.js')
def service_worker():
    sw_content = """
const CACHE = 'bubblemanga-v1';
const IMG_CACHE = 'bubblemanga-images-v1';

self.addEventListener('install', e => {
    self.skipWaiting();
});

self.addEventListener('activate', e => {
    e.waitUntil(
        caches.keys().then(keys =>
            Promise.all(keys
                .filter(k => k !== CACHE && k !== IMG_CACHE)
                .map(k => caches.delete(k))
            )
        )
    );
    self.clients.claim();
});

async function cacheFirst(req, cacheName) {
    const cached = await caches.match(req);
    if (cached) return cached;
    const resp = await fetch(req);
    if (resp.ok) {
        const cache = await caches.open(cacheName);
        cache.put(req, resp.clone());
    }
    return resp;
}

async function staleWhileRevalidate(req, cacheName) {
    const cache = await caches.open(cacheName);
    const cached = await cache.match(req);
    const fetchPromise = fetch(req).then(resp => {
        if (resp.ok) cache.put(req, resp.clone());
        return resp;
    }).catch(() => null);
    return cached || fetchPromise;
}

async function networkFirst(req, cacheName) {
    try {
        const resp = await fetch(req);
        if (resp.ok) {
            const cache = await caches.open(cacheName);
            cache.put(req, resp.clone());
        }
        return resp;
    } catch {
        const cached = await caches.match(req);
        return cached || new Response('Нет подключения', { status: 503 });
    }
}

self.addEventListener('fetch', e => {
    const { request } = e;
    const url = new URL(request.url);

    // Картинки (обложки, страницы глав) — cache-first
    if (request.destination === 'image') {
        e.respondWith(cacheFirst(request, IMG_CACHE));
        return;
    }

    // API главной — stale-while-revalidate
    if (url.pathname.startsWith('/api/home/')) {
        e.respondWith(staleWhileRevalidate(request, CACHE));
        return;
    }

    // HTML страницы — network-first с fallback
    if (request.mode === 'navigate') {
        e.respondWith(networkFirst(request, CACHE));
        return;
    }
});
""".strip()
    resp = make_response(sw_content, 200)
    resp.headers['Content-Type'] = 'application/javascript'
    resp.headers['Service-Worker-Allowed'] = '/'
    resp.headers['Cache-Control'] = 'no-cache'
    return resp

@app.route('/login/<token>')
def login_token(token):
    """Вход по токену из Telegram"""
    user = get_user_by_token(token)
    if user:
        session['user_id'] = user['id']
        session['username'] = user['telegram_username'] or user['telegram_first_name'] or f"User_{user['id']}"
        session.permanent = True
        return redirect(url_for('index'))
    return "Неверный или устаревший токен. Получите новый через Telegram бота.", 403

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

@app.route('/search')
def search():
    _PER = 25

    query   = request.args.get('q', '').strip()
    user_id = session.get('user_id')

    if not query or len(query) < 2:
        return render_template('search.html',
                               query=query, results=[],
                               total=0, user_id=user_id)

    if user_id:
        save_search_history(user_id, query)

    like   = f'%{query}%'
    starts = f'{query}%'

    conn = get_db()
    c    = conn.cursor()

    c.execute('''SELECT COUNT(*) FROM manga
                 WHERE manga_title LIKE ? OR original_name LIKE ? OR manga_slug LIKE ?''',
              (like, like, like))
    total = c.fetchone()[0]

    results = []
    if total > 0:
        c.execute('''
            SELECT manga_id, manga_slug, manga_title, manga_type,
                   manga_status, cover_url, rating, score, views, chapters_count, last_updated,
                   CASE
                     WHEN lower(manga_title) = lower(?)    THEN 10
                     WHEN lower(manga_title) LIKE lower(?) THEN 5
                     ELSE 1
                   END AS _rel
            FROM manga
            WHERE manga_title LIKE ? OR original_name LIKE ? OR manga_slug LIKE ?
            ORDER BY _rel DESC, COALESCE(score, 0) DESC
            LIMIT ?
        ''', (query, starts, like, like, like, _PER))
        results = [dict(r) for r in c.fetchall()]
        for r in results:
            r.pop('_rel', None)

    conn.close()

    # Fallback на API только если БД пуста
    if total == 0:
        api_results = search_manga_api(query, _PER)
        total   = len(api_results)
        results = api_results

    return render_template('search.html',
                           query=query, results=results,
                           total=total, user_id=user_id)

@app.route('/api/search')
def api_search():
    """AJAX-поиск манги с сортировкой и offset-пагинацией."""
    _PER = 25
    _SORT = {
        'relevance': None,                            # специальная логика
        'score':     'COALESCE(score, 0) DESC',
        'views':     'COALESCE(views, 0) DESC',
        'chapters':  'COALESCE(chapters_count, 0) DESC',
        'updated':   "COALESCE(last_updated, '1970') DESC",
    }

    query  = request.args.get('q', '').strip()
    offset = max(0, request.args.get('offset', 0, type=int))
    limit  = min(max(1, request.args.get('limit', _PER, type=int)), 100)
    sort   = request.args.get('sort', 'relevance')
    if sort not in _SORT:
        sort = 'relevance'

    if not query or len(query) < 2:
        return jsonify({'results': [], 'total': 0, 'has_more': False})

    like   = f'%{query}%'
    starts = f'{query}%'

    conn = get_db()
    c    = conn.cursor()

    c.execute('''SELECT COUNT(*) FROM manga
                 WHERE manga_title LIKE ? OR original_name LIKE ? OR manga_slug LIKE ?''',
              (like, like, like))
    total = c.fetchone()[0]

    if total == 0:
        conn.close()
        return jsonify({'results': [], 'total': 0, 'has_more': False})

    if sort == 'relevance':
        c.execute('''
            SELECT manga_id, manga_slug, manga_title, manga_type, manga_status,
                   cover_url, rating, score, views, chapters_count, last_updated,
                   CASE
                     WHEN lower(manga_title) = lower(?)    THEN 10
                     WHEN lower(manga_title) LIKE lower(?) THEN 5
                     ELSE 1
                   END AS _rel
            FROM manga
            WHERE manga_title LIKE ? OR original_name LIKE ? OR manga_slug LIKE ?
            ORDER BY _rel DESC, COALESCE(score, 0) DESC
            LIMIT ? OFFSET ?
        ''', (query, starts, like, like, like, limit, offset))
    else:
        order_sql = _SORT[sort]
        c.execute(f'''
            SELECT manga_id, manga_slug, manga_title, manga_type, manga_status,
                   cover_url, rating, score, views, chapters_count, last_updated
            FROM manga
            WHERE manga_title LIKE ? OR original_name LIKE ? OR manga_slug LIKE ?
            ORDER BY {order_sql}
            LIMIT ? OFFSET ?
        ''', (like, like, like, limit, offset))

    rows = [dict(r) for r in c.fetchall()]
    conn.close()

    for r in rows:
        r.pop('_rel', None)

    return jsonify({
        'results': rows,
        'total':    total,
        'has_more': offset + len(rows) < total,
    })


@app.route('/api/search/suggestions')
def search_suggestions():
    query   = request.args.get('q', '').strip()
    user_id = session.get('user_id')

    if len(query) < 2:
        return jsonify([])

    # Сначала — тайтлы из каталога БД
    conn = get_db()
    c    = conn.cursor()
    c.execute('''SELECT manga_title FROM manga
                 WHERE manga_title LIKE ? OR original_name LIKE ?
                 ORDER BY COALESCE(score, 0) DESC
                 LIMIT 8''', (f'{query}%', f'{query}%'))
    from_catalog = [row[0] for row in c.fetchall()]

    # Добавляем из истории поиска (только если мало результатов)
    from_history = []
    if len(from_catalog) < 5:
        c.execute('''SELECT DISTINCT query FROM search_history
                     WHERE query LIKE ?
                     ORDER BY created_at DESC
                     LIMIT ?''', (f'{query}%', 8 - len(from_catalog)))
        from_history = [row[0] for row in c.fetchall()]
    conn.close()

    # Объединяем, дедупликация с сохранением порядка
    seen = set()
    suggestions = []
    for s in from_catalog + from_history:
        if s.lower() not in seen:
            seen.add(s.lower())
            suggestions.append(s)

    return jsonify(suggestions[:8])

@app.route('/catalog')
def catalog_page():
    """Каталог всех манг с фильтрацией"""
    conn = get_db()
    c = conn.cursor()
    # Собрать все уникальные жанры с частотой
    c.execute('SELECT tags FROM manga WHERE tags IS NOT NULL AND tags != "[]" AND tags != ""')
    genre_freq = {}
    for row in c.fetchall():
        try:
            tags = json.loads(row[0])
            for tag in tags:
                tag = tag.strip()
                if tag:
                    genre_freq[tag] = genre_freq.get(tag, 0) + 1
        except Exception:
            pass
    genres = sorted(genre_freq.keys(), key=lambda g: -genre_freq[g])
    conn.close()
    return render_template('catalog.html', genres=genres)


@app.route('/api/catalog')
def api_catalog():
    """AJAX-каталог с фильтрацией и сортировкой"""
    _PER = 28
    _SORT = {
        'score':    'COALESCE(score, 0) DESC',
        'views':    'COALESCE(views, 0) DESC',
        'chapters': 'COALESCE(chapters_count, 0) DESC',
        'updated':  "COALESCE(last_updated, '1970') DESC",
        'title':    'manga_title ASC',
    }
    manga_type = request.args.get('type', '').strip().upper()
    manga_status = request.args.get('status', '').strip().upper()
    genres_raw = request.args.get('genres', '').strip()
    sort = request.args.get('sort', 'score')
    offset = max(0, request.args.get('offset', 0, type=int))
    limit = min(max(1, request.args.get('limit', _PER, type=int)), 100)

    if sort not in _SORT:
        sort = 'score'

    selected_genres = [g.strip() for g in genres_raw.split(',') if g.strip()] if genres_raw else []

    where = ['1=1']
    params = []

    if manga_type and manga_type in ('MANGA', 'MANHWA', 'MANHUA', 'OEL', 'NOVEL', 'ONE_SHOT', 'DOUJINSHI', 'COMICS'):
        where.append('manga_type = ?')
        params.append(manga_type)

    if manga_status and manga_status in ('ONGOING', 'FINISHED', 'CANCELLED', 'HIATUS', 'ANNOUNCED'):
        where.append('manga_status = ?')
        params.append(manga_status)

    for genre in selected_genres[:10]:
        safe = genre.replace('"', '').replace('%', '').replace('_', '\\_')
        where.append('tags LIKE ?')
        params.append(f'%"{safe}"%')

    where_sql = ' AND '.join(where)
    order_sql = _SORT[sort]

    conn = get_db()
    c = conn.cursor()
    c.execute(f'SELECT COUNT(*) FROM manga WHERE {where_sql}', params)
    total = c.fetchone()[0]

    c.execute(
        f'''SELECT manga_id, manga_slug, manga_title, manga_type, manga_status,
                   cover_url, rating, score, views, chapters_count, last_updated,
                   SUBSTR(description, 1, 160) AS description, tags
            FROM manga WHERE {where_sql}
            ORDER BY {order_sql}
            LIMIT ? OFFSET ?''',
        params + [limit, offset]
    )
    rows = [dict(r) for r in c.fetchall()]
    conn.close()

    return jsonify({'results': rows, 'total': total, 'has_more': offset + len(rows) < total})


@app.route('/api/subscribe/<manga_id>', methods=['POST'])
def subscribe(manga_id):
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Not authenticated'}), 401
    
    subscribed = toggle_subscription(user_id, manga_id)
    return jsonify({'subscribed': subscribed})

@app.route('/read/<manga_slug>/<chapter_slug>')
def read_chapter(manga_slug, chapter_slug):
    # Сначала попробуем найти в БД
    conn = get_db()
    c = conn.cursor()
    c.execute('''SELECT c.*, m.manga_title, m.manga_id, m.manga_slug 
                 FROM chapters c 
                 JOIN manga m ON c.manga_id = m.manga_id 
                 WHERE c.chapter_slug = ?''', (chapter_slug,))
    chapter = c.fetchone()
    
    if not chapter:
        # Если главы нет в БД, получаем через API
        pages = get_chapter_pages(chapter_slug)
        if not pages:
            conn.close()
            return "Глава не найдена", 404
        
        # Ищем manga_id по slug
        c.execute('SELECT manga_id, manga_title, manga_slug FROM manga WHERE manga_slug = ?', (manga_slug,))
        manga_result = c.fetchone()
        
        if not manga_result:
            conn.close()
            return "Манга не найдена", 404
        
        manga_id = manga_result['manga_id']
        manga_title = manga_result['manga_title']
        manga_slug_db = manga_result['manga_slug']  # Получаем manga_slug из БД
        
        # Получаем URL страниц
        page_urls = [p.get("image", {}).get("compress", {}).get("url", "") 
                     for p in pages if p.get("image", {}).get("compress", {}).get("url")]
        
        # Создаем временный объект главы
        chapter_dict = {
            'chapter_id': f"temp_{chapter_slug}",
            'chapter_slug': chapter_slug,
            'chapter_number': '1',
            'chapter_volume': None,
            'chapter_name': 'Глава из API',
            'manga_title': manga_title,
            'manga_id': manga_id,
            'manga_slug': manga_slug_db,  # Используем manga_slug из БД
            'pages_json': json.dumps(page_urls),
            'pages': page_urls,
            'chapter_url': f"{SITE_URL}/read/{manga_slug_db}/{chapter_slug}"
        }
        
        # Показываем главу без сохранения в БД
        subscribed = False
        user_id = session.get('user_id')
        if user_id:
            subscribed = is_subscribed(user_id, manga_id)
        
        conn.close()
        return render_template('chapter.html',
                              chapter=chapter_dict,
                              subscribed=subscribed,
                              user_id=user_id,
                              prev_chapter=None,
                              next_chapter=None)
    
    # Преобразуем результат запроса в словарь
    chapter_dict = dict(chapter)
    
    # Убедимся, что manga_slug присутствует в словаре
    # (он должен быть в запросе из-за JOIN с таблицей manga)
    if 'manga_slug' not in chapter_dict:
        chapter_dict['manga_slug'] = manga_slug
    
    # Проверяем, что pages_json не None и содержит данные
    if chapter_dict.get('pages_json'):
        try:
            chapter_dict['pages'] = json.loads(chapter_dict['pages_json'])
        except (json.JSONDecodeError, TypeError) as e:
            print(f"❌ Ошибка загрузки JSON для главы {chapter_slug}: {e}")
            chapter_dict['pages'] = []
    else:
        chapter_dict['pages'] = []
    
    # Если страниц нет или они пустые, получаем через API
    if not chapter_dict['pages']:
        print(f"📄 Получение страниц через API для главы {chapter_slug}")
        pages = get_chapter_pages(chapter_slug)
        
        if pages:
            # Извлекаем URL страниц
            page_urls = [p.get("image", {}).get("compress", {}).get("url", "") 
                        for p in pages if p.get("image", {}).get("compress", {}).get("url")]
            
            chapter_dict['pages'] = page_urls
            
            # Обновляем в БД
            c.execute('UPDATE chapters SET pages_json = ?, pages_count = ? WHERE chapter_slug = ?',
                      (json.dumps(page_urls), len(page_urls), chapter_slug))
            conn.commit()
            print(f"✅ Обновлено {len(page_urls)} страниц для главы {chapter_slug}")
        else:
            print(f"⚠️ Не удалось получить страницы для главы {chapter_slug}")
    
    # Обновляем счетчик просмотров
    increment_manga_views(chapter_dict['manga_id'])
    
    # Обновляем историю чтения и начисляем XP
    user_id = session.get('user_id')
    if user_id:
        c.execute('''INSERT OR REPLACE INTO reading_history
                     (user_id, manga_id, chapter_id, last_read)
                     VALUES (?, ?, ?, ?)''',
                  (user_id, chapter_dict['manga_id'],
                   chapter_dict['chapter_id'], datetime.now()))
        # Сохраняем каждую прочитанную главу отдельно
        c.execute('''INSERT OR IGNORE INTO chapters_read
                     (user_id, chapter_id, manga_id)
                     VALUES (?, ?, ?)''',
                  (user_id, chapter_dict['chapter_id'], chapter_dict['manga_id']))

        # Увеличиваем счётчик прочитанных глав
        c.execute('INSERT OR IGNORE INTO user_stats (user_id) VALUES (?)', (user_id,))
        c.execute(
            'UPDATE user_stats SET total_chapters_read = total_chapters_read + 1,'
            ' total_pages_read = total_pages_read + ? WHERE user_id = ?',
            (len(chapter_dict.get('pages', [])), user_id)
        )
        conn.commit()

        # Начисляем XP: +10 за главу + 1 за каждую страницу
        pages_count = len(chapter_dict.get('pages', []))
        xp_amount = 10 + pages_count
        award_xp(user_id, xp_amount, 'chapter_read', ref_id=chapter_dict['chapter_id'])

        # Стрик чтения
        update_reading_streak(user_id, conn)
        # Дневные задания
        update_daily_quest_progress(user_id, 'chapters_today', conn)
        # Сезонные задания
        update_season_quest_progress(user_id, 'chapters_read', 1, conn)

    # Предыдущая и следующая главы
    manga_id_nav = chapter_dict['manga_id']
    chapter_num_nav = chapter_dict['chapter_number']

    c.execute('''SELECT chapter_slug, chapter_number FROM chapters
                 WHERE manga_id = ? AND CAST(chapter_number AS FLOAT) < CAST(? AS FLOAT)
                 ORDER BY CAST(chapter_number AS FLOAT) DESC LIMIT 1''',
              (manga_id_nav, chapter_num_nav))
    prev_ch = c.fetchone()

    c.execute('''SELECT chapter_slug, chapter_number FROM chapters
                 WHERE manga_id = ? AND CAST(chapter_number AS FLOAT) > CAST(? AS FLOAT)
                 ORDER BY CAST(chapter_number AS FLOAT) ASC LIMIT 1''',
              (manga_id_nav, chapter_num_nav))
    next_ch = c.fetchone()

    prev_chapter = dict(prev_ch) if prev_ch else None
    next_chapter = dict(next_ch) if next_ch else None

    conn.close()

    # Если пользователь дочитал последнюю доступную главу — предложить похожую мангу
    if user_id and next_chapter is None:
        import threading as _th
        _th.Thread(
            target=_suggest_similar_manga,
            args=(user_id, chapter_dict['manga_id'], chapter_dict.get('manga_title', manga_slug)),
            daemon=True
        ).start()

    subscribed = False
    if user_id:
        subscribed = is_subscribed(user_id, chapter_dict['manga_id'])

    return render_template('chapter.html',
                          chapter=chapter_dict,
                          subscribed=subscribed,
                          user_id=user_id,
                          prev_chapter=prev_chapter,
                          next_chapter=next_chapter)

# ==================== ФИЛЬТРЫ ДЛЯ ШАБЛОНОВ ====================

@app.template_filter('relative_time')
def relative_time_filter(timestamp):
    """Фильтр для преобразования времени в относительный формат"""
    if not timestamp:
        return "Недавно"
    
    try:
        # Пробуем разные форматы дат
        if isinstance(timestamp, str):
            # Убираем временную зону если есть
            timestamp = timestamp.split('+')[0].split('Z')[0]
            
            # Пробуем разные форматы
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S.%f']:
                try:
                    date = datetime.strptime(timestamp, fmt)
                    break
                except ValueError:
                    continue
            else:
                return timestamp[:10]  # Возвращаем только дату если не распарсилось
        else:
            date = timestamp
        
        now = datetime.now()
        diff = now - date
        
        # Вычисляем разницу в различных единицах
        seconds = diff.total_seconds()
        minutes = seconds / 60
        hours = minutes / 60
        days = hours / 24
        
        if seconds < 60:
            return "Только что"
        elif minutes < 60:
            mins = int(minutes)
            if mins == 1:
                return "1 минуту назад"
            elif 2 <= mins <= 4:
                return f"{mins} минуты назад"
            else:
                return f"{mins} минут назад"
        elif hours < 24:
            hrs = int(hours)
            if hrs == 1:
                return "1 час назад"
            elif 2 <= hrs <= 4:
                return f"{hrs} часа назад"
            else:
                return f"{hrs} часов назад"
        elif days < 7:
            ds = int(days)
            if ds == 1:
                return "Вчера"
            elif ds == 2:
                return "Позавчера"
            else:
                return f"{ds} дней назад"
        elif days < 30:
            weeks = int(days / 7)
            if weeks == 1:
                return "1 неделю назад"
            elif weeks == 2:
                return "2 недели назад"
            else:
                return f"{weeks} недель назад"
        elif days < 365:
            months = int(days / 30)
            if months == 1:
                return "1 месяц назад"
            elif 2 <= months <= 4:
                return f"{months} месяца назад"
            else:
                return f"{months} месяцев назад"
        else:
            years = int(days / 365)
            if years == 1:
                return "1 год назад"
            elif 2 <= years <= 4:
                return f"{years} года назад"
            else:
                return f"{years} лет назад"
                
    except Exception as e:
        logger.error(f"Ошибка в фильтре relative_time: {e}")
        return timestamp[:10] if isinstance(timestamp, str) and len(timestamp) >= 10 else "Недавно"

# Добавьте также другие полезные фильтры
@app.template_filter('format_date')
def format_date_filter(date_str, format='%d.%m.%Y'):
    """Форматирование даты"""
    if not date_str:
        return ""
    
    try:
        if isinstance(date_str, str):
            # Убираем временную зону если есть
            date_str = date_str.split('+')[0].split('Z')[0]
            
            # Пробуем разные форматы
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d']:
                try:
                    date = datetime.strptime(date_str, fmt)
                    return date.strftime(format)
                except ValueError:
                    continue
        
        return date_str[:10] if len(date_str) >= 10 else date_str
    except Exception as e:
        logger.error(f"Ошибка в фильтре format_date: {e}")
        return date_str
def _bg_load_all_chapters(manga_slug):
    """Фоновый поток: загрузить все главы и сохранить в БД"""
    try:
        logger.info(f"🔄 [BG] Фоновая загрузка всех глав для {manga_slug}")
        get_manga_chapters_api(manga_slug, limit=10000)
        logger.info(f"✅ [BG] Фоновая загрузка завершена для {manga_slug}")
    except Exception as e:
        logger.error(f"❌ [BG] Ошибка фоновой загрузки для {manga_slug}: {e}")
    finally:
        _manga_loading.pop(manga_slug, None)


@app.route('/api/manga/<manga_slug>/chapters')
def api_manga_chapters(manga_slug):
    """API: получить главы манги с пагинацией (для фоновой подгрузки)"""
    try:
        offset = int(request.args.get('offset', 0))
        limit = min(int(request.args.get('limit', 50)), 5000)
        order = request.args.get('order', 'desc').lower()
        if order not in ('asc', 'desc'):
            order = 'desc'
    except (ValueError, TypeError):
        offset, limit, order = 0, 50, 'desc'

    order_sql = 'ASC' if order == 'asc' else 'DESC'

    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT manga_id FROM manga WHERE manga_slug = ?', (manga_slug,))
    manga_row = c.fetchone()

    if not manga_row:
        conn.close()
        return jsonify({'chapters': [], 'is_loading': manga_slug in _manga_loading, 'total_in_db': 0, 'has_more': False})

    manga_id = manga_row['manga_id']
    c.execute(
        f'''SELECT chapter_id, chapter_slug, chapter_number, chapter_volume,
                   chapter_name, created_at, chapter_url
            FROM chapters
            WHERE manga_id = ?
            ORDER BY CAST(chapter_number AS FLOAT) {order_sql}
            LIMIT ? OFFSET ?''',
        (manga_id, limit, offset)
    )
    chapters = [dict(row) for row in c.fetchall()]

    c.execute('SELECT COUNT(*) as cnt FROM chapters WHERE manga_id = ?', (manga_id,))
    total_in_db = c.fetchone()['cnt']
    conn.close()

    return jsonify({
        'chapters': chapters,
        'is_loading': manga_slug in _manga_loading,
        'total_in_db': total_in_db,
        'has_more': len(chapters) == limit
    })


@app.route('/api/manga/<manga_slug>/read-chapters')
def api_read_chapters(manga_slug):
    """API: список прочитанных глав пользователя для конкретной манги"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify([])
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT manga_id FROM manga WHERE manga_slug = ?', (manga_slug,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify([])
    manga_id = row['manga_id']
    c.execute(
        'SELECT chapter_id FROM chapters_read WHERE user_id = ? AND manga_id = ?',
        (user_id, manga_id)
    )
    chapter_ids = [r['chapter_id'] for r in c.fetchall()]
    conn.close()
    return jsonify(chapter_ids)


def _refresh_manga_worker(slugs):
    """Фоновый поток: загрузить/обновить полные данные манг по slug-ам.

    Обновляем если:
      - манги нет в БД вообще, или
      - описание пустое (данные неполные), или
      - прошло больше 24 ч с последнего обновления.
    """
    threshold = datetime.now() - timedelta(hours=24)
    for slug in slugs:
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute(
                'SELECT manga_id, last_updated, description FROM manga WHERE manga_slug = ?',
                (slug,)
            )
            row = c.fetchone()
            conn.close()

            needs_refresh = True
            if row:
                # Пропускаем только если описание есть и данные свежие
                has_desc = bool((row['description'] or '').strip())
                if has_desc and row['last_updated']:
                    try:
                        last_upd = datetime.fromisoformat(row['last_updated'])
                        if last_upd > threshold:
                            needs_refresh = False
                    except Exception:
                        pass

            if not needs_refresh:
                continue

            fresh = api.fetch_manga(slug)
            if not fresh:
                continue

            # Сохраняем полные данные через общую функцию
            save_manga_details_to_db(fresh)
            logger.info(f"[bulk-refresh] обновлена манга {slug}")
        except Exception as e:
            logger.warning(f"[bulk-refresh] ошибка для {slug}: {e}")


@app.route('/api/manga/bulk-refresh', methods=['POST'])
def api_manga_bulk_refresh():
    """API: обновить метаданные манг в фоне (TTL 24 ч).
    Body: {"slugs": ["slug1", "slug2", ...]}  (до 20 штук за раз)
    """
    data = request.get_json(silent=True) or {}
    slugs = [s for s in (data.get('slugs') or []) if isinstance(s, str)][:20]
    if slugs:
        t = threading.Thread(target=_refresh_manga_worker, args=(slugs,), daemon=True)
        t.start()
    return jsonify({'queued': len(slugs)})


@app.route('/manga/<manga_slug>')
def manga_detail(manga_slug):
    """Детальная страница манги"""

    force_refresh = request.args.get('refresh') == 'true'

    # Сначала пытаемся получить из БД
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM manga WHERE manga_slug = ?', (manga_slug,))
    manga_db = c.fetchone()
    conn.close()

    # Проверяем свежесть данных
    need_api_update = force_refresh
    if manga_db and not force_refresh:
        # Если описание отсутствует — данные неполные, обновляем
        if not (manga_db['description'] or '').strip():
            need_api_update = True
            logger.info(f"Описание отсутствует для {manga_slug}, обновляем через API...")
        else:
            last_updated = manga_db['last_updated']
            if last_updated:
                try:
                    if datetime.now() - datetime.fromisoformat(last_updated) > timedelta(hours=1):
                        need_api_update = True
                        logger.info(f"Данные устарели для {manga_slug}, обновляем...")
                except Exception:
                    need_api_update = True
    elif not manga_db:
        need_api_update = True

    # Обновляем только метаданные манги через API (быстро, без глав)
    if need_api_update:
        logger.info(f"📄 Обновление метаданных через API для {manga_slug}")
        manga_details = get_manga_details_api(manga_slug)
        if not manga_details:
            if manga_db:
                logger.warning(f"⚠️ API не ответил, используем данные из БД")
                manga_data = dict(manga_db)
            else:
                return "Манга не найдена", 404
        else:
            manga_data = manga_details
    else:
        logger.info(f"📦 Используем закешированные данные для {manga_slug}")
        manga_data = dict(manga_db)

    # Десериализуем JSON-поля если они пришли из БД (строки)
    import json as _json
    for _field in ('tags', 'formats'):
        val = manga_data.get(_field)
        if isinstance(val, str):
            try:
                manga_data[_field] = _json.loads(val)
            except Exception:
                manga_data[_field] = []
        elif val is None:
            manga_data[_field] = []

    manga_id = manga_data.get('manga_id')

    # Берём только первые 50 глав для начального рендера (#15)
    chapters = []
    total_in_db = 0
    if manga_id:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            '''SELECT chapter_id, chapter_slug, chapter_number, chapter_volume,
                      chapter_name, created_at, chapter_url
               FROM chapters
               WHERE manga_id = ?
               ORDER BY CAST(chapter_number AS FLOAT) DESC
               LIMIT 50''',
            (manga_id,)
        )
        chapters = [dict(row) for row in c.fetchall()]
        c.execute('SELECT COUNT(*) as cnt FROM chapters WHERE manga_id = ?', (manga_id,))
        total_in_db = c.fetchone()['cnt']
        conn.close()

    # Запускаем фоновую загрузку глав если они неполные (#14)
    expected_chapters = manga_data.get('chapters_count', 0)
    is_loading_more = manga_slug in _manga_loading
    if manga_id and not is_loading_more and expected_chapters > 0 and total_in_db < expected_chapters:
        _manga_loading[manga_slug] = True
        threading.Thread(
            target=_bg_load_all_chapters,
            args=(manga_slug,),
            daemon=True
        ).start()
        is_loading_more = True
        logger.info(f"🔄 Фоновая загрузка глав: {manga_slug} ({total_in_db}/{expected_chapters})")

    # Проверяем подписку
    subscribed = False
    user_id = session.get('user_id')
    if user_id and manga_id:
        subscribed = is_subscribed(user_id, manga_id)
        # Дневное задание "открой 2 разные манги"
        try:
            _conn_dq = get_db()
            update_daily_quest_progress(user_id, 'manga_today', _conn_dq)
            _conn_dq.close()
        except Exception:
            pass

    # Проверяем историю чтения
    reading_history = None
    if user_id and manga_id:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            '''SELECT rh.*, c.chapter_slug, c.chapter_number
               FROM reading_history rh
               JOIN chapters c ON rh.chapter_id = c.chapter_id
               WHERE rh.user_id = ? AND rh.manga_id = ?
               ORDER BY rh.last_read DESC LIMIT 1''',
            (user_id, manga_id)
        )
        history = c.fetchone()
        conn.close()
        if history:
            reading_history = dict(history)

    # Вишлист
    in_wishlist = False
    if user_id and manga_id:
        conn2 = get_db()
        in_wishlist = bool(conn2.execute(
            'SELECT 1 FROM reading_wishlist WHERE user_id=? AND manga_id=?', (user_id, manga_id)
        ).fetchone())
        conn2.close()

    logger.info(
        f"📄 Рендер {manga_slug}: {len(chapters)} глав показано, "
        f"{total_in_db} в БД, {expected_chapters} ожидается"
    )

    return render_template('manga_detail.html',
                           manga=manga_data,
                           chapters=chapters,
                           subscribed=subscribed,
                           reading_history=reading_history,
                           is_loading_more=is_loading_more,
                           in_wishlist=in_wishlist,
                           user_id=user_id)

# ==================== ПРОФИЛИ / ТОП / МАГАЗИН ====================

@app.route('/profile/me')
def profile_me():
    """Редирект на свой профиль"""
    user_id = session.get('user_id')
    if not user_id:
        return redirect(url_for('index'))
    return redirect(url_for('profile_page', user_id=user_id))


@app.route('/profile/<int:user_id>')
def profile_page(user_id):
    """Публичная страница профиля"""
    profile_data = get_user_full_profile(user_id)
    if not profile_data:
        return "Пользователь не найден", 404

    viewer_id = session.get('user_id')
    is_own = (viewer_id == user_id)

    # Монеты зрителя для проверки в шаблоне
    viewer_coins = 0
    if viewer_id:
        stats = get_or_create_user_stats(viewer_id)
        viewer_coins = stats.get('coins', 0) if stats else 0

    return render_template('profile.html',
                           profile=profile_data,
                           is_own=is_own,
                           user_id=viewer_id,
                           viewer_coins=viewer_coins)


@app.route('/top')
def top_page():
    """Таблица лидеров"""
    conn = get_db()
    c = conn.cursor()
    ROW_SQL = '''SELECT u.id, u.telegram_first_name, u.telegram_username,
                  u.is_premium,
                  s.xp, s.level, s.total_chapters_read,
                  COALESCE(p.custom_avatar_url, p.avatar_url) as avatar_url,
                  NULLIF(TRIM(COALESCE(p.custom_name, '')), '') as custom_name,
                  (SELECT si.css_value FROM shop_items si
                   JOIN user_items ui ON si.id = ui.item_id
                   WHERE ui.user_id = u.id AND ui.is_equipped = 1 AND si.type = 'frame'
                   LIMIT 1) as frame_css
           FROM users u
           JOIN user_stats s ON u.id = s.user_id
           LEFT JOIN user_profile p ON u.id = p.user_id'''

    c.execute(ROW_SQL + ' ORDER BY s.xp DESC LIMIT 100')
    rows = c.fetchall()

    def make_display(r):
        return (r.get('custom_name') or '').strip() or \
               r.get('telegram_first_name') or \
               r.get('telegram_username') or \
               f"#{r['id']}"

    leaders = []
    top_ids = set()
    for row in rows:
        r = dict(row)
        r['display_name'] = make_display(r)
        leaders.append(r)
        top_ids.add(r['id'])

    user_id = session.get('user_id')
    my_rank_data = None
    if user_id and user_id not in top_ids:
        # Ранг пользователя среди всех
        c.execute('''SELECT COUNT(*) + 1 AS rank FROM user_stats
                     WHERE xp > (SELECT xp FROM user_stats WHERE user_id = ?)''',
                  (user_id,))
        rank_row = c.fetchone()
        if rank_row:
            c.execute(ROW_SQL + ' WHERE u.id = ?', (user_id,))
            ur = c.fetchone()
            if ur:
                my_data = dict(ur)
                my_data['display_name'] = make_display(my_data)
                my_data['rank'] = rank_row['rank']
                my_rank_data = my_data

    conn.close()
    return render_template('top.html', leaders=leaders, user_id=user_id, my_rank_data=my_rank_data)


@app.route('/shop')
def shop_page():
    """Страница магазина"""
    user_id = session.get('user_id')

    conn = get_db()
    c = conn.cursor()

    # Все товары
    c.execute('SELECT * FROM shop_items ORDER BY type, price')
    items = [dict(row) for row in c.fetchall()]

    # Купленные товары текущего пользователя
    owned_ids = set()
    loaned_ids = set()
    equipped = {}
    coins = 0
    is_premium = 0
    premium_expires_at = None
    temp_expires = {}
    if user_id:
        c.execute('SELECT item_id, is_equipped, is_premium_loan, expires_at FROM user_items WHERE user_id = ?', (user_id,))
        for row in c.fetchall():
            owned_ids.add(row['item_id'])
            if row['is_premium_loan']:
                loaned_ids.add(row['item_id'])
            if row['is_equipped']:
                equipped[row['item_id']] = True
            if row['expires_at']:
                temp_expires[row['item_id']] = row['expires_at']
        c.execute('SELECT coins FROM user_stats WHERE user_id = ?', (user_id,))
        r = c.fetchone()
        coins = r['coins'] if r else 0
        c.execute('SELECT is_premium, premium_expires_at FROM users WHERE id = ?', (user_id,))
        ur = c.fetchone()
        if ur:
            is_premium = ur['is_premium']
            premium_expires_at = ur['premium_expires_at']

    conn.close()

    return render_template('shop.html',
                           items=items,
                           owned_ids=list(owned_ids),
                           loaned_ids=list(loaned_ids),
                           equipped=equipped,
                           coins=coins,
                           user_id=user_id,
                           is_premium=is_premium,
                           premium_expires_at=premium_expires_at,
                           temp_expires=temp_expires)


@app.route('/api/shop/buy/<int:item_id>', methods=['POST'])
def shop_buy(item_id):
    """Купить товар из магазина"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    conn = get_db()
    c = conn.cursor()

    # Проверяем товар
    c.execute('SELECT * FROM shop_items WHERE id = ?', (item_id,))
    item = c.fetchone()
    if not item:
        conn.close()
        return jsonify({'error': 'Товар не найден'}), 404

    # Уже куплен?
    c.execute('SELECT id, is_premium_loan, expires_at FROM user_items WHERE user_id = ? AND item_id = ?', (user_id, item_id))
    existing = c.fetchone()
    now_iso = datetime.utcnow().isoformat()
    # Постоянный — блокируем
    if existing and not existing['is_premium_loan'] and not existing['expires_at']:
        conn.close()
        return jsonify({'error': 'Уже куплено навсегда'}), 400
    # Временный истёк — удалить старую запись
    if existing and existing['expires_at'] and existing['expires_at'] < now_iso:
        c.execute('DELETE FROM user_items WHERE user_id = ? AND item_id = ?', (user_id, item_id))
        existing = None

    # Проверяем монеты
    c.execute('INSERT OR IGNORE INTO user_stats (user_id) VALUES (?)', (user_id,))
    c.execute('SELECT coins FROM user_stats WHERE user_id = ?', (user_id,))
    row = c.fetchone()
    coins = row['coins'] if row else 0

    if coins < item['price']:
        conn.close()
        return jsonify({'error': 'Недостаточно монет'}), 400

    # Если был loan или temp — удалить (переход к постоянному)
    if existing:
        c.execute('DELETE FROM user_items WHERE user_id = ? AND item_id = ?', (user_id, item_id))

    # Списываем монеты и добавляем товар
    c.execute('UPDATE user_stats SET coins = coins - ? WHERE user_id = ?', (item['price'], user_id))
    if item['duration_days']:
        exp = (datetime.utcnow() + timedelta(days=item['duration_days'])).isoformat()
        c.execute('INSERT INTO user_items (user_id, item_id, expires_at) VALUES (?, ?, ?)', (user_id, item_id, exp))
    else:
        c.execute('INSERT INTO user_items (user_id, item_id) VALUES (?, ?)', (user_id, item_id))
    conn.commit()

    c.execute('SELECT coins FROM user_stats WHERE user_id = ?', (user_id,))
    new_coins = c.fetchone()['coins']
    conn.close()

    return jsonify({'success': True, 'coins': new_coins})


@app.route('/api/shop/activate/<int:item_id>', methods=['POST'])
def shop_activate(item_id):
    """Premium: бесплатно активировать любой товар (loan)"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    conn = get_db()
    c = conn.cursor()

    # Проверяем Premium
    c.execute('SELECT is_premium, premium_expires_at FROM users WHERE id = ?', (user_id,))
    u = c.fetchone()
    now_iso = datetime.utcnow().isoformat()
    if not u or not u['is_premium'] or (u['premium_expires_at'] and u['premium_expires_at'] < now_iso):
        conn.close()
        return jsonify({'error': 'Требуется Premium подписка'}), 403

    # Проверяем товар
    c.execute('SELECT * FROM shop_items WHERE id = ?', (item_id,))
    item = c.fetchone()
    if not item:
        conn.close()
        return jsonify({'error': 'Товар не найден'}), 404

    # Проверяем, есть ли уже у пользователя
    c.execute('SELECT id, is_premium_loan FROM user_items WHERE user_id = ? AND item_id = ?', (user_id, item_id))
    existing = c.fetchone()
    if existing and not existing['is_premium_loan']:
        conn.close()
        return jsonify({'error': 'Уже куплено навсегда, предмет активен'}), 400
    if existing and existing['is_premium_loan']:
        conn.close()
        return jsonify({'error': 'Уже активировано'}), 400

    c.execute('INSERT INTO user_items (user_id, item_id, is_premium_loan) VALUES (?, ?, 1)', (user_id, item_id))
    conn.commit()
    conn.close()

    return jsonify({'success': True, 'activated': True})


@app.route('/api/shop/packages')
def shop_packages():
    """Возвращает доступные пакеты монет за Stars"""
    return jsonify(COIN_PACKAGES)


@app.route('/api/shop/create-invoice', methods=['POST'])
def shop_create_invoice():
    """Создаёт Telegram Stars invoice для покупки монет"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    data = request.get_json(silent=True) or {}
    package_id = data.get('package_id')
    pkg = next((p for p in COIN_PACKAGES if p['id'] == package_id), None)
    if not pkg:
        return jsonify({'error': 'Пакет не найден'}), 404

    if not _bot_loop or not _bot_loop.is_running() or not telegram_app:
        return jsonify({'error': 'Бот недоступен'}), 503

    payload = f"{pkg['id']}:{user_id}"

    async def _create_link():
        return await telegram_app.bot.create_invoice_link(
            title=pkg['label'],
            description=f"{pkg['coins']} монет для BubbleManga",
            payload=payload,
            currency="XTR",
            provider_token="",
            prices=[LabeledPrice(label=pkg['label'], amount=pkg['stars'])],
        )

    future = asyncio.run_coroutine_threadsafe(_create_link(), _bot_loop)
    try:
        url = future.result(timeout=10)
    except Exception as e:
        return jsonify({'error': f'Ошибка создания счёта: {e}'}), 500

    return jsonify({'url': url})


@app.route('/api/user/balance')
def user_balance():
    """Возвращает текущий баланс монет пользователя"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT coins FROM user_stats WHERE user_id = ?', (user_id,))
    row = c.fetchone()
    conn.close()
    coins = row['coins'] if row else 0
    return jsonify({'coins': coins})


def _credit_coins(user_id, package_id, payment_id, payment_method='stars'):
    """Общая функция начисления монет после любой оплаты."""
    pkg = next((p for p in COIN_PACKAGES if p['id'] == package_id), None)
    if not pkg:
        return False
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute(
            '''INSERT OR IGNORE INTO coin_purchases
               (user_id, package_id, stars_paid, coins_received, payment_id, payment_method)
               VALUES (?, ?, 0, ?, ?, ?)''',
            (user_id, package_id, pkg['coins'], payment_id, payment_method)
        )
        credited = c.rowcount > 0
        if credited:
            c.execute('UPDATE user_stats SET coins = coins + ? WHERE user_id = ?',
                      (pkg['coins'], user_id))
        conn.commit()
        return credited
    finally:
        conn.close()


def _grant_premium(user_id, package_id, payment_id, payment_method='yookassa'):
    """Активирует/продлевает Premium после успешной оплаты."""
    pkg = next((p for p in PREMIUM_PACKAGES if p['id'] == package_id), None)
    if not pkg:
        return False
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('SELECT id FROM premium_purchases WHERE payment_id = ?', (payment_id,))
        if c.fetchone():
            return False  # уже обработано
        now = datetime.utcnow()
        c.execute('SELECT is_premium, premium_expires_at FROM users WHERE id = ?', (user_id,))
        row = c.fetchone()
        # Продлить если подписка ещё активна, иначе начать с сейчас
        if row and row['is_premium'] and row['premium_expires_at']:
            try:
                current_exp = datetime.fromisoformat(row['premium_expires_at'])
                base = current_exp if current_exp > now else now
            except Exception:
                base = now
        else:
            base = now
        new_exp = base + timedelta(days=pkg['days'])
        c.execute(
            'UPDATE users SET is_premium=1, premium_expires_at=? WHERE id=?',
            (new_exp.isoformat(), user_id)
        )
        c.execute(
            'INSERT OR IGNORE INTO premium_purchases (user_id, package_id, payment_id, payment_method, expires_at) VALUES (?, ?, ?, ?, ?)',
            (user_id, package_id, payment_id, payment_method, new_exp.isoformat())
        )
        conn.commit()
        return True
    finally:
        conn.close()


@app.route('/api/shop/create-payment', methods=['POST'])
def shop_create_payment():
    """Создаёт платёж через ЮКасса или CryptoBot."""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    data = request.get_json(silent=True) or {}
    package_id = data.get('package_id')
    method = data.get('method')

    pkg = next((p for p in COIN_PACKAGES if p['id'] == package_id), None)
    if not pkg:
        return jsonify({'error': 'Пакет не найден'}), 404

    if method == 'yookassa':
        if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET:
            return jsonify({'error': 'ЮКасса не настроена на сервере'}), 503
        try:
            from yookassa import Configuration, Payment as YKPayment
            import uuid as _uuid
            Configuration.account_id = YOOKASSA_SHOP_ID
            Configuration.secret_key = YOOKASSA_SECRET
            payment = YKPayment.create({
                'amount': {'value': str(pkg['rub']) + '.00', 'currency': 'RUB'},
                'confirmation': {'type': 'redirect',
                                 'return_url': f'{SITE_URL}/shop?tab=buy&paid=1'},
                'capture': True,
                'description': f"{pkg['label']} — BubbleManga",
                'metadata': {'package_id': pkg['id'], 'user_id': str(user_id)},
            }, str(_uuid.uuid4()))
            return jsonify({'url': payment.confirmation.confirmation_url})
        except ImportError:
            return jsonify({'error': 'Библиотека yookassa не установлена'}), 503
        except Exception as e:
            return jsonify({'error': f'Ошибка ЮКасса: {e}'}), 500

    elif method == 'crypto':
        if not CRYPTOCLOUD_API_KEY or not CRYPTOCLOUD_SHOP_ID:
            return jsonify({'error': 'Crypto Cloud не настроен на сервере'}), 503
        try:
            import requests as _req
            resp = _req.post(
                'https://api.cryptocloud.plus/v2/invoice/create',
                headers={
                    'Authorization': f'Token {CRYPTOCLOUD_API_KEY}',
                    'Content-Type': 'application/json',
                },
                json={
                    'shop_id': CRYPTOCLOUD_SHOP_ID,
                    'amount': float(pkg['usd']),
                    'currency': 'USD',
                    'order_id': f"{pkg['id']}:{user_id}",
                },
                timeout=10
            )
            result = resp.json()
            if resp.status_code != 200 or result.get('status') == 'error':
                return jsonify({'error': 'Ошибка Crypto Cloud: ' + str(result)}), 500
            return jsonify({'url': result['result']['link']})
        except ImportError:
            return jsonify({'error': 'Библиотека requests не установлена'}), 503
        except Exception as e:
            return jsonify({'error': f'Ошибка Crypto Cloud: {e}'}), 500

    return jsonify({'error': 'Неизвестный способ оплаты'}), 400


@app.route('/api/shop/create-premium-payment', methods=['POST'])
def shop_create_premium_payment():
    """Создаёт платёж за Premium подписку через ЮКасса или Crypto Cloud."""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    data = request.get_json(silent=True) or {}
    package_id = data.get('package_id')
    method = data.get('method')

    pkg = next((p for p in PREMIUM_PACKAGES if p['id'] == package_id), None)
    if not pkg:
        return jsonify({'error': 'Пакет не найден'}), 404

    if method == 'yookassa':
        if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET:
            return jsonify({'error': 'ЮКасса не настроена на сервере'}), 503
        try:
            from yookassa import Configuration, Payment as YKPayment
            import uuid as _uuid
            Configuration.account_id = YOOKASSA_SHOP_ID
            Configuration.secret_key = YOOKASSA_SECRET
            payment = YKPayment.create({
                'amount': {'value': str(pkg['rub']) + '.00', 'currency': 'RUB'},
                'confirmation': {'type': 'redirect',
                                 'return_url': f'{SITE_URL}/shop?tab=premium&paid=1'},
                'capture': True,
                'description': f"{pkg['label']} — BubbleManga",
                'metadata': {'premium_package_id': pkg['id'], 'user_id': str(user_id)},
            }, str(_uuid.uuid4()))
            return jsonify({'url': payment.confirmation.confirmation_url})
        except ImportError:
            return jsonify({'error': 'Библиотека yookassa не установлена'}), 503
        except Exception as e:
            return jsonify({'error': f'Ошибка ЮКасса: {e}'}), 500

    elif method == 'crypto':
        if not CRYPTOCLOUD_API_KEY or not CRYPTOCLOUD_SHOP_ID:
            return jsonify({'error': 'Crypto Cloud не настроен на сервере'}), 503
        try:
            import requests as _req
            resp = _req.post(
                'https://api.cryptocloud.plus/v2/invoice/create',
                headers={
                    'Authorization': f'Token {CRYPTOCLOUD_API_KEY}',
                    'Content-Type': 'application/json',
                },
                json={
                    'shop_id': CRYPTOCLOUD_SHOP_ID,
                    'amount': float(pkg['usd']),
                    'currency': 'USD',
                    'order_id': f"premium:{pkg['id']}:{user_id}",
                },
                timeout=10
            )
            result = resp.json()
            if resp.status_code != 200 or result.get('status') == 'error':
                return jsonify({'error': 'Ошибка Crypto Cloud: ' + str(result)}), 500
            return jsonify({'url': result['result']['link']})
        except ImportError:
            return jsonify({'error': 'Библиотека requests не установлена'}), 503
        except Exception as e:
            return jsonify({'error': f'Ошибка Crypto Cloud: {e}'}), 500

    return jsonify({'error': 'Неизвестный способ оплаты'}), 400


@app.route('/api/shop/gift-premium', methods=['POST'])
def shop_gift_premium():
    """Создаёт Stars invoice для подарочного Premium. {recipient_id, days} → {url}"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    data = request.get_json(silent=True) or {}
    recipient_id = data.get('recipient_id')
    days = data.get('days')

    if not recipient_id or not days:
        return jsonify({'error': 'recipient_id и days обязательны'}), 400
    try:
        recipient_id = int(recipient_id)
        days = int(days)
    except (ValueError, TypeError):
        return jsonify({'error': 'Неверные параметры'}), 400
    if days not in (30, 90, 365):
        return jsonify({'error': 'days должен быть 30, 90 или 365'}), 400
    if recipient_id == user_id:
        return jsonify({'error': 'Нельзя подарить самому себе'}), 400

    # Проверяем получателя
    conn = get_db()
    c = conn.cursor()
    rec = c.execute('SELECT id, telegram_first_name, telegram_username FROM users WHERE id=?', (recipient_id,)).fetchone()
    conn.close()
    if not rec:
        return jsonify({'error': 'Получатель не найден'}), 404

    label_map = {30: '1 месяц', 90: '3 месяца', 365: '1 год'}
    stars_map = {30: 50, 90: 130, 365: 450}
    label = label_map[days]
    stars = stars_map[days]
    recipient_name = rec['telegram_first_name'] or rec['telegram_username'] or f'ID {recipient_id}'
    payload = f'gift_premium:{recipient_id}:{days}:{user_id}'

    try:
        bot = telegram_app.bot if telegram_app else None
        if not bot:
            return jsonify({'error': 'Бот недоступен'}), 503
        if _bot_loop and _bot_loop.is_running():
            future = asyncio.run_coroutine_threadsafe(
                bot.create_invoice_link(
                    title=f'Premium на {label} для {recipient_name}',
                    description=f'Подарок Premium BubbleManga на {label}',
                    payload=payload,
                    currency='XTR',
                    provider_token='',
                    prices=[LabeledPrice(label=f'Premium {label}', amount=stars)],
                ),
                _bot_loop
            )
            url = future.result(timeout=10)
            return jsonify({'url': url})
        else:
            return jsonify({'error': 'Бот не запущен'}), 503
    except Exception as e:
        return jsonify({'error': f'Ошибка создания инвойса: {e}'}), 500


@app.route('/webhook/yookassa', methods=['POST'])
def webhook_yookassa():
    """Вебхук от ЮКасса — зачисляет монеты или активирует Premium после успешной оплаты."""
    data = request.get_json(silent=True) or {}
    if data.get('event') != 'payment.succeeded':
        return '', 200
    obj = data.get('object', {})
    meta = obj.get('metadata', {})
    package_id = meta.get('package_id')
    premium_package_id = meta.get('premium_package_id')
    user_id_str = meta.get('user_id')
    payment_id = obj.get('id')
    if user_id_str and payment_id:
        if premium_package_id:
            _grant_premium(int(user_id_str), premium_package_id, f'yk_{payment_id}', 'yookassa')
        elif package_id:
            _credit_coins(int(user_id_str), package_id, f'yk_{payment_id}', 'yookassa')
    return '', 200


@app.route('/webhook/cryptocloud', methods=['POST'])
def webhook_cryptocloud():
    """Вебхук от Crypto Cloud — зачисляет монеты после оплаты."""
    data = request.form.to_dict() if request.content_type and 'form' in request.content_type \
        else (request.get_json(silent=True) or {})

    if data.get('status') != 'success':
        return '', 200

    # Верификация JWT-токена (HS256, подписан SECRET KEY проекта)
    token = data.get('token', '')
    if CRYPTOCLOUD_SECRET_KEY and token:
        try:
            import jwt as _jwt
            _jwt.decode(token, CRYPTOCLOUD_SECRET_KEY, algorithms=['HS256'])
        except Exception:
            return '', 403

    order_id = data.get('order_id', '')
    invoice_id = str(data.get('invoice_id', ''))

    try:
        if order_id.startswith('premium:'):
            _, package_id, user_id_str = order_id.split(':', 2)
            _grant_premium(int(user_id_str), package_id, f'cc_{invoice_id}', 'crypto')
        else:
            package_id, user_id_str = order_id.rsplit(':', 1)
            _credit_coins(int(user_id_str), package_id, f'cc_{invoice_id}', 'crypto')
    except (ValueError, AttributeError):
        pass

    return '', 200


# ==================== WISHLIST ====================

@app.route('/api/wishlist/<manga_id>', methods=['POST'])
def toggle_wishlist(manga_id):
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    existing = conn.execute(
        'SELECT id FROM reading_wishlist WHERE user_id=? AND manga_id=?', (user_id, manga_id)
    ).fetchone()
    if existing:
        conn.execute('DELETE FROM reading_wishlist WHERE user_id=? AND manga_id=?', (user_id, manga_id))
        in_wishlist = False
    else:
        conn.execute('INSERT OR IGNORE INTO reading_wishlist (user_id, manga_id) VALUES (?,?)', (user_id, manga_id))
        in_wishlist = True
    conn.commit()
    conn.close()
    return jsonify({'in_wishlist': in_wishlist})


@app.route('/api/user/wishlist')
def api_user_wishlist():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    rows = conn.execute(
        '''SELECT m.manga_id, m.manga_slug, m.manga_title, m.cover_url, m.manga_type,
                  m.manga_status, rw.added_at
           FROM reading_wishlist rw
           JOIN manga m ON rw.manga_id = m.manga_id
           WHERE rw.user_id=?
           ORDER BY rw.added_at DESC''',
        (user_id,)
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


# ==================== COMMENT LIKES ====================

@app.route('/api/comments/<int:comment_id>/like', methods=['POST'])
def toggle_comment_like(comment_id):
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    existing = conn.execute(
        'SELECT id FROM comment_likes WHERE user_id=? AND comment_id=?', (user_id, comment_id)
    ).fetchone()
    if existing:
        conn.execute('DELETE FROM comment_likes WHERE user_id=? AND comment_id=?', (user_id, comment_id))
        liked = False
    else:
        conn.execute('INSERT OR IGNORE INTO comment_likes (user_id, comment_id) VALUES (?,?)', (user_id, comment_id))
        liked = True
    conn.commit()
    likes_count = conn.execute(
        'SELECT COUNT(*) FROM comment_likes WHERE comment_id=?', (comment_id,)
    ).fetchone()[0]
    conn.close()
    return jsonify({'liked': liked, 'likes_count': likes_count})


# ==================== REFERRAL ====================

@app.route('/api/profile/referral')
def api_referral():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    user = conn.execute('SELECT referral_code FROM users WHERE id=?', (user_id,)).fetchone()
    code = user['referral_code'] if user else None
    if not code:
        code = secrets.token_urlsafe(6).upper()
        conn.execute('UPDATE users SET referral_code=? WHERE id=?', (code, user_id))
        conn.commit()
    count = conn.execute('SELECT COUNT(*) FROM referrals WHERE referrer_id=?', (user_id,)).fetchone()[0]
    rewarded = conn.execute(
        'SELECT SUM(100) as total FROM referrals WHERE referrer_id=? AND rewarded=1', (user_id,)
    ).fetchone()
    coins_earned = rewarded[0] or 0
    conn.close()
    referral_url = f"https://t.me/bubblemanga_bot?start={code}"
    return jsonify({'code': code, 'referral_url': referral_url, 'count': count, 'coins_earned': coins_earned})


# ==================== DAILY QUESTS / SEASON API ====================

@app.route('/api/daily-quests')
def api_daily_quests():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    today = _date.today().isoformat()
    conn = get_db()
    get_or_create_daily_quests(user_id, conn)
    rows = conn.execute(
        '''SELECT dq.id, dq.title, dq.description, dq.icon, dq.condition_value,
                  dq.xp_reward, dq.coins_reward,
                  udq.progress, udq.completed_at
           FROM daily_quests dq
           LEFT JOIN user_daily_quests udq ON dq.id = udq.quest_id
               AND udq.user_id=? AND udq.date=?
           WHERE dq.is_active=1''',
        (user_id, today)
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        d['progress'] = d['progress'] or 0
        d['completed'] = bool(d['completed_at'])
        result.append(d)
    return jsonify(result)


@app.route('/api/season')
def api_season():
    user_id = session.get('user_id')
    conn = get_db()
    season = get_active_season(conn)
    if not season:
        conn.close()
        return jsonify(None)
    season_dict = dict(season)
    quests = conn.execute('SELECT * FROM season_quests WHERE season_id=?', (season['id'],)).fetchall()
    result_quests = []
    for q in quests:
        qd = dict(q)
        if user_id:
            row = conn.execute(
                'SELECT progress, completed_at FROM user_season_quests WHERE user_id=? AND season_quest_id=?',
                (user_id, q['id'])
            ).fetchone()
            qd['progress'] = row['progress'] if row else 0
            qd['completed'] = bool(row['completed_at']) if row else False
        else:
            qd['progress'] = 0
            qd['completed'] = False
        result_quests.append(qd)
    conn.close()
    season_dict['quests'] = result_quests
    return jsonify(season_dict)


# ==================== КОММЕНТАРИИ ====================

_COMMENT_QUERY = '''
    SELECT cm.id, cm.parent_id, cm.text, cm.created_at,
           u.id as user_id, u.telegram_first_name, u.telegram_username, u.is_premium,
           p.custom_name, p.avatar_url, p.custom_avatar_url, s.level,
           (SELECT si.css_value FROM shop_items si
            JOIN user_items ui ON si.id = ui.item_id
            WHERE ui.user_id = u.id AND ui.is_equipped = 1 AND si.type = 'frame'
            LIMIT 1) as frame_css,
           (SELECT COUNT(*) FROM comment_likes cl WHERE cl.comment_id = cm.id) as likes_count
    FROM comments cm
    JOIN users u ON cm.user_id = u.id
    LEFT JOIN user_profile p ON u.id = p.user_id
    LEFT JOIN user_stats s ON u.id = s.user_id
'''


@app.route('/api/manga/<manga_slug>/comments')
def get_comments(manga_slug):
    offset = max(0, int(request.args.get('offset', 0)))
    limit  = min(50, max(1, int(request.args.get('limit', 20))))
    sort   = request.args.get('sort', 'new')  # 'new' | 'top'
    viewer_id = session.get('user_id')
    conn = get_db()
    c = conn.cursor()

    order_clause = 'ORDER BY likes_count DESC, cm.created_at DESC' if sort == 'top' else 'ORDER BY cm.created_at DESC'

    # Верхнеуровневые комментарии (без ответов)
    c.execute(
        _COMMENT_QUERY + f'WHERE cm.manga_slug = ? AND cm.parent_id IS NULL {order_clause} LIMIT ? OFFSET ?',
        (manga_slug, limit, offset)
    )
    top_comments = [dict(r) for r in c.fetchall()]

    # Количество верхнеуровневых (для пагинации)
    c.execute('SELECT COUNT(*) FROM comments WHERE manga_slug = ? AND parent_id IS NULL', (manga_slug,))
    top_total = c.fetchone()[0]

    # Общее количество (включая ответы — для счётчика)
    c.execute('SELECT COUNT(*) FROM comments WHERE manga_slug = ?', (manga_slug,))
    total_all = c.fetchone()[0]

    # liked_by_me для верхнеуровневых
    if viewer_id and top_comments:
        liked_set = {
            r[0] for r in c.execute(
                f'SELECT comment_id FROM comment_likes WHERE user_id=? AND comment_id IN ({",".join("?"*len(top_comments))})',
                (viewer_id, *[cmt['id'] for cmt in top_comments])
            ).fetchall()
        }
        for cmt in top_comments:
            cmt['liked_by_me'] = cmt['id'] in liked_set
    else:
        for cmt in top_comments:
            cmt['liked_by_me'] = False

    # Загрузить ответы для этих комментариев одним запросом
    if top_comments:
        parent_ids = [cmt['id'] for cmt in top_comments]
        placeholders = ','.join('?' * len(parent_ids))
        c.execute(
            _COMMENT_QUERY + f'WHERE cm.parent_id IN ({placeholders}) ORDER BY cm.created_at ASC',
            parent_ids
        )
        replies = [dict(r) for r in c.fetchall()]
        if viewer_id and replies:
            liked_replies = {
                r[0] for r in c.execute(
                    f'SELECT comment_id FROM comment_likes WHERE user_id=? AND comment_id IN ({",".join("?"*len(replies))})',
                    (viewer_id, *[r['id'] for r in replies])
                ).fetchall()
            }
            for r in replies:
                r['liked_by_me'] = r['id'] in liked_replies
        else:
            for r in replies:
                r['liked_by_me'] = False
        reply_map = {}
        for r in replies:
            reply_map.setdefault(r['parent_id'], []).append(r)
        for cmt in top_comments:
            cmt['replies'] = reply_map.get(cmt['id'], [])

    conn.close()
    return jsonify({
        'comments': top_comments,
        'total': total_all,
        'has_more': offset + limit < top_total
    })


@app.route('/api/manga/<manga_slug>/comments', methods=['POST'])
def post_comment(manga_slug):
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    body = request.json or {}
    text = body.get('text', '').strip()
    parent_id = body.get('parent_id')
    if not text:
        return jsonify({'error': 'Пустой комментарий'}), 400
    if len(text) > 1000:
        return jsonify({'error': 'Максимум 1000 символов'}), 400

    conn = get_db()
    c = conn.cursor()

    # Проверить parent_id и «выровнять» до верхнего уровня
    if parent_id:
        c.execute('SELECT id, parent_id, manga_slug FROM comments WHERE id = ?', (parent_id,))
        parent_row = c.fetchone()
        if not parent_row or parent_row['manga_slug'] != manga_slug:
            conn.close()
            return jsonify({'error': 'Родительский комментарий не найден'}), 404
        # Ответ на ответ → прикрепить к верхнему родителю
        if parent_row['parent_id'] is not None:
            parent_id = parent_row['parent_id']

    c.execute('INSERT INTO comments (manga_slug, user_id, text, parent_id) VALUES (?, ?, ?, ?)',
              (manga_slug, user_id, text, parent_id))
    comment_id = c.lastrowid
    conn.commit()
    check_quests(user_id, conn)
    update_daily_quest_progress(user_id, 'comments_today', conn)
    update_season_quest_progress(user_id, 'comments_posted', 1, conn)
    c.execute(_COMMENT_QUERY + 'WHERE cm.id = ?', (comment_id,))
    comment = dict(c.fetchone())
    conn.close()
    return jsonify({'success': True, 'comment': comment})


@app.route('/api/comments/<int:comment_id>', methods=['DELETE'])
def delete_comment(comment_id):
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT user_id, parent_id FROM comments WHERE id = ?', (comment_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'Не найдено'}), 404
    c.execute('SELECT telegram_id FROM users WHERE id = ?', (user_id,))
    u = c.fetchone()
    is_admin = u and u['telegram_id'] in ADMIN_TELEGRAM_IDS
    if row['user_id'] != user_id and not is_admin:
        conn.close()
        return jsonify({'error': 'Нет доступа'}), 403
    # Удалить сам комментарий и все ответы на него
    c.execute('DELETE FROM comments WHERE id = ? OR parent_id = ?', (comment_id, comment_id))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/profile/equip/<int:item_id>', methods=['POST'])
def profile_equip(item_id):
    """Надеть / снять украшение"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    conn = get_db()
    c = conn.cursor()

    c.execute('SELECT ui.*, si.type FROM user_items ui JOIN shop_items si ON ui.item_id = si.id'
              ' WHERE ui.user_id = ? AND ui.item_id = ?', (user_id, item_id))
    ui = c.fetchone()
    if not ui:
        conn.close()
        return jsonify({'error': 'Товар не куплен'}), 403

    item_type = ui['type']
    now_equipped = ui['is_equipped']

    c.execute('INSERT OR IGNORE INTO user_profile (user_id) VALUES (?)', (user_id,))

    if now_equipped:
        # ── Снять ──────────────────────────────────────────────────────────
        c.execute('UPDATE user_items SET is_equipped = 0 WHERE user_id = ? AND item_id = ?',
                  (user_id, item_id))
        col_map = {'frame': 'frame_item_id', 'badge': 'badge_item_id', 'title': 'title_item_id'}
        if item_type in col_map:
            c.execute(f'UPDATE user_profile SET {col_map[item_type]} = NULL WHERE user_id = ?',
                      (user_id,))
        elif item_type == 'avatar':
            # Восстановить кастомный аватар если был
            c.execute('SELECT custom_avatar_url FROM user_profile WHERE user_id = ?', (user_id,))
            row = c.fetchone()
            custom = row['custom_avatar_url'] if row else None
            c.execute('UPDATE user_profile SET avatar_url = ? WHERE user_id = ?', (custom, user_id))
        elif item_type == 'background':
            # Восстановить кастомный фон если был
            c.execute('SELECT custom_avatar_url FROM user_profile WHERE user_id = ?', (user_id,))
            # background_url просто обнуляем (кастомный фон тоже пропадёт — приемлемо)
            c.execute('UPDATE user_profile SET background_url = NULL WHERE user_id = ?', (user_id,))
    else:
        # ── Надеть ─────────────────────────────────────────────────────────
        # Снимаем другие того же типа
        c.execute(
            '''UPDATE user_items SET is_equipped = 0
               WHERE user_id = ? AND item_id IN (
                   SELECT ui2.item_id FROM user_items ui2
                   JOIN shop_items si2 ON ui2.item_id = si2.id
                   WHERE ui2.user_id = ? AND si2.type = ?
               )''',
            (user_id, user_id, item_type)
        )
        c.execute('UPDATE user_items SET is_equipped = 1 WHERE user_id = ? AND item_id = ?',
                  (user_id, item_id))
        col_map = {'frame': 'frame_item_id', 'badge': 'badge_item_id', 'title': 'title_item_id'}
        if item_type in col_map:
            c.execute(f'UPDATE user_profile SET {col_map[item_type]} = ? WHERE user_id = ?',
                      (item_id, user_id))
        elif item_type == 'avatar':
            # Получаем preview_url товара и ставим как аватар
            c.execute('SELECT preview_url FROM shop_items WHERE id = ?', (item_id,))
            si = c.fetchone()
            avatar_url = si['preview_url'] if si else None
            c.execute('UPDATE user_profile SET avatar_url = ? WHERE user_id = ?',
                      (avatar_url, user_id))
        elif item_type == 'background':
            # Получаем preview_url товара и ставим как фон
            c.execute('SELECT preview_url, css_value FROM shop_items WHERE id = ?', (item_id,))
            si = c.fetchone()
            bg_url = (si['preview_url'] if si else None)
            c.execute('UPDATE user_profile SET background_url = ? WHERE user_id = ?',
                      (bg_url, user_id))

    conn.commit()
    conn.close()
    return jsonify({'success': True, 'equipped': not now_equipped})


@app.route('/api/profile/update', methods=['POST'])
def profile_update():
    """Обновить bio и/или имя профиля"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    data = request.json or {}
    bio = data.get('bio', '')[:300]
    custom_name = data.get('custom_name', '').strip()[:50]
    conn = get_db()
    c = conn.cursor()
    c.execute('INSERT OR IGNORE INTO user_profile (user_id) VALUES (?)', (user_id,))

    # Проверяем ограничение смены ника (2 раза в месяц)
    current_month = datetime.utcnow().strftime('%Y-%m')
    c.execute('SELECT custom_name, name_change_count, name_change_month FROM user_profile WHERE user_id = ?', (user_id,))
    row = c.fetchone()
    old_name = (row['custom_name'] or '') if row else ''
    name_changing = custom_name != old_name

    if name_changing and custom_name:  # смена ника только если новое значение непустое
        count = row['name_change_count'] or 0 if row else 0
        month = row['name_change_month'] if row else None
        if month != current_month:
            count = 0  # новый месяц — сбрасываем счётчик
        if count >= 2:
            conn.close()
            return jsonify({'error': 'Ник можно менять не более 2 раз в месяц', 'name_limit': True}), 429
        c.execute(
            'UPDATE user_profile SET bio = ?, custom_name = ?, name_change_count = ?, name_change_month = ? WHERE user_id = ?',
            (bio, custom_name, count + 1, current_month, user_id)
        )
    else:
        c.execute('UPDATE user_profile SET bio = ? WHERE user_id = ?', (bio, user_id))

    conn.commit()
    conn.close()
    display_name = custom_name or None
    return jsonify({'success': True, 'display_name': display_name})


@app.route('/upload/avatar', methods=['POST'])
def upload_avatar():
    """Загрузить аватар (только Premium)"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    conn = get_db()
    c = conn.cursor()

    c.execute('SELECT is_premium FROM users WHERE id = ?', (user_id,))
    u = c.fetchone()
    if not u or not u['is_premium']:
        conn.close()
        return jsonify({'error': 'Требуется Premium подписка', 'premium_required': True}), 403

    if 'file' not in request.files:
        conn.close()
        return jsonify({'error': 'Файл не выбран'}), 400

    f = request.files['file']
    if not f.filename or not _allowed_file(f.filename):
        conn.close()
        return jsonify({'error': 'Недопустимый формат файла'}), 400

    user_dir = os.path.join(UPLOAD_FOLDER, str(user_id))
    os.makedirs(user_dir, exist_ok=True)
    ext = f.filename.rsplit('.', 1)[1].lower()
    filename = f'avatar.{ext}'
    f.save(os.path.join(user_dir, filename))

    avatar_url = f'/static/uploads/{user_id}/{filename}'
    c.execute('INSERT OR IGNORE INTO user_profile (user_id) VALUES (?)', (user_id,))
    # Сохраняем как кастомный аватар и как текущий (снимаем shop-аватар)
    c.execute(
        'UPDATE user_profile SET avatar_url = ?, custom_avatar_url = ? WHERE user_id = ?',
        (avatar_url, avatar_url, user_id)
    )
    # Снять все shop-аватары
    c.execute(
        '''UPDATE user_items SET is_equipped = 0
           WHERE user_id = ? AND item_id IN (
               SELECT id FROM shop_items WHERE type = 'avatar'
           )''',
        (user_id,)
    )
    conn.commit()
    conn.close()

    return jsonify({'success': True, 'avatar_url': avatar_url})


@app.route('/upload/background', methods=['POST'])
def upload_background():
    """Загрузить фон профиля (только Premium)"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    conn = get_db()
    c = conn.cursor()

    c.execute('SELECT is_premium FROM users WHERE id = ?', (user_id,))
    u = c.fetchone()
    if not u or not u['is_premium']:
        conn.close()
        return jsonify({'error': 'Требуется Premium подписка', 'premium_required': True}), 403

    if 'file' not in request.files:
        conn.close()
        return jsonify({'error': 'Файл не выбран'}), 400

    f = request.files['file']
    if not f.filename or not _allowed_file(f.filename):
        conn.close()
        return jsonify({'error': 'Недопустимый формат файла'}), 400

    user_dir = os.path.join(UPLOAD_FOLDER, str(user_id))
    os.makedirs(user_dir, exist_ok=True)
    ext = f.filename.rsplit('.', 1)[1].lower()
    filename = f'bg.{ext}'
    f.save(os.path.join(user_dir, filename))

    bg_url = f'/static/uploads/{user_id}/{filename}'
    c.execute('INSERT OR IGNORE INTO user_profile (user_id) VALUES (?)', (user_id,))
    c.execute('UPDATE user_profile SET background_url = ? WHERE user_id = ?', (bg_url, user_id))
    conn.commit()
    conn.close()

    return jsonify({'success': True, 'background_url': bg_url})


@app.route('/api/user/stats')
def api_user_stats():
    """Получить XP и уровень текущего пользователя (для хедера)"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'authenticated': False})

    # Проверяем in-memory кеш (30 сек)
    now = time.time()
    with _stats_cache_lock:
        cached = _stats_cache.get(user_id)
        if cached and cached['expires'] > now:
            return jsonify(cached['data'])

    stats = get_or_create_user_stats(user_id)
    if not stats:
        return jsonify({'authenticated': True, 'xp': 0, 'level': 1, 'coins': 0})

    data = {
        'authenticated': True,
        'xp': stats['xp'],
        'coins': stats['coins'],
        'level': stats['level'],
        'xp_progress_pct': min(100, int(
            (stats['xp'] - get_xp_for_level(stats['level'])) /
            max(1, get_xp_for_level(stats['level'] + 1) - get_xp_for_level(stats['level'])) * 100
        ))
    }

    with _stats_cache_lock:
        _stats_cache[user_id] = {'data': data, 'expires': now + 30}

    return jsonify(data)


# ==================== БИБЛИОТЕКА И КОЛЛЕКЦИИ ====================

@app.route('/api/user/history')
def api_user_history():
    """История чтения пользователя"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify([])
    limit = min(int(request.args.get('limit', 50)), 200)
    offset = int(request.args.get('offset', 0))
    conn = get_db()
    c = conn.cursor()
    c.execute(
        '''SELECT m.manga_title, m.manga_slug, m.cover_url, m.manga_type,
                  c.chapter_number, c.chapter_slug, rh.last_read
           FROM reading_history rh
           JOIN manga m ON rh.manga_id = m.manga_id
           JOIN chapters c ON rh.chapter_id = c.chapter_id
           WHERE rh.user_id = ?
           ORDER BY rh.last_read DESC
           LIMIT ? OFFSET ?''',
        (user_id, limit, offset)
    )
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify(rows)


@app.route('/api/user/subscriptions')
def api_user_subscriptions():
    """Подписки пользователя (uid= для просмотра чужих)"""
    uid = request.args.get('uid', type=int)
    target_id = uid if uid else session.get('user_id')
    if not target_id:
        return jsonify([])
    conn = get_db()
    c = conn.cursor()
    c.execute(
        '''SELECT m.manga_id, m.manga_title, m.manga_slug, m.cover_url,
                  m.manga_type, m.manga_status, s.subscribed_at
           FROM subscriptions s
           JOIN manga m ON s.manga_id = m.manga_id
           WHERE s.user_id = ?
           ORDER BY s.subscribed_at DESC''',
        (target_id,)
    )
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify(rows)


@app.route('/api/user/collections')
def api_user_collections():
    """Коллекции пользователя (uid= для просмотра чужих публичных)"""
    uid = request.args.get('uid', type=int)
    viewer_id = session.get('user_id')
    if uid:
        target_id = uid
        only_public = True
    else:
        target_id = viewer_id
        only_public = False
    if not target_id:
        return jsonify([])
    conn = get_db()
    c = conn.cursor()
    if only_public:
        c.execute(
            '''SELECT c.id, c.name, c.description, c.cover_url, c.is_public, c.created_at,
                      COUNT(DISTINCT ci.manga_id) as items_count,
                      COUNT(DISTINCT lk.user_id) as likes_count
               FROM collections c
               LEFT JOIN collection_items ci ON c.id = ci.collection_id
               LEFT JOIN collection_likes lk ON c.id = lk.collection_id
               WHERE c.user_id = ? AND c.is_public = 1
               GROUP BY c.id
               ORDER BY c.created_at DESC''',
            (target_id,)
        )
    else:
        c.execute(
            '''SELECT c.id, c.name, c.description, c.cover_url, c.is_public, c.created_at,
                      COUNT(DISTINCT ci.manga_id) as items_count,
                      COUNT(DISTINCT lk.user_id) as likes_count
               FROM collections c
               LEFT JOIN collection_items ci ON c.id = ci.collection_id
               LEFT JOIN collection_likes lk ON c.id = lk.collection_id
               WHERE c.user_id = ?
               GROUP BY c.id
               ORDER BY c.created_at DESC''',
            (target_id,)
        )
    rows = [dict(r) for r in c.fetchall()]
    # Пометить лайкнутые текущим пользователем
    if viewer_id and rows:
        ids = [r['id'] for r in rows]
        placeholders = ','.join('?' * len(ids))
        c.execute(f'SELECT collection_id FROM collection_likes WHERE user_id = ? AND collection_id IN ({placeholders})',
                  [viewer_id] + ids)
        liked_ids = {r[0] for r in c.fetchall()}
        for r in rows:
            r['my_like'] = r['id'] in liked_ids
    conn.close()
    return jsonify(rows)


@app.route('/api/users/<int:author_id>/follow-curator', methods=['POST'])
def api_follow_curator(author_id):
    """Toggle подписки на куратора"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    if user_id == author_id:
        return jsonify({'error': 'Нельзя подписаться на себя'}), 400
    conn = get_db()
    c = conn.cursor()
    existing = c.execute('SELECT id FROM curator_follows WHERE follower_id=? AND author_id=?',
                         (user_id, author_id)).fetchone()
    if existing:
        c.execute('DELETE FROM curator_follows WHERE follower_id=? AND author_id=?', (user_id, author_id))
        following = False
    else:
        c.execute('INSERT OR IGNORE INTO curator_follows (follower_id, author_id) VALUES (?,?)',
                  (user_id, author_id))
        following = True
    conn.commit()
    cnt = c.execute('SELECT COUNT(*) FROM curator_follows WHERE author_id=?', (author_id,)).fetchone()[0]
    conn.close()
    return jsonify({'following': following, 'followers_count': cnt})


@app.route('/api/users/<int:author_id>/followers')
def api_curator_followers(author_id):
    """Число подписчиков куратора и статус текущего пользователя"""
    user_id = session.get('user_id')
    conn = get_db()
    c = conn.cursor()
    cnt = c.execute('SELECT COUNT(*) FROM curator_follows WHERE author_id=?', (author_id,)).fetchone()[0]
    following = False
    if user_id:
        following = bool(c.execute('SELECT id FROM curator_follows WHERE follower_id=? AND author_id=?',
                                   (user_id, author_id)).fetchone())
    conn.close()
    return jsonify({'followers_count': cnt, 'following': following})


@app.route('/api/user/following-curators')
def api_following_curators():
    """Список кураторов, на которых подписан текущий пользователь"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'curators': []})
    conn = get_db()
    c = conn.cursor()
    rows = c.execute(
        '''SELECT u.id, u.telegram_first_name, u.telegram_username, u.is_premium,
                  up.custom_name, up.custom_avatar_url
           FROM curator_follows cf
           JOIN users u ON cf.author_id = u.id
           LEFT JOIN user_profile up ON u.id = up.user_id
           WHERE cf.follower_id = ?
           ORDER BY cf.created_at DESC''',
        (user_id,)
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        display = (r['custom_name'] or '').strip() or r['telegram_first_name'] or r['telegram_username'] or f'Пользователь #{r["id"]}'
        result.append({'id': r['id'], 'display_name': display, 'avatar_url': r['custom_avatar_url'], 'is_premium': r['is_premium']})
    return jsonify({'curators': result})


@app.route('/api/collections', methods=['POST'])
def api_create_collection():
    """Создать коллекцию"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()[:100]
    if not name:
        return jsonify({'error': 'Название обязательно'}), 400
    description = (data.get('description') or '').strip()[:500]
    is_public = 1 if data.get('is_public', True) else 0
    conn = get_db()
    c = conn.cursor()
    c.execute(
        'INSERT INTO collections (user_id, name, description, is_public) VALUES (?, ?, ?, ?)',
        (user_id, name, description, is_public)
    )
    conn.commit()
    new_id = c.lastrowid
    # Определяем имя автора
    c.execute('SELECT telegram_first_name, telegram_username FROM users WHERE id=?', (user_id,))
    _author = c.fetchone()
    author_name = (
        (_author['telegram_first_name'] or _author['telegram_username'] or f'Пользователь #{user_id}')
        if _author else f'Пользователь #{user_id}'
    )
    # Уведомляем подписчиков-кураторов
    followers = c.execute('SELECT follower_id FROM curator_follows WHERE author_id=?', (user_id,)).fetchall()
    conn.close()
    for f in followers:
        create_site_notification(
            f['follower_id'], 'new_collection',
            f'{author_name} создал(а) коллекцию',
            name, f'/collection/{new_id}'
        )
    return jsonify({'success': True, 'id': new_id, 'name': name, 'items_count': 0})


@app.route('/api/collections/<int:coll_id>', methods=['PUT'])
def api_update_collection(coll_id):
    """Обновить коллекцию"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()[:100]
    if not name:
        return jsonify({'error': 'Название обязательно'}), 400
    description = (data.get('description') or '').strip()[:500]
    is_public = 1 if data.get('is_public', True) else 0
    conn = get_db()
    c = conn.cursor()
    c.execute(
        'UPDATE collections SET name=?, description=?, is_public=? WHERE id=? AND user_id=?',
        (name, description, is_public, coll_id, user_id)
    )
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/collections/<int:coll_id>', methods=['DELETE'])
def api_delete_collection(coll_id):
    """Удалить коллекцию"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    c = conn.cursor()
    c.execute('DELETE FROM collection_items WHERE collection_id = ?', (coll_id,))
    c.execute('DELETE FROM collections WHERE id = ? AND user_id = ?', (coll_id, user_id))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/collections/<int:coll_id>/items', methods=['GET'])
def api_collection_items(coll_id):
    """Манги в коллекции"""
    conn = get_db()
    c = conn.cursor()
    c.execute(
        '''SELECT m.manga_id, m.manga_title, m.manga_slug, m.cover_url, m.manga_type, ci.added_at
           FROM collection_items ci
           JOIN manga m ON ci.manga_id = m.manga_id
           WHERE ci.collection_id = ?
           ORDER BY ci.added_at DESC''',
        (coll_id,)
    )
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify(rows)


@app.route('/api/collections/<int:coll_id>/manga', methods=['POST'])
def api_add_to_collection(coll_id):
    """Добавить мангу в коллекцию"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    data = request.get_json(silent=True) or {}
    manga_id = (data.get('manga_id') or '').strip()
    if not manga_id:
        return jsonify({'error': 'manga_id обязателен'}), 400
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id FROM collections WHERE id = ? AND user_id = ?', (coll_id, user_id))
    if not c.fetchone():
        conn.close()
        return jsonify({'error': 'Коллекция не найдена'}), 404
    try:
        c.execute(
            'INSERT OR IGNORE INTO collection_items (collection_id, manga_id) VALUES (?, ?)',
            (coll_id, manga_id)
        )
        conn.commit()
    except Exception:
        pass
    conn.close()
    return jsonify({'success': True})


@app.route('/api/collections/<int:coll_id>/manga/<manga_id>', methods=['DELETE'])
def api_remove_from_collection(coll_id, manga_id):
    """Удалить мангу из коллекции"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id FROM collections WHERE id = ? AND user_id = ?', (coll_id, user_id))
    if not c.fetchone():
        conn.close()
        return jsonify({'error': 'Коллекция не найдена'}), 404
    c.execute(
        'DELETE FROM collection_items WHERE collection_id = ? AND manga_id = ?',
        (coll_id, manga_id)
    )
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/collections/<int:coll_id>/like', methods=['POST', 'DELETE'])
def api_collection_like(coll_id):
    """Лайк / анлайк коллекции"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id, is_public, user_id FROM collections WHERE id = ?', (coll_id,))
    coll = c.fetchone()
    if not coll:
        conn.close()
        return jsonify({'error': 'Не найдено'}), 404
    if not coll['is_public'] and coll['user_id'] != user_id:
        conn.close()
        return jsonify({'error': 'Доступ запрещён'}), 403
    if request.method == 'POST':
        c.execute('INSERT OR IGNORE INTO collection_likes (user_id, collection_id) VALUES (?, ?)', (user_id, coll_id))
    else:
        c.execute('DELETE FROM collection_likes WHERE user_id = ? AND collection_id = ?', (user_id, coll_id))
    conn.commit()
    c.execute('SELECT COUNT(*) as cnt FROM collection_likes WHERE collection_id = ?', (coll_id,))
    likes_count = c.fetchone()['cnt']
    conn.close()
    return jsonify({'success': True, 'likes_count': likes_count, 'my_like': request.method == 'POST'})


@app.route('/upload/collection-cover', methods=['POST'])
def upload_collection_cover():
    """Загрузить обложку коллекции"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    coll_id = request.form.get('collection_id', type=int)
    if not coll_id:
        return jsonify({'error': 'collection_id обязателен'}), 400
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id FROM collections WHERE id = ? AND user_id = ?', (coll_id, user_id))
    if not c.fetchone():
        conn.close()
        return jsonify({'error': 'Коллекция не найдена'}), 404
    if 'file' not in request.files:
        conn.close()
        return jsonify({'error': 'Файл не выбран'}), 400
    f = request.files['file']
    if not f.filename or not _allowed_file(f.filename):
        conn.close()
        return jsonify({'error': 'Недопустимый формат файла'}), 400
    coll_dir = os.path.join(UPLOAD_FOLDER, 'collections', str(coll_id))
    os.makedirs(coll_dir, exist_ok=True)
    ext = f.filename.rsplit('.', 1)[1].lower()
    filename = f'cover.{ext}'
    f.save(os.path.join(coll_dir, filename))
    cover_url = f'/static/uploads/collections/{coll_id}/{filename}'
    c.execute('UPDATE collections SET cover_url = ? WHERE id = ?', (cover_url, coll_id))
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'cover_url': cover_url})


@app.route('/collection/<int:coll_id>')
def collection_detail(coll_id):
    """Страница коллекции"""
    conn = get_db()
    c = conn.cursor()
    c.execute(
        '''SELECT c.id, c.name, c.description, c.cover_url, c.is_public, c.created_at, c.user_id,
                  COUNT(DISTINCT ci.manga_id) as items_count,
                  COUNT(DISTINCT lk.user_id) as likes_count,
                  COALESCE(p.custom_avatar_url, p.avatar_url) as owner_avatar,
                  NULLIF(TRIM(COALESCE(p.custom_name, '')), '') as owner_name,
                  u.telegram_first_name, u.telegram_username
           FROM collections c
           LEFT JOIN collection_items ci ON c.id = ci.collection_id
           LEFT JOIN collection_likes lk ON c.id = lk.collection_id
           LEFT JOIN user_profile p ON c.user_id = p.user_id
           LEFT JOIN users u ON c.user_id = u.id
           WHERE c.id = ?
           GROUP BY c.id''',
        (coll_id,)
    )
    row = c.fetchone()
    if not row:
        conn.close()
        abort(404)
    coll = dict(row)
    user_id = session.get('user_id')
    if not coll['is_public'] and coll['user_id'] != user_id:
        conn.close()
        abort(403)
    my_like = False
    if user_id:
        c.execute('SELECT 1 FROM collection_likes WHERE user_id = ? AND collection_id = ?', (user_id, coll_id))
        my_like = c.fetchone() is not None
    c.execute(
        '''SELECT m.manga_id, m.manga_title, m.manga_slug, m.cover_url, m.manga_type
           FROM collection_items ci
           JOIN manga m ON ci.manga_id = m.manga_id
           WHERE ci.collection_id = ?
           ORDER BY ci.added_at DESC''',
        (coll_id,)
    )
    items = [dict(r) for r in c.fetchall()]
    conn.close()
    owner_name = (coll['owner_name'] or coll['telegram_first_name'] or
                  coll['telegram_username'] or f'#{coll["user_id"]}')
    is_owner = (user_id == coll['user_id'])
    return render_template('collection_detail.html',
                           coll=coll, items=items, my_like=my_like,
                           owner_name=owner_name, user_id=user_id, is_owner=is_owner)


@app.route('/collections/top')
def collections_top_page():
    """Топ коллекций"""
    conn = get_db()
    c = conn.cursor()
    user_id = session.get('user_id')
    c.execute(
        '''SELECT c.id, c.name, c.description, c.cover_url, c.created_at, c.user_id,
                  COUNT(DISTINCT ci.manga_id) as items_count,
                  COUNT(DISTINCT lk.user_id) as likes_count,
                  COALESCE(p.custom_avatar_url, p.avatar_url) as owner_avatar,
                  NULLIF(TRIM(COALESCE(p.custom_name, '')), '') as owner_name,
                  u.telegram_first_name, u.telegram_username
           FROM collections c
           LEFT JOIN collection_items ci ON c.id = ci.collection_id
           LEFT JOIN collection_likes lk ON c.id = lk.collection_id
           LEFT JOIN user_profile p ON c.user_id = p.user_id
           LEFT JOIN users u ON c.user_id = u.id
           WHERE c.is_public = 1
           GROUP BY c.id
           ORDER BY likes_count DESC, items_count DESC, c.created_at DESC
           LIMIT 50'''
    )
    collections = []
    for row in c.fetchall():
        d = dict(row)
        d['owner_name'] = (d['owner_name'] or d['telegram_first_name'] or
                           d['telegram_username'] or f'#{d["user_id"]}')
        collections.append(d)
    my_likes = set()
    if user_id and collections:
        ids = [d['id'] for d in collections]
        placeholders = ','.join('?' * len(ids))
        c.execute(f'SELECT collection_id FROM collection_likes WHERE user_id = ? AND collection_id IN ({placeholders})',
                  [user_id] + ids)
        my_likes = {r[0] for r in c.fetchall()}
    conn.close()
    return render_template('collections_top.html',
                           collections=collections, my_likes=my_likes, user_id=user_id)


# ==================== АДМИНКА ====================

def admin_required(f):
    """Декоратор: проверяем, что текущий пользователь — администратор"""
    @wraps(f)
    def decorated(*args, **kwargs):
        user_id = session.get('user_id')
        if not user_id:
            return redirect(url_for('index'))
        conn = get_db()
        c = conn.cursor()
        c.execute('SELECT telegram_id FROM users WHERE id = ?', (user_id,))
        row = c.fetchone()
        conn.close()
        if not row or row['telegram_id'] not in ADMIN_TELEGRAM_IDS:
            return "403 Forbidden", 403
        return f(*args, **kwargs)
    return decorated


@app.route('/api/user/quests')
def api_user_quests():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    conn = get_db()
    c = conn.cursor()

    c.execute('SELECT level FROM user_stats WHERE user_id = ?', (user_id,))
    row = c.fetchone()
    current_level = row['level'] if row else 1

    c.execute(
        '''SELECT q.*,
                  COALESCE(uq.progress, 0) as progress,
                  uq.completed_at
           FROM quests q
           LEFT JOIN user_quests uq ON uq.quest_id = q.id AND uq.user_id = ?
           WHERE q.is_active = 1 AND q.required_level <= ?
           ORDER BY q.required_level ASC, q.id ASC''',
        (user_id, current_level)
    )
    quests = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({'quests': quests, 'current_level': current_level})


@app.route('/admin')
@admin_required
def admin_panel():
    return render_template('admin.html')


# ── Статистика ──────────────────────────────────────────────────────────────

@app.route('/api/admin/stats')
@admin_required
def api_admin_stats():
    conn = get_db()
    c = conn.cursor()

    c.execute('SELECT COUNT(*) FROM users')
    total_users = c.fetchone()[0]

    c.execute('SELECT COUNT(*) FROM users WHERE is_active = 0')
    banned_users = c.fetchone()[0]

    c.execute('SELECT COUNT(*) FROM users WHERE is_premium = 1')
    premium_users = c.fetchone()[0]

    # Новые пользователи за последние 7 дней
    c.execute("SELECT COUNT(*) FROM users WHERE created_at >= datetime('now', '-7 days')")
    new_users_week = c.fetchone()[0]

    c.execute('SELECT COUNT(*) FROM manga')
    total_manga = c.fetchone()[0]

    c.execute('SELECT COUNT(*) FROM chapters')
    total_chapters = c.fetchone()[0]

    c.execute('SELECT COUNT(*) FROM comments')
    total_comments = c.fetchone()[0]

    c.execute('SELECT COUNT(*) FROM subscriptions')
    total_subscriptions = c.fetchone()[0]

    c.execute('SELECT COUNT(*) FROM reading_history')
    total_history = c.fetchone()[0]

    c.execute('SELECT SUM(total_chapters_read) FROM user_stats')
    row = c.fetchone()
    total_chapters_read = row[0] or 0

    # Топ-10 по XP
    c.execute('''SELECT u.id, u.telegram_username, u.telegram_first_name,
                        COALESCE(up.custom_name,'') as custom_name,
                        s.xp, s.level, s.coins
                 FROM user_stats s
                 JOIN users u ON s.user_id = u.id
                 LEFT JOIN user_profile up ON up.user_id = u.id
                 ORDER BY s.xp DESC LIMIT 10''')
    top_users = [dict(r) for r in c.fetchall()]

    # Топ манги по просмотрам
    c.execute('SELECT manga_id, manga_title, manga_slug, views FROM manga ORDER BY views DESC LIMIT 10')
    top_manga = [dict(r) for r in c.fetchall()]

    # Активность — регистрации по дням (последние 14 дней)
    c.execute("""SELECT date(created_at) as day, COUNT(*) as cnt
                 FROM users
                 WHERE created_at >= datetime('now', '-14 days')
                 GROUP BY day ORDER BY day""")
    reg_activity = [dict(r) for r in c.fetchall()]

    conn.close()
    return jsonify({
        'users': {
            'total': total_users,
            'banned': banned_users,
            'premium': premium_users,
            'new_week': new_users_week,
        },
        'manga': {'total': total_manga},
        'chapters': {'total': total_chapters},
        'comments': {'total': total_comments},
        'subscriptions': {'total': total_subscriptions},
        'history': {'total': total_history, 'chapters_read': total_chapters_read},
        'top_users': top_users,
        'top_manga': top_manga,
        'reg_activity': reg_activity,
    })


# ── Пользователи ────────────────────────────────────────────────────────────

@app.route('/api/admin/users')
@admin_required
def api_admin_users():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 30))
    search = request.args.get('q', '').strip()
    offset = (page - 1) * per_page

    conn = get_db()
    c = conn.cursor()

    where = ''
    params = []
    if search:
        where = "WHERE u.telegram_username LIKE ? OR u.telegram_first_name LIKE ? OR CAST(u.telegram_id AS TEXT) LIKE ? OR COALESCE(up.custom_name,'') LIKE ?"
        like = f'%{search}%'
        params = [like, like, like, like]

    c.execute(f'''SELECT COUNT(*) FROM users u LEFT JOIN user_profile up ON up.user_id = u.id {where}''', params)
    total = c.fetchone()[0]

    c.execute(f'''
        SELECT u.id, u.telegram_id, u.telegram_username, u.telegram_first_name,
               u.is_active, u.is_premium, u.premium_expires_at,
               u.created_at, u.last_login,
               COALESCE(up.custom_name,'') as custom_name,
               COALESCE(up.avatar_url,'') as avatar_url,
               COALESCE(s.xp,0) as xp, COALESCE(s.level,1) as level, COALESCE(s.coins,0) as coins,
               COALESCE(s.total_chapters_read,0) as chapters_read,
               (SELECT COUNT(*) FROM subscriptions WHERE user_id=u.id) as sub_count,
               (SELECT COUNT(*) FROM comments WHERE user_id=u.id) as comment_count
        FROM users u
        LEFT JOIN user_profile up ON up.user_id = u.id
        LEFT JOIN user_stats s ON s.user_id = u.id
        {where}
        ORDER BY u.id DESC
        LIMIT ? OFFSET ?
    ''', params + [per_page, offset])
    users = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({'users': users, 'total': total, 'page': page, 'per_page': per_page})


@app.route('/api/admin/users/<int:uid>/ban', methods=['POST'])
@admin_required
def api_admin_ban_user(uid):
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT is_active FROM users WHERE id = ?', (uid,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'Пользователь не найден'}), 404
    new_state = 0 if row['is_active'] else 1
    c.execute('UPDATE users SET is_active = ? WHERE id = ?', (new_state, uid))
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'is_active': new_state})


@app.route('/api/admin/users/<int:uid>/premium', methods=['POST'])
@admin_required
def api_admin_set_premium(uid):
    data = request.get_json(silent=True) or {}
    enabled = bool(data.get('enabled', True))
    days = int(data.get('days', 30))
    conn = get_db()
    c = conn.cursor()
    if enabled:
        expires = (datetime.utcnow() + timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        c.execute('UPDATE users SET is_premium=1, premium_expires_at=? WHERE id=?', (expires, uid))
    else:
        c.execute('UPDATE users SET is_premium=0, premium_expires_at=NULL WHERE id=?', (uid,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/admin/users/<int:uid>/coins', methods=['POST'])
@admin_required
def api_admin_set_coins(uid):
    data = request.get_json(silent=True) or {}
    amount = int(data.get('amount', 0))
    mode = data.get('mode', 'set')  # 'set' | 'add'
    conn = get_db()
    c = conn.cursor()
    get_or_create_user_stats(uid, conn)
    if mode == 'add':
        c.execute('UPDATE user_stats SET coins = coins + ? WHERE user_id = ?', (amount, uid))
    else:
        c.execute('UPDATE user_stats SET coins = ? WHERE user_id = ?', (amount, uid))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/admin/users/<int:uid>/xp', methods=['POST'])
@admin_required
def api_admin_set_xp(uid):
    data = request.get_json(silent=True) or {}
    amount = int(data.get('amount', 0))
    mode = data.get('mode', 'set')
    conn = get_db()
    c = conn.cursor()
    get_or_create_user_stats(uid, conn)
    if mode == 'add':
        c.execute('UPDATE user_stats SET xp = xp + ? WHERE user_id = ?', (amount, uid))
    else:
        c.execute('UPDATE user_stats SET xp = ? WHERE user_id = ?', (amount, uid))
    # Пересчитываем уровень
    c.execute('SELECT xp FROM user_stats WHERE user_id = ?', (uid,))
    row = c.fetchone()
    if row:
        new_level = get_level_from_xp(row['xp'])
        c.execute('UPDATE user_stats SET level = ? WHERE user_id = ?', (new_level, uid))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/admin/users/<int:uid>/subscriptions', methods=['GET'])
@admin_required
def api_admin_user_subs(uid):
    conn = get_db()
    c = conn.cursor()
    c.execute('''SELECT s.id, s.manga_id, s.subscribed_at,
                        COALESCE(m.manga_title,'') as manga_title, COALESCE(m.manga_slug,'') as manga_slug
                 FROM subscriptions s
                 LEFT JOIN manga m ON m.manga_id = s.manga_id
                 WHERE s.user_id = ?
                 ORDER BY s.subscribed_at DESC''', (uid,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify(rows)


@app.route('/api/admin/users/<int:uid>/subscriptions/<manga_id>', methods=['DELETE'])
@admin_required
def api_admin_remove_sub(uid, manga_id):
    conn = get_db()
    c = conn.cursor()
    c.execute('DELETE FROM subscriptions WHERE user_id=? AND manga_id=?', (uid, manga_id))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/admin/users/<int:uid>/subscriptions', methods=['POST'])
@admin_required
def api_admin_add_sub(uid):
    data = request.get_json(silent=True) or {}
    manga_id = (data.get('manga_id') or '').strip()
    if not manga_id:
        return jsonify({'error': 'manga_id обязателен'}), 400
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('INSERT OR IGNORE INTO subscriptions (user_id, manga_id) VALUES (?,?)', (uid, manga_id))
        conn.commit()
    except Exception:
        pass
    conn.close()
    return jsonify({'success': True})


# ── Комментарии ─────────────────────────────────────────────────────────────

@app.route('/api/admin/comments')
@admin_required
def api_admin_comments():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    search = request.args.get('q', '').strip()
    offset = (page - 1) * per_page

    conn = get_db()
    c = conn.cursor()

    where = ''
    params: list = []
    if search:
        where = "WHERE c.text LIKE ? OR u.telegram_username LIKE ? OR c.manga_slug LIKE ?"
        like = f'%{search}%'
        params = [like, like, like]

    c.execute(f'SELECT COUNT(*) FROM comments c JOIN users u ON c.user_id=u.id {where}', params)
    total = c.fetchone()[0]

    c.execute(f'''
        SELECT c.id, c.manga_slug, c.text, c.created_at,
               u.id as user_id, u.telegram_username, u.telegram_first_name,
               COALESCE(up.custom_name,'') as custom_name,
               COALESCE(up.avatar_url,'') as avatar_url,
               COALESCE(m.manga_title,'') as manga_title
        FROM comments c
        JOIN users u ON c.user_id = u.id
        LEFT JOIN user_profile up ON up.user_id = u.id
        LEFT JOIN manga m ON m.manga_slug = c.manga_slug
        {where}
        ORDER BY c.created_at DESC
        LIMIT ? OFFSET ?
    ''', params + [per_page, offset])
    comments = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({'comments': comments, 'total': total, 'page': page, 'per_page': per_page})


@app.route('/api/admin/comments/<int:cid>', methods=['DELETE'])
@admin_required
def api_admin_delete_comment(cid):
    conn = get_db()
    c = conn.cursor()
    c.execute('DELETE FROM comments WHERE id = ?', (cid,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


# ── Магазин ─────────────────────────────────────────────────────────────────

SHOP_UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'static', 'uploads', 'shop')

@app.route('/api/admin/shop/upload', methods=['POST'])
@admin_required
def api_admin_shop_upload():
    """Загрузить файл для товара магазина (аватар, фон, значок, рамка-картинка)"""
    if 'file' not in request.files:
        return jsonify({'error': 'Файл не выбран'}), 400
    f = request.files['file']
    if not f.filename or not _allowed_file(f.filename):
        return jsonify({'error': 'Недопустимый формат файла (png/jpg/jpeg/gif/webp)'}), 400

    os.makedirs(SHOP_UPLOAD_FOLDER, exist_ok=True)
    ext = f.filename.rsplit('.', 1)[1].lower()
    filename = f'{secrets.token_hex(12)}.{ext}'
    f.save(os.path.join(SHOP_UPLOAD_FOLDER, filename))
    url = f'/static/uploads/shop/{filename}'
    return jsonify({'success': True, 'url': url})


@app.route('/api/admin/shop', methods=['GET'])
@admin_required
def api_admin_shop_items():
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM shop_items ORDER BY type, id')
    items = [dict(r) for r in c.fetchall()]
    # Добавляем кол-во покупок
    for it in items:
        c.execute('SELECT COUNT(*) FROM user_items WHERE item_id=?', (it['id'],))
        it['purchases'] = c.fetchone()[0]
    conn.close()
    return jsonify(items)


@app.route('/api/admin/shop', methods=['POST'])
@admin_required
def api_admin_shop_create():
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'name обязателен'}), 400
    conn = get_db()
    c = conn.cursor()
    c.execute(
        'INSERT INTO shop_items (name, description, type, preview_url, css_value, price, is_upload, is_animated, duration_days) VALUES (?,?,?,?,?,?,?,?,?)',
        (
            name,
            (data.get('description') or '').strip(),
            (data.get('type') or 'frame').strip(),
            (data.get('preview_url') or '').strip() or None,
            (data.get('css_value') or '').strip(),
            int(data.get('price', 0)),
            int(bool(data.get('is_upload', False))),
            int(bool(data.get('is_animated', False))),
            int(data['duration_days']) if data.get('duration_days') else None,
        )
    )
    conn.commit()
    new_id = c.lastrowid
    conn.close()
    return jsonify({'success': True, 'id': new_id})


@app.route('/api/admin/shop/<int:item_id>', methods=['PUT'])
@admin_required
def api_admin_shop_update(item_id):
    data = request.get_json(silent=True) or {}
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id FROM shop_items WHERE id=?', (item_id,))
    if not c.fetchone():
        conn.close()
        return jsonify({'error': 'Товар не найден'}), 404
    c.execute(
        '''UPDATE shop_items SET name=?, description=?, type=?, preview_url=?,
           css_value=?, price=?, is_upload=?, is_animated=?, duration_days=? WHERE id=?''',
        (
            (data.get('name') or '').strip(),
            (data.get('description') or '').strip(),
            (data.get('type') or 'frame').strip(),
            (data.get('preview_url') or '').strip() or None,
            (data.get('css_value') or '').strip(),
            int(data.get('price', 0)),
            int(bool(data.get('is_upload', False))),
            int(bool(data.get('is_animated', False))),
            int(data['duration_days']) if data.get('duration_days') else None,
            item_id,
        )
    )
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/admin/shop/<int:item_id>', methods=['DELETE'])
@admin_required
def api_admin_shop_delete(item_id):
    conn = get_db()
    c = conn.cursor()
    c.execute('DELETE FROM user_items WHERE item_id=?', (item_id,))
    c.execute('DELETE FROM shop_items WHERE id=?', (item_id,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


# ── Манга (в БД) ─────────────────────────────────────────────────────────────

@app.route('/api/admin/manga')
@admin_required
def api_admin_manga():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 30))
    search = request.args.get('q', '').strip()
    offset = (page - 1) * per_page

    conn = get_db()
    c = conn.cursor()

    where = ''
    params: list = []
    if search:
        where = 'WHERE manga_title LIKE ? OR manga_slug LIKE ?'
        like = f'%{search}%'
        params = [like, like]

    c.execute(f'SELECT COUNT(*) FROM manga {where}', params)
    total = c.fetchone()[0]

    c.execute(f'''
        SELECT manga_id, manga_slug, manga_title, manga_type, manga_status,
               cover_url, views, chapters_count, last_updated,
               (SELECT COUNT(*) FROM subscriptions WHERE manga_id=m.manga_id) as sub_count,
               (SELECT COUNT(*) FROM comments WHERE manga_slug=m.manga_slug) as comment_count
        FROM manga m {where}
        ORDER BY views DESC
        LIMIT ? OFFSET ?
    ''', params + [per_page, offset])
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({'manga': rows, 'total': total, 'page': page, 'per_page': per_page})


# ── XP Лог ──────────────────────────────────────────────────────────────────

@app.route('/api/admin/xp_log')
@admin_required
def api_admin_xp_log():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    uid = request.args.get('uid')
    offset = (page - 1) * per_page

    conn = get_db()
    c = conn.cursor()

    where = 'WHERE x.user_id = ?' if uid else ''
    params: list = [int(uid)] if uid else []

    c.execute(f'SELECT COUNT(*) FROM xp_log x {where}', params)
    total = c.fetchone()[0]

    c.execute(f'''
        SELECT x.id, x.user_id, x.reason, x.ref_id, x.amount, x.created_at,
               u.telegram_username, u.telegram_first_name,
               COALESCE(up.custom_name,'') as custom_name
        FROM xp_log x
        JOIN users u ON u.id = x.user_id
        LEFT JOIN user_profile up ON up.user_id = x.user_id
        {where}
        ORDER BY x.created_at DESC
        LIMIT ? OFFSET ?
    ''', params + [per_page, offset])
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({'logs': rows, 'total': total})


# ── Достижения (admin CRUD) ──────────────────────────────────────────────────

ACHIEVEMENT_UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'static', 'uploads', 'achievements')

@app.route('/api/admin/achievements/upload', methods=['POST'])
@admin_required
def api_admin_achievements_upload():
    if 'file' not in request.files:
        return jsonify({'error': 'Файл не выбран'}), 400
    f = request.files['file']
    if not f.filename or not _allowed_file(f.filename):
        return jsonify({'error': 'Недопустимый формат файла'}), 400
    os.makedirs(ACHIEVEMENT_UPLOAD_FOLDER, exist_ok=True)
    ext = f.filename.rsplit('.', 1)[1].lower()
    filename = f'{secrets.token_hex(12)}.{ext}'
    f.save(os.path.join(ACHIEVEMENT_UPLOAD_FOLDER, filename))
    return jsonify({'success': True, 'url': f'/static/uploads/achievements/{filename}'})


@app.route('/api/admin/achievements', methods=['GET'])
@admin_required
def api_admin_achievements_list():
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM achievements ORDER BY id')
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({'achievements': rows})


@app.route('/api/admin/achievements', methods=['POST'])
@admin_required
def api_admin_achievements_create():
    data = request.get_json(silent=True) or {}
    key = data.get('key', '').strip()
    name = data.get('name', '').strip()
    description = data.get('description', '').strip()
    icon = data.get('icon', '🏆').strip()
    icon_url = data.get('icon_url', '').strip() or None
    xp_reward = int(data.get('xp_reward', 0))
    condition_type = data.get('condition_type', '').strip()
    condition_value = int(data.get('condition_value', 1))
    if not key or not name or not condition_type:
        return jsonify({'error': 'Обязательные поля: key, name, condition_type'}), 400
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute(
            'INSERT INTO achievements (key,name,description,icon,icon_url,xp_reward,condition_type,condition_value) VALUES (?,?,?,?,?,?,?,?)',
            (key, name, description, icon, icon_url, xp_reward, condition_type, condition_value)
        )
        conn.commit()
        ach_id = c.lastrowid
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({'error': 'Ключ уже существует'}), 409
    conn.close()
    return jsonify({'success': True, 'id': ach_id})


@app.route('/api/admin/achievements/<int:ach_id>', methods=['PUT'])
@admin_required
def api_admin_achievements_update(ach_id):
    data = request.get_json(silent=True) or {}
    conn = get_db()
    c = conn.cursor()
    fields, vals = [], []
    for col in ('key', 'name', 'description', 'icon', 'icon_url', 'xp_reward', 'condition_type', 'condition_value'):
        if col in data:
            fields.append(f'{col}=?')
            vals.append(data[col])
    if not fields:
        conn.close()
        return jsonify({'error': 'Нет данных'}), 400
    vals.append(ach_id)
    c.execute(f'UPDATE achievements SET {", ".join(fields)} WHERE id=?', vals)
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/admin/achievements/<int:ach_id>', methods=['DELETE'])
@admin_required
def api_admin_achievements_delete(ach_id):
    conn = get_db()
    c = conn.cursor()
    c.execute('DELETE FROM achievements WHERE id=?', (ach_id,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


# ── Задания (admin CRUD) ─────────────────────────────────────────────────────

QUEST_UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'static', 'uploads', 'quests')

@app.route('/api/admin/quests/upload', methods=['POST'])
@admin_required
def api_admin_quests_upload():
    if 'file' not in request.files:
        return jsonify({'error': 'Файл не выбран'}), 400
    f = request.files['file']
    if not f.filename or not _allowed_file(f.filename):
        return jsonify({'error': 'Недопустимый формат файла'}), 400
    os.makedirs(QUEST_UPLOAD_FOLDER, exist_ok=True)
    ext = f.filename.rsplit('.', 1)[1].lower()
    filename = f'{secrets.token_hex(12)}.{ext}'
    f.save(os.path.join(QUEST_UPLOAD_FOLDER, filename))
    return jsonify({'success': True, 'url': f'/static/uploads/quests/{filename}'})


@app.route('/api/admin/quests', methods=['GET'])
@admin_required
def api_admin_quests_list():
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM quests ORDER BY required_level, id')
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({'quests': rows})


@app.route('/api/admin/quests', methods=['POST'])
@admin_required
def api_admin_quests_create():
    data = request.get_json(silent=True) or {}
    title = data.get('title', '').strip()
    description = data.get('description', '').strip()
    icon = data.get('icon', '📋').strip()
    icon_url = data.get('icon_url', '').strip() or None
    required_level = int(data.get('required_level', 1))
    condition_type = data.get('condition_type', '').strip()
    condition_value = int(data.get('condition_value', 1))
    xp_reward = int(data.get('xp_reward', 0))
    coins_reward = int(data.get('coins_reward', 0))
    is_active = int(data.get('is_active', 1))
    if not title or not condition_type:
        return jsonify({'error': 'Обязательные поля: title, condition_type'}), 400
    conn = get_db()
    c = conn.cursor()
    c.execute(
        'INSERT INTO quests (title,description,icon,icon_url,required_level,condition_type,condition_value,xp_reward,coins_reward,is_active) VALUES (?,?,?,?,?,?,?,?,?,?)',
        (title, description, icon, icon_url, required_level, condition_type, condition_value, xp_reward, coins_reward, is_active)
    )
    conn.commit()
    qid = c.lastrowid
    conn.close()
    return jsonify({'success': True, 'id': qid})


@app.route('/api/admin/quests/<int:qid>', methods=['PUT'])
@admin_required
def api_admin_quests_update(qid):
    data = request.get_json(silent=True) or {}
    conn = get_db()
    c = conn.cursor()
    fields, vals = [], []
    for col in ('title', 'description', 'icon', 'icon_url', 'required_level', 'condition_type', 'condition_value', 'xp_reward', 'coins_reward', 'is_active'):
        if col in data:
            fields.append(f'{col}=?')
            vals.append(data[col])
    if not fields:
        conn.close()
        return jsonify({'error': 'Нет данных'}), 400
    vals.append(qid)
    c.execute(f'UPDATE quests SET {", ".join(fields)} WHERE id=?', vals)
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/admin/quests/<int:qid>', methods=['DELETE'])
@admin_required
def api_admin_quests_delete(qid):
    conn = get_db()
    c = conn.cursor()
    c.execute('DELETE FROM user_quests WHERE quest_id=?', (qid,))
    c.execute('DELETE FROM quests WHERE id=?', (qid,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


# ==================== КАЛЕНДАРЬ МАНГИ ====================

@app.route('/calendar')
def calendar_page():
    user_id = session.get('user_id')
    return render_template('calendar.html', user_id=user_id)


@app.route('/api/calendar/days')
def api_calendar_days():
    """Дни месяца, в которые выходили главы. ?year=YYYY&month=M"""
    year  = request.args.get('year',  type=int)
    month = request.args.get('month', type=int)
    if not year or not month:
        return jsonify([])

    prefix = f'{year:04d}-{month:02d}'
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT DATE(created_at) AS day, COUNT(*) AS count
               FROM chapters
               WHERE DATE(created_at) LIKE ?
               GROUP BY DATE(created_at)
               ORDER BY day""",
            (f'{prefix}%',)
        ).fetchall()
        return jsonify([{'day': row['day'], 'count': row['count']} for row in rows])
    except Exception as e:
        logger.error(f"api_calendar_days error: {e}")
        return jsonify([])
    finally:
        conn.close()


@app.route('/api/calendar/day')
def api_calendar_day():
    """Все главы, вышедшие в конкретный день. ?date=YYYY-MM-DD"""
    date = request.args.get('date', '').strip()
    if not date or len(date) != 10:
        return jsonify([])

    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT c.chapter_id, c.chapter_slug, c.chapter_number,
                      c.chapter_volume, c.chapter_name, c.chapter_url,
                      m.manga_title, m.cover_url, m.manga_slug, m.manga_type
               FROM chapters c
               JOIN manga m ON c.manga_id = m.manga_id
               WHERE DATE(c.created_at) = ?
               ORDER BY m.manga_title, CAST(c.chapter_number AS REAL)""",
            (date,)
        ).fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        logger.error(f"api_calendar_day error: {e}")
        return jsonify([])
    finally:
        conn.close()


# ==================== ADMIN: СЕЗОНЫ ====================

@app.route('/admin/api/seasons', methods=['POST'])
@admin_required
def admin_create_season():
    data = request.json or {}
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'error': 'name required'}), 400
    conn = get_db()
    conn.execute(
        'INSERT INTO seasons (name, description, icon, banner_url, starts_at, ends_at, is_active) VALUES (?,?,?,?,?,?,1)',
        (name, data.get('description',''), data.get('icon','🌸'), data.get('banner_url'), data.get('starts_at',''), data.get('ends_at',''))
    )
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@app.route('/admin/api/seasons/<int:season_id>/quests', methods=['POST'])
@admin_required
def admin_create_season_quest(season_id):
    data = request.json or {}
    conn = get_db()
    conn.execute(
        '''INSERT INTO season_quests (season_id, title, description, icon, condition_type, condition_value,
           xp_reward, coins_reward, item_reward_id) VALUES (?,?,?,?,?,?,?,?,?)''',
        (season_id, data.get('title',''), data.get('description',''), data.get('icon','✨'),
         data.get('condition_type','chapters_read'), int(data.get('condition_value',1)),
         int(data.get('xp_reward',0)), int(data.get('coins_reward',0)),
         data.get('item_reward_id'))
    )
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@app.route('/admin/api/seasons/<int:season_id>', methods=['DELETE'])
@admin_required
def admin_deactivate_season(season_id):
    conn = get_db()
    conn.execute('UPDATE seasons SET is_active=0 WHERE id=?', (season_id,))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@app.route('/admin/api/seasons')
@admin_required
def admin_list_seasons():
    conn = get_db()
    rows = conn.execute('SELECT * FROM seasons ORDER BY id DESC').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


# ==================== УВЕДОМЛЕНИЯ НА САЙТЕ ====================

@app.route('/api/notifications')
def api_notifications():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'not_auth'}), 401
    conn = get_db()
    rows = conn.execute(
        '''SELECT id, type, title, body, url, ref_id, is_read, created_at
           FROM site_notifications
           WHERE user_id = ?
           ORDER BY created_at DESC LIMIT 50''',
        (user_id,)
    ).fetchall()
    unread = conn.execute(
        'SELECT COUNT(*) as cnt FROM site_notifications WHERE user_id = ? AND is_read = 0',
        (user_id,)
    ).fetchone()['cnt']
    conn.close()
    return jsonify({'notifications': [dict(r) for r in rows], 'unread': unread})


@app.route('/api/notifications/read-all', methods=['POST'])
def api_notifications_read_all():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'not_auth'}), 401
    conn = get_db()
    conn.execute('UPDATE site_notifications SET is_read = 1 WHERE user_id = ?', (user_id,))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@app.route('/api/notifications/<int:notif_id>/read', methods=['POST'])
def api_notification_read(notif_id):
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'not_auth'}), 401
    conn = get_db()
    conn.execute(
        'UPDATE site_notifications SET is_read = 1 WHERE id = ? AND user_id = ?',
        (notif_id, user_id)
    )
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@app.route('/api/settings/digest-hour', methods=['POST'])
def api_set_digest_hour():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'not_auth'}), 401
    data = request.get_json(silent=True) or {}
    hour = data.get('hour')
    if hour is None or not isinstance(hour, int) or not (0 <= hour <= 23):
        return jsonify({'error': 'invalid_hour'}), 400
    conn = get_db()
    conn.execute('UPDATE users SET digest_hour = ? WHERE id = ?', (hour, user_id))
    conn.commit()
    conn.close()
    return jsonify({'ok': True, 'hour': hour})


# ==================== ЗАПУСК ====================

if __name__ == "__main__":
    init_db()
    
    # Запуск фонового процесса проверки новых глав
    checker_thread = threading.Thread(target=background_checker, daemon=True)
    checker_thread.start()
    
    # Запуск Telegram бота (теперь он сам создает поток)
    run_telegram_bot()
    
    print(f"🌐 Веб-сервер запущен на {SITE_URL}")
    socketio.run(app, debug=True, use_reloader=False,
                 host='0.0.0.0', port=80, allow_unsafe_werkzeug=True)