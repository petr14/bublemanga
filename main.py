import time
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request, session, redirect, url_for, Response, make_response
import threading
import sqlite3
import secrets
import json
import asyncio
import re
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters
)
from functools import wraps
import logging
from concurrent.futures import ThreadPoolExecutor
from flask_compress import Compress
from senkuro_api import SenkuroAPI


# Конфигурация логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Конфигурация
TELEGRAM_BOT_TOKEN = "7082209603:AAG97jX6MHgYOywy5hdDl03hduVMD6VBsW0"

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=30)
app.config['COMPRESS_MIMETYPES'] = [
    'text/html', 'text/css', 'application/json',
    'application/javascript', 'text/javascript'
]
app.config['COMPRESS_LEVEL'] = 6
Compress(app)

_TYPE_RU = {
    'MANGA': 'Манга', 'MANHWA': 'Манхва', 'MANHUA': 'Маньхуа',
    'OEL': 'OEL', 'NOVEL': 'Новелла', 'ONE_SHOT': 'Короткие истории',
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

# Клиент API Senkuro
api = SenkuroAPI()

# Словарь для отслеживания фоновой загрузки глав: manga_slug -> True
_manga_loading = {}

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

    # ── Индексы ────────────────────────────────────────────────────────────
    c.execute('CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_users_login_token ON users(login_token)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_manga_slug ON manga(manga_slug)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_subscriptions_user ON subscriptions(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_subscriptions_manga ON subscriptions(manga_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_search_user ON search_history(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_user_stats ON user_stats(xp DESC)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_xp_log ON xp_log(user_id, ref_id)')

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

    conn.commit()
    conn.close()
    print("✅ База данных инициализирована")

def get_db():
    conn = sqlite3.connect('manga.db', timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    return conn

# ==================== ГЕЙМИФИКАЦИЯ: XP / УРОВНИ / АЧИВКИ ====================

import math
import os
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

    # Проверяем ачивки
    new_achievements = check_achievements(user_id, conn)

    conn.close()

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

    conn.commit()
    if close:
        conn.close()
    return unlocked


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

    conn.close()

    xp = stats['xp'] if stats else 0
    level = stats['level'] if stats else 1
    xp_current_level = get_xp_for_level(level)
    xp_next_level = get_xp_for_level(level + 1)
    xp_progress = xp - xp_current_level
    xp_needed = xp_next_level - xp_current_level
    progress_pct = min(100, int(xp_progress / max(1, xp_needed) * 100))

    return {
        'user': dict(user),
        'stats': dict(stats) if stats else {},
        'profile': dict(profile) if profile else {},
        'achievements': achievements,
        'items': items,
        'history': history,
        'level': level,
        'xp': xp,
        'coins': stats['coins'] if stats else 0,
        'xp_progress_pct': progress_pct,
        'xp_for_next': xp_needed - xp_progress,
        'display_name': (
            dict(user).get('telegram_first_name') or
            dict(user).get('telegram_username') or
            f"Пользователь #{user_id}"
        )
    }

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
    
    return {
        'id': spotlight_id,
        'ru_title': ru_title,
        'en_title': en_title,
        'title': ru_title or en_title or f"Блок {spotlight_id}",
        'mangas': parsed_mangas
    }

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
            title = spotlight.get('title', '').lower()
            ru_title = spotlight.get('ru_title', '').lower()
            
            if any(keyword in title or keyword in ru_title for keyword in ['последние манги', 'last manga']):
                spotlights_by_type['last_manga'] = spotlight
            elif any(keyword in title or keyword in ru_title for keyword in ['популярные новинки', 'new popular']):
                spotlights_by_type['popular_new'] = spotlight
            elif any(keyword in title or keyword in ru_title for keyword in ['топ манхв', 'top manhwa']):
                spotlights_by_type['top_manhwa'] = spotlight
            elif any(keyword in title or keyword in ru_title for keyword in ['топ манхуа', 'top manhua']):
                spotlights_by_type['top_manhua'] = spotlight
            elif any(keyword in title or keyword in ru_title for keyword in ['топ манг', 'top manga']):
                spotlights_by_type['top_manga'] = spotlight
            elif any(keyword in title or keyword in ru_title for keyword in ['самое читаемое', 'most read']):
                spotlights_by_type['most_read'] = spotlight
            elif any(keyword in title or keyword in ru_title for keyword in ['последние обновления', 'latest updates']):
                spotlights_by_type['latest_updates'] = spotlight
            elif any(keyword in title or keyword in ru_title for keyword in ['лейблы', 'labels', 'жанры', 'genres']):
                spotlights_by_type['genres'] = spotlight
        
        # Если каких-то спотлайтов нет, создаем заглушки
        for key in spotlights_by_type:
            if spotlights_by_type[key] is None:
                # Используем первый спотлайт как заглушку
                if all_spotlights:
                    spotlights_by_type[key] = all_spotlights[0]
                else:
                    # Создаем пустой спотлайт
                    spotlights_by_type[key] = {
                        'id': key,
                        'title': key.replace('_', ' ').title(),
                        'mangas': []
                    }
        
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

def search_manga_api(query, limit=200):
    """Поиск манги через API с кешированием результатов в БД"""
    results = api.search(query)

    # Кешируем результаты в БД
    for manga in results:
        save_manga_search_result(manga)

    return results[:limit]

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
    c.execute('''INSERT INTO users 
                 (telegram_id, telegram_username, telegram_first_name, telegram_last_name, login_token) 
                 VALUES (?, ?, ?, ?, ?)''',
              (telegram_id, username, first_name, last_name, login_token))
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
    """Сохранить детали манги в БД"""
    conn = get_db()
    c = conn.cursor()
    
    try:
        # Проверяем, существует ли манга
        c.execute('SELECT manga_id FROM manga WHERE manga_id = ?', (manga_data['manga_id'],))
        existing = c.fetchone()
        
        if existing:
            # Обновляем существующую запись
            c.execute('''UPDATE manga SET 
                        manga_title = ?, manga_type = ?, manga_status = ?,
                        rating = ?, views = ?, cover_url = ?,
                        branch_id = ?, chapters_count = ?, last_updated = ?
                        WHERE manga_id = ?''',
                      (manga_data['manga_title'], manga_data['manga_type'],
                       manga_data['manga_status'], manga_data['rating'],
                       manga_data['views'], manga_data['cover_url'],
                       manga_data.get('branch_id'), manga_data.get('chapters_count', 0),
                       datetime.now(), manga_data['manga_id']))
        else:
            # Создаем новую запись
            c.execute('''INSERT INTO manga 
                        (manga_id, manga_slug, manga_title, manga_type, 
                         manga_status, rating, views, cover_url, 
                         branch_id, chapters_count, last_updated) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                      (manga_data['manga_id'], manga_data['manga_slug'],
                       manga_data['manga_title'], manga_data['manga_type'],
                       manga_data['manga_status'], manga_data['rating'],
                       manga_data['views'], manga_data['cover_url'],
                       manga_data.get('branch_id'), manga_data.get('chapters_count', 0),
                       datetime.now()))
        
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
                'chapter_url': f"http://144.31.49.103:5000/read/{manga_slug}/{latest_chapter.get('slug')}"
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
    chapter_url = f"http://144.31.49.103:5000/read/{manga_slug}/{chapter_slug}"

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
    c.execute('SELECT user_id FROM subscriptions WHERE manga_id = ?', (manga_id,))
    subscribers = c.fetchall()
    conn.close()
    
    for sub in subscribers:
        user_id = sub[0]
        asyncio.run(send_telegram_notification(
            user_id, 
            manga_title, 
            chapter_data, 
            chapter_url
        ))

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

    except Exception as e:
        logger.error(f"❌ Ошибка в check_new_chapters: {e}")
        import traceback
        traceback.print_exc()

def background_checker():
    """Фоновый процесс проверки"""
    logger.info("🤖 Фоновый мониторинг запущен!")
    check_new_chapters()
    
    while True:
        try:
            time.sleep(60)  # Проверка каждую минуту
            check_new_chapters()
        except Exception as e:
            logger.error(f"❌ Ошибка в background_checker: {e}")
            time.sleep(60)
# ==================== TELEGRAM BOT ====================

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
    
    login_url = f"http://144.31.49.103:5000/login/{user['login_token']}"
    webapp_url = f"http://144.31.49.103:5000"
    
    keyboard = [
        [InlineKeyboardButton("🌐 Открыть сайт", url=webapp_url)],
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
            url=f"http://144.31.49.103:5000/search?q={query}"
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
                           url=f"http://144.31.49.103:5000/login/{user['login_token']}")
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
    
    login_url = f"http://144.31.49.103:5000/login/{user['login_token']}"
    webapp_url = f"http://144.31.49.103:5000"
    
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


def run_telegram_bot():
    """Запуск Telegram бота"""
    global telegram_app
    
    def start_bot():
        """Запуск бота в отдельном потоке с явным созданием event loop"""
        try:
            # Явно создаем новый event loop для этого потока
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Теперь создаем приложение
            telegram_app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
            
            # Команды
            telegram_app.add_handler(CommandHandler("start", start_command))
            telegram_app.add_handler(CommandHandler("search", search_manga_command))
            
            # Callback кнопки
            telegram_app.add_handler(CallbackQueryHandler(handle_callback))
            
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
    data = get_popular_manga_from_api(period, 12)
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
    query = request.args.get('q', '').strip()
    user_id = session.get('user_id')
    
    if not query or len(query) < 2:
        return render_template('search.html', 
                             query=query,
                             results=[],
                             user_id=user_id)
    
    # Сохраняем историю поиска
    if user_id:
        save_search_history(user_id, query)
    
    # Ищем мангу
    results = search_manga_api(query, 30)
    
    return render_template('search.html',
                         query=query,
                         results=results,
                         user_id=user_id)

@app.route('/api/search/suggestions')
def search_suggestions():
    query = request.args.get('q', '').strip()
    user_id = session.get('user_id')
    
    if len(query) < 2:
        return jsonify([])
    
    suggestions = get_search_suggestions(query, 10)
    return jsonify(suggestions)

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
            'chapter_url': f"http://144.31.49.103:5000/read/{manga_slug_db}/{chapter_slug}"
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

    subscribed = False
    if user_id:
        subscribed = is_subscribed(user_id, chapter_dict['manga_id'])

    return render_template('chapter.html',
                          chapter=chapter_dict,
                          subscribed=subscribed,
                          user_id=user_id,
                          prev_chapter=prev_chapter,
                          next_chapter=next_chapter)
from datetime import datetime

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


@app.route('/manga/<manga_slug>')
def manga_detail(manga_slug):
    """ИСПРАВЛЕННАЯ ВЕРСИЯ - детальная страница манги"""
    
    # Проверяем параметр обновления
    force_refresh = request.args.get('refresh') == 'true'
    
    # Сначала пытаемся получить из БД
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM manga WHERE manga_slug = ?', (manga_slug,))
    manga_db = c.fetchone()
    
    # Проверяем свежесть данных (если не force_refresh)
    need_api_update = force_refresh
    if manga_db and not force_refresh:
        last_updated = manga_db['last_updated']
        # Обновляем если данные старше 1 часа
        if last_updated:
            try:
                last_update_time = datetime.fromisoformat(last_updated)
                if datetime.now() - last_update_time > timedelta(hours=1):
                    need_api_update = True
                    logger.info(f"Данные устарели для {manga_slug}, обновляем...")
            except:
                need_api_update = True
    elif not manga_db:
        need_api_update = True
    
    # Получаем главы из БД
    chapters_db = []
    if manga_db:
        manga_id = dict(manga_db)['manga_id']
        c.execute('''SELECT * FROM chapters 
                     WHERE manga_id = ? 
                     ORDER BY CAST(chapter_number AS FLOAT) DESC 
                     LIMIT 10000''', (manga_id,))
        chapters_db = [dict(row) for row in c.fetchall()]
        logger.info(f"📚 Найдено {len(chapters_db)} глав в БД для {manga_slug}")
    
    conn.close()
    
    # Если нужно обновление через API
    if need_api_update:
        logger.info(f"📄 Обновление данных через API для {manga_slug}")
        manga_details, chapters_api = get_manga_details_with_chapters(manga_slug, 10000)
        
        if not manga_details:
            if manga_db:
                # Используем данные из БД если API не ответил
                logger.warning(f"⚠️ API не ответил, используем данные из БД")
                manga_data = dict(manga_db)
                chapters = chapters_db
            else:
                return "Манга не найдена", 404
        else:
            # Используем данные из API
            manga_data = manga_details
            
            # Объединяем главы из API и БД
            chapters = []
            chapter_ids_seen = set()
            
            # Сначала добавляем главы из API (они свежее)
            for chapter in chapters_api:
                if chapter['chapter_id'] not in chapter_ids_seen:
                    chapters.append({
                        'chapter_id': chapter['chapter_id'],
                        'chapter_slug': chapter['chapter_slug'],
                        'chapter_number': chapter['chapter_number'],
                        'chapter_volume': chapter['chapter_volume'],
                        'chapter_name': chapter['chapter_name'],
                        'created_at': chapter['created_at'],
                        'chapter_url': f"http://144.31.49.103:5000/read/{manga_slug}/{chapter['chapter_slug']}"
                    })
                    chapter_ids_seen.add(chapter['chapter_id'])
            
            # Добавляем главы из БД которых нет в API
            for chapter in chapters_db:
                if chapter['chapter_id'] not in chapter_ids_seen:
                    chapters.append({
                        'chapter_id': chapter['chapter_id'],
                        'chapter_slug': chapter['chapter_slug'],
                        'chapter_number': chapter['chapter_number'],
                        'chapter_volume': chapter['chapter_volume'],
                        'chapter_name': chapter['chapter_name'],
                        'created_at': chapter.get('created_at'),
                        'chapter_url': chapter.get('chapter_url', 
                                      f"http://144.31.49.103:5000/read/{manga_slug}/{chapter['chapter_slug']}")
                    })
                    chapter_ids_seen.add(chapter['chapter_id'])
            
            logger.info(f"✅ Всего глав после объединения: {len(chapters)}")
    else:
        # Используем данные из БД
        logger.info(f"📦 Используем закешированные данные для {manga_slug}")
        manga_data = dict(manga_db)
        chapters = []
        for chapter in chapters_db:
            chapters.append({
                'chapter_id': chapter['chapter_id'],
                'chapter_slug': chapter['chapter_slug'],
                'chapter_number': chapter['chapter_number'],
                'chapter_volume': chapter['chapter_volume'],
                'chapter_name': chapter['chapter_name'],
                'created_at': chapter.get('created_at'),
                'chapter_url': chapter.get('chapter_url', 
                              f"http://144.31.49.103:5000/read/{manga_slug}/{chapter['chapter_slug']}")
            })
    
    # Сортируем главы по номеру
    try:
        chapters.sort(
            key=lambda x: float(x['chapter_number']) if x.get('chapter_number') and str(x['chapter_number']).replace('.', '').replace('-', '').isdigit() else 0, 
            reverse=True
        )
    except Exception as e:
        logger.error(f"❌ Ошибка сортировки глав: {e}")
    
    # Проверяем подписку
    subscribed = False
    user_id = session.get('user_id')
    if user_id and manga_data.get('manga_id'):
        subscribed = is_subscribed(user_id, manga_data['manga_id'])
    
    # Проверяем историю чтения
    reading_history = None
    if user_id and manga_data.get('manga_id'):
        conn = get_db()
        c = conn.cursor()
        c.execute('''SELECT rh.*, c.chapter_slug, c.chapter_number 
                     FROM reading_history rh 
                     JOIN chapters c ON rh.chapter_id = c.chapter_id 
                     WHERE rh.user_id = ? AND rh.manga_id = ? 
                     ORDER BY rh.last_read DESC LIMIT 1''',
                  (user_id, manga_data['manga_id']))
        history = c.fetchone()
        conn.close()
        
        if history:
            reading_history = dict(history)
    
    logger.info(f"📄 Отображаем {len(chapters)} глав для {manga_slug}")
    
    return render_template('manga_detail.html',
                         manga=manga_data,
                         chapters=chapters,
                         subscribed=subscribed,
                         reading_history=reading_history,
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
    c.execute(
        '''SELECT u.id, u.telegram_first_name, u.telegram_username,
                  s.xp, s.level, s.total_chapters_read,
                  p.avatar_url,
                  (SELECT si.css_value FROM shop_items si
                   JOIN user_items ui ON si.id = ui.item_id
                   WHERE ui.user_id = u.id AND ui.is_equipped = 1 AND si.type = 'frame'
                   LIMIT 1) as frame_css
           FROM users u
           JOIN user_stats s ON u.id = s.user_id
           LEFT JOIN user_profile p ON u.id = p.user_id
           ORDER BY s.xp DESC
           LIMIT 100''')
    leaders = [dict(row) for row in c.fetchall()]
    conn.close()

    user_id = session.get('user_id')
    return render_template('top.html', leaders=leaders, user_id=user_id)


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
    equipped = {}
    coins = 0
    if user_id:
        c.execute('SELECT item_id, is_equipped FROM user_items WHERE user_id = ?', (user_id,))
        for row in c.fetchall():
            owned_ids.add(row['item_id'])
            if row['is_equipped']:
                equipped[row['item_id']] = True
        c.execute('SELECT coins FROM user_stats WHERE user_id = ?', (user_id,))
        r = c.fetchone()
        coins = r['coins'] if r else 0

    conn.close()

    return render_template('shop.html',
                           items=items,
                           owned_ids=list(owned_ids),
                           equipped=equipped,
                           coins=coins,
                           user_id=user_id)


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
    c.execute('SELECT id FROM user_items WHERE user_id = ? AND item_id = ?', (user_id, item_id))
    if c.fetchone():
        conn.close()
        return jsonify({'error': 'Уже куплено'}), 400

    # Проверяем монеты
    c.execute('INSERT OR IGNORE INTO user_stats (user_id) VALUES (?)', (user_id,))
    c.execute('SELECT coins FROM user_stats WHERE user_id = ?', (user_id,))
    row = c.fetchone()
    coins = row['coins'] if row else 0

    if coins < item['price']:
        conn.close()
        return jsonify({'error': 'Недостаточно монет'}), 400

    # Списываем монеты и добавляем товар
    c.execute('UPDATE user_stats SET coins = coins - ? WHERE user_id = ?', (item['price'], user_id))
    c.execute('INSERT INTO user_items (user_id, item_id) VALUES (?, ?)', (user_id, item_id))
    conn.commit()

    c.execute('SELECT coins FROM user_stats WHERE user_id = ?', (user_id,))
    new_coins = c.fetchone()['coins']
    conn.close()

    return jsonify({'success': True, 'coins': new_coins})


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

    if now_equipped:
        # Снять
        c.execute('UPDATE user_items SET is_equipped = 0 WHERE user_id = ? AND item_id = ?',
                  (user_id, item_id))
        # Обновить user_profile
        col_map = {'frame': 'frame_item_id', 'badge': 'badge_item_id', 'title': 'title_item_id'}
        if item_type in col_map:
            c.execute(f'UPDATE user_profile SET {col_map[item_type]} = NULL WHERE user_id = ?',
                      (user_id,))
    else:
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
        # Обновить user_profile
        c.execute('INSERT OR IGNORE INTO user_profile (user_id) VALUES (?)', (user_id,))
        col_map = {'frame': 'frame_item_id', 'badge': 'badge_item_id', 'title': 'title_item_id'}
        if item_type in col_map:
            c.execute(f'UPDATE user_profile SET {col_map[item_type]} = ? WHERE user_id = ?',
                      (item_id, user_id))

    conn.commit()
    conn.close()
    return jsonify({'success': True, 'equipped': not now_equipped})


@app.route('/api/profile/update', methods=['POST'])
def profile_update():
    """Обновить bio профиля"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    bio = request.json.get('bio', '')[:300]
    conn = get_db()
    c = conn.cursor()
    c.execute('INSERT OR IGNORE INTO user_profile (user_id) VALUES (?)', (user_id,))
    c.execute('UPDATE user_profile SET bio = ? WHERE user_id = ?', (bio, user_id))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/upload/avatar', methods=['POST'])
def upload_avatar():
    """Загрузить аватар (требует купленный слот)"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    # Проверяем, куплен ли слот аватара
    conn = get_db()
    c = conn.cursor()
    c.execute(
        '''SELECT ui.id FROM user_items ui
           JOIN shop_items si ON ui.item_id = si.id
           WHERE ui.user_id = ? AND si.type = 'avatar_slot' ''',
        (user_id,)
    )
    if not c.fetchone():
        conn.close()
        return jsonify({'error': 'Купите слот загрузки аватара в магазине'}), 403

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
    c.execute('UPDATE user_profile SET avatar_url = ? WHERE user_id = ?', (avatar_url, user_id))
    conn.commit()
    conn.close()

    return jsonify({'success': True, 'avatar_url': avatar_url})


@app.route('/upload/background', methods=['POST'])
def upload_background():
    """Загрузить фон профиля (требует купленный слот)"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    conn = get_db()
    c = conn.cursor()
    c.execute(
        '''SELECT ui.id FROM user_items ui
           JOIN shop_items si ON ui.item_id = si.id
           WHERE ui.user_id = ? AND si.type = 'bg_slot' ''',
        (user_id,)
    )
    if not c.fetchone():
        conn.close()
        return jsonify({'error': 'Купите слот загрузки фона в магазине'}), 403

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
    stats = get_or_create_user_stats(user_id)
    if not stats:
        return jsonify({'authenticated': True, 'xp': 0, 'level': 1, 'coins': 0})
    return jsonify({
        'authenticated': True,
        'xp': stats['xp'],
        'coins': stats['coins'],
        'level': stats['level'],
        'xp_progress_pct': min(100, int(
            (stats['xp'] - get_xp_for_level(stats['level'])) /
            max(1, get_xp_for_level(stats['level'] + 1) - get_xp_for_level(stats['level'])) * 100
        ))
    })


# ==================== ЗАПУСК ====================

if __name__ == "__main__":
    init_db()
    
    # Запуск фонового процесса проверки новых глав
    checker_thread = threading.Thread(target=background_checker, daemon=True)
    checker_thread.start()
    
    # Запуск Telegram бота (теперь он сам создает поток)
    run_telegram_bot()
    
    print("🌐 Веб-сервер запущен на http://144.31.49.103:5000")
    app.run(debug=True, use_reloader=False,
            host='0.0.0.0', port=5000)