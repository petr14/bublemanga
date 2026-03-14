import os
import time
import hmac
from config import (
    TELEGRAM_BOT_TOKEN, ADMIN_TELEGRAM_IDS, SITE_URL,
    DATABASE_URL as _DATABASE_URL_CFG,
    REDIS_URL as _REDIS_URL,
    YOOKASSA_SHOP_ID, YOOKASSA_SECRET,
    CRYPTOCLOUD_API_KEY, CRYPTOCLOUD_SECRET_KEY, CRYPTOCLOUD_SHOP_ID,
    COIN_PACKAGES, PREMIUM_PACKAGES,
)
import hashlib
import urllib.parse
from datetime import datetime, timedelta
from flask import Flask, render_template, render_template_string, jsonify, request, session, redirect, url_for, Response, make_response, send_from_directory, send_file
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
from flask_caching import Cache
from senkuro_api import SenkuroAPI


# Конфигурация логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Конфигурация импортирована из config.py

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

# Flask-Caching: Redis если доступен, иначе SimpleCache (в памяти)
# _REDIS_URL импортирован из config.py как _REDIS_URL
try:
    import redis as _redis_lib
    _r = _redis_lib.from_url(_REDIS_URL, socket_connect_timeout=1)
    _r.ping()
    _CACHE_TYPE = 'RedisCache'
    _CACHE_OPTS = {'CACHE_REDIS_URL': _REDIS_URL}
    print('✅ Flask-Cache: Redis backend')
except Exception:
    _CACHE_TYPE = 'SimpleCache'
    _CACHE_OPTS = {}
    print('⚠️  Flask-Cache: SimpleCache (Redis недоступен)')

app.config['CACHE_TYPE'] = _CACHE_TYPE
app.config['CACHE_DEFAULT_TIMEOUT'] = 300
app.config.update(_CACHE_OPTS)
cache = Cache(app)

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

import bot as _bot_module
from bot import run_telegram_bot, send_telegram_notification, send_daily_digest, _revoke_premium_loans

# Клиент API Senkuro
api = SenkuroAPI()

# Словарь для отслеживания фоновой загрузки глав: manga_slug -> True
_manga_loading = {}

# ── In-memory кеш user_stats для /api/user/stats ─────────────────────────
_stats_cache: dict = {}          # {user_id: {'data': dict, 'expires': float}}
_stats_cache_lock = threading.Lock()

# ── Rate limiting ────────────────────────────────────────────────────────────
_rl_store: dict = {}
_rl_lock = threading.Lock()

def _rate_limit_check(key: str, max_calls: int, period: int) -> bool:
    """Возвращает True если запрос разрешён, False если превышен лимит."""
    now = time.time()
    with _rl_lock:
        calls = _rl_store.get(key, [])
        calls = [t for t in calls if now - t < period]
        if len(calls) >= max_calls:
            _rl_store[key] = calls
            return False
        calls.append(now)
        _rl_store[key] = calls
        return True

def rate_limit(max_calls: int, period: int = 60):
    """Декоратор ограничения запросов по IP. max_calls за period секунд."""
    def decorator(f):
        import functools
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            ip = request.headers.get('X-Forwarded-For', request.remote_addr or 'unknown').split(',')[0].strip()
            key = f"{f.__name__}:{ip}"
            if not _rate_limit_check(key, max_calls, period):
                return jsonify({'error': 'Слишком много запросов. Подождите немного.'}), 429
            return f(*args, **kwargs)
        return wrapper
    return decorator

# ==================== СЛОЙ БД (вынесен в database.py) ====================
from database import (
    _DATABASE_URL, _USE_PG, _to_dt,
    _CompatRow, _CompatCursor, _CompatConn,
    _translate_sql, _build_on_conflict, _get_pg_conn,
    get_db, init_db, init_pg_schema,
)


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
        avatar = row['avatar_url'] or None
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
THUMB_CACHE_DIR = os.path.join(os.path.dirname(__file__), 'static', 'cache', 'thumbs')
os.makedirs(THUMB_CACHE_DIR, exist_ok=True)


def _convert_to_webm_bg(abs_path):
    """Конвертирует анимированный файл в WebM в фоновом потоке."""
    import subprocess
    import tempfile
    from PIL import Image, ImageSequence

    dst = abs_path + '.webm'
    if os.path.exists(dst):
        return
    ext = abs_path.rsplit('.', 1)[-1].lower()
    tmp_gif = None
    ffmpeg_input = abs_path
    try:
        if ext == 'webp':
            img = Image.open(abs_path)
            frames, durations = [], []
            for frame in ImageSequence.Iterator(img):
                rgba = frame.convert('RGBA')
                frames.append(rgba.convert('P', palette=Image.ADAPTIVE, colors=256))
                durations.append(frame.info.get('duration', 50))
            tmp = tempfile.NamedTemporaryFile(suffix='.gif', delete=False)
            frames[0].save(tmp.name, save_all=True, append_images=frames[1:],
                           loop=0, duration=durations, optimize=False)
            tmp.close()
            tmp_gif = tmp.name
            ffmpeg_input = tmp_gif
        cmd = ['ffmpeg', '-y', '-i', ffmpeg_input,
               '-c:v', 'libvpx-vp9', '-b:v', '0', '-crf', '35',
               '-cpu-used', '5', '-deadline', 'realtime',
               '-auto-alt-ref', '0', '-an', dst]
        subprocess.run(cmd, capture_output=True, timeout=300)
    except Exception as e:
        logger.error(f'WebM bg convert error {abs_path}: {e}')
    finally:
        if tmp_gif:
            try: os.unlink(tmp_gif)
            except OSError: pass


def schedule_webm_conversion(abs_path):
    """Запускает конвертацию в WebM в daemon-потоке."""
    ext = abs_path.rsplit('.', 1)[-1].lower() if '.' in abs_path else ''
    if ext in ('gif', 'webp'):
        t = threading.Thread(target=_convert_to_webm_bg, args=(abs_path,), daemon=True)
        t.start()


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

    # Инвалидируем кеш таблицы лидеров
    cache.delete('top_leaders')

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
                                     ach['description'],
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

    # Находим надетую рамку для рендера на профиле
    equipped_frame = next((i for i in items if i.get('type') == 'frame' and i.get('is_equipped')), None)
    frame_preview_url = equipped_frame.get('preview_url') if equipped_frame else None
    frame_is_animated = bool(equipped_frame.get('is_animated')) if equipped_frame else False
    frame_css_value   = equipped_frame.get('css_value') if equipped_frame else None

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
        'frame_preview_url': frame_preview_url,
        'frame_is_animated': frame_is_animated,
        'frame_css_value': frame_css_value,
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
    conn = get_db()
    # дедупликация — не отправлять повторно для той же манги
    already = conn.execute(
        'SELECT 1 FROM sent_similar_notifications WHERE user_id=? AND manga_id=?',
        (user_id, manga_id)
    ).fetchone()
    if already:
        conn.close()
        return

    row = conn.execute('SELECT telegram_id, notifications_enabled FROM users WHERE id=?', (user_id,)).fetchone()
    if not row or not row['telegram_id'] or row['notifications_enabled'] == 0:
        conn.close()
        return

    similar = get_similar_manga(manga_id, user_id, limit=3)
    if not similar:
        conn.close()
        return

    # записываем факт отправки ДО отправки, чтобы гонка потоков не дала дубль
    try:
        conn.execute(
            'INSERT OR IGNORE INTO sent_similar_notifications (user_id, manga_id) VALUES (?,?)',
            (user_id, manga_id)
        )
        conn.commit()
    finally:
        conn.close()

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
            _upd = cache_row['updated_at']
            cache_time = _upd if isinstance(_upd, datetime) else datetime.fromisoformat(str(_upd))
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
        # Обновляем FTS индекс
        try:
            manga_id = manga_data['manga_id']
            title = manga_data['manga_title']
            original_name = manga_data.get('original_name')
            description = manga_data.get('description')
            if not _USE_PG:
                c.execute('DELETE FROM manga_fts WHERE manga_id = ?', (manga_id,))
                c.execute('''INSERT INTO manga_fts(manga_id, manga_title, original_name, description)
                             VALUES (?, ?, ?, ?)''',
                          (manga_id, title, original_name or '', description or ''))
            conn.commit()
        except Exception:
            pass
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
    c.execute('''SELECT * FROM users WHERE login_token = ? AND is_active IS NOT FALSE''', (token,))
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

    BATCH_SIZE = 50  # коммитить каждые N глав чтобы не держать write-lock долго

    conn = get_db()
    c = conn.cursor()

    try:
        saved_count = 0
        updated_count = 0
        errors = 0

        for i, chapter in enumerate(chapters):
            try:
                c.execute('SELECT chapter_id, chapter_number FROM chapters WHERE chapter_id = ?',
                         (chapter['chapter_id'],))
                existing = c.fetchone()

                if not existing:
                    chapter_url = f"/read/{chapter['manga_slug']}/{chapter['chapter_slug']}"
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
                    if existing['chapter_number'] != chapter['chapter_number']:
                        c.execute('UPDATE chapters SET chapter_number = ? WHERE chapter_id = ?',
                                 (chapter['chapter_number'], chapter['chapter_id']))
                        updated_count += 1

            except Exception as e:
                errors += 1
                logger.error(f"❌ Ошибка сохранения главы {chapter.get('chapter_id', 'unknown')}: {e}")

            # Промежуточный коммит — освобождаем write-lock
            if (i + 1) % BATCH_SIZE == 0:
                conn.commit()

        conn.commit()
        logger.info(f"✅ Сохранено {saved_count} новых глав, обновлено {updated_count}, ошибок: {errors}")

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
            _upd = row['updated_at']
            _upd_dt = _upd if isinstance(_upd, datetime) else datetime.fromisoformat(str(_upd))
            age = (datetime.now() - _upd_dt).total_seconds()
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
    c.execute('''SELECT m.*, rh_agg.last_read_time
                 FROM manga m
                 JOIN (
                     SELECT manga_id, MAX(last_read) as last_read_time
                     FROM reading_history
                     WHERE user_id = ?
                     GROUP BY manga_id
                 ) rh_agg ON m.manga_id = rh_agg.manga_id
                 ORDER BY rh_agg.last_read_time DESC
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
        # Автоматически ставим статус "Читаю" если статус не установлен
        c.execute(
            'INSERT INTO user_manga_status (user_id, manga_id, status) VALUES (?,?,?) '
            'ON CONFLICT(user_id, manga_id) DO NOTHING',
            (user_id, manga_id, 'reading')
        )
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

_BOT_UA_KEYWORDS = (
    'bot', 'spider', 'crawler', 'crawl', 'scraper', 'fetch',
    'python-requests', 'python-urllib', 'curl/', 'wget/',
    'scrapy', 'headlesschrome', 'phantomjs', 'selenium',
    'facebookexternalhit', 'twitterbot', 'slackbot', 'telegrambot',
    'vkshare', 'whatsapp', 'pinterest', 'linkedinbot',
)

def _is_bot_request() -> bool:
    ua = (request.headers.get('User-Agent') or '').lower()
    return any(kw in ua for kw in _BOT_UA_KEYWORDS)

def increment_manga_views(manga_id):
    if _is_bot_request():
        return
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
           WHERE s.manga_id = ? AND u.is_active IS NOT FALSE''',
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

# ==================== ЗАПУСК ====================


# ==================== BLUEPRINT REGISTRATION ====================
from routes import bp
app.register_blueprint(bp)

if __name__ == "__main__":
    if not _USE_PG:
        init_db()
    else:
        print("ℹ️  PostgreSQL mode: init_db() пропускается")
        init_pg_schema()
    
    # Запуск фонового процесса проверки новых глав
    checker_thread = threading.Thread(target=background_checker, daemon=True)
    checker_thread.start()
    
    # Запуск Telegram бота (теперь он сам создает поток)
    run_telegram_bot()
    
    print(f"🌐 Веб-сервер запущен на {SITE_URL}")
    socketio.run(app, debug=True, use_reloader=False,
                 host='0.0.0.0', port=5000, allow_unsafe_werkzeug=True)