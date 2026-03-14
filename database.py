"""
database.py — слой доступа к данным для BubbleManga.

Содержит:
  - SQLite / PostgreSQL совместимый слой (_CompatConn / _CompatCursor)
  - init_db()       — создание схемы SQLite
  - init_pg_schema() — PostgreSQL-специфичные триггеры / таблицы
  - get_db()        — возвращает соединение нужного типа
"""

import os
import re
import sqlite3
import logging
from datetime import datetime
from config import DATABASE_URL as _DATABASE_URL

logger = logging.getLogger(__name__)

# ==================== СОВМЕСТИМЫЙ СЛОЙ БД (SQLite / PostgreSQL) ====================

_USE_PG = bool(_DATABASE_URL)


def _to_dt(val):
    """Приводит datetime-объект или ISO-строку к naive UTC datetime."""
    if val is None:
        return None
    if isinstance(val, str):
        val = datetime.fromisoformat(val.replace('Z', '+00:00'))
    if hasattr(val, 'tzinfo') and val.tzinfo is not None:
        val = val.replace(tzinfo=None)
    return val


if _USE_PG:
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        logger.warning("psycopg2 не установлен, переключаемся на SQLite")
        _USE_PG = False


# Паттерны для транслирования SQLite → PostgreSQL
_RE_INSERT_OR_IGNORE  = re.compile(r'\bINSERT\s+OR\s+IGNORE\s+INTO\b',  re.IGNORECASE)
_RE_INSERT_OR_REPLACE = re.compile(r'\bINSERT\s+OR\s+REPLACE\s+INTO\b', re.IGNORECASE)
_RE_TABLE_NAME        = re.compile(r'\bINTO\s+(\w+)\s*\(([^)]+)\)',      re.IGNORECASE)
_RE_DATETIME_NOW      = re.compile(r"datetime\('now'(?:,\s*'([^']+)')?\)", re.IGNORECASE)
_RE_DATE_NOW          = re.compile(r"date\('now'(?:,\s*'([^']+)')?\)",    re.IGNORECASE)
_RE_STRFTIME          = re.compile(r"strftime\('([^']+)',\s*'now'\)",      re.IGNORECASE)

# Таблица → (столбец конфликта, список столбцов для UPDATE)
# None в update_cols → обновлять все вставляемые столбцы кроме конфликтного
_PG_UPSERT_MAP: dict = {
    'cache':           ('key',               None),
    'manga':           ('manga_id',          None),
    'chapters':        ('chapter_id',        None),
    'reading_history': ('user_id, manga_id', ['chapter_id', 'page_number', 'last_read']),
}


def _translate_sql(sql: str) -> str:
    """Конвертирует SQLite-специфичный SQL в PostgreSQL-совместимый."""
    sql = re.sub(r"'(?:[^']|'')*'|(\?)", lambda m: m.group(0) if m.group(1) is None else '%s', sql)
    sql = _RE_INSERT_OR_IGNORE.sub('INSERT INTO', sql)
    sql = _RE_INSERT_OR_REPLACE.sub('INSERT INTO', sql)

    def _dt_repl(m):
        modifier = m.group(1)
        if modifier:
            mod  = modifier.strip().lstrip('+-')
            sign = '-' if '-' in modifier else '+'
            return f"NOW() {sign} INTERVAL '{mod}'"
        return 'NOW()'
    sql = _RE_DATETIME_NOW.sub(_dt_repl, sql)

    def _date_repl(m):
        modifier = m.group(1)
        if modifier:
            mod  = modifier.strip().lstrip('+-')
            sign = '-' if '-' in modifier else '+'
            return f"CURRENT_DATE {sign} INTERVAL '{mod}'"
        return 'CURRENT_DATE'
    sql = _RE_DATE_NOW.sub(_date_repl, sql)

    _STRFTIME_MAP = {
        '%Y-%m-%d': 'YYYY-MM-DD', '%Y': 'YYYY', '%m': 'MM',
        '%d': 'DD',  '%H': 'HH24', '%M': 'MI',  '%S': 'SS',
    }
    def _sf_repl(m):
        return f"TO_CHAR(NOW(), '{_STRFTIME_MAP.get(m.group(1), m.group(1))}')"
    sql = _RE_STRFTIME.sub(_sf_repl, sql)
    return sql


def _build_on_conflict(sql_orig: str, is_replace: bool) -> str:
    """Добавляет ON CONFLICT ... к INSERT INTO.
    Для INSERT OR IGNORE  → DO NOTHING.
    Для INSERT OR REPLACE → DO UPDATE SET (если таблица есть в _PG_UPSERT_MAP).
    """
    m = _RE_TABLE_NAME.search(sql_orig)
    if not m:
        return ' ON CONFLICT DO NOTHING'
    table = m.group(1).lower()
    cols  = [c.strip() for c in m.group(2).split(',')]

    if not is_replace or table not in _PG_UPSERT_MAP:
        return ' ON CONFLICT DO NOTHING'

    conflict_col, update_cols = _PG_UPSERT_MAP[table]
    if update_cols is None:
        conflict_set = {c.strip() for c in conflict_col.split(',')}
        update_cols  = [c for c in cols if c not in conflict_set]
    sets = ', '.join(f'{c}=EXCLUDED.{c}' for c in update_cols)
    return f' ON CONFLICT ({conflict_col}) DO UPDATE SET {sets}'


class _CompatRow:
    """Строка результата с доступом по индексу и по имени (как sqlite3.Row)."""
    __slots__ = ('_data', '_keys')

    def __init__(self, raw):
        self._data = dict(raw)
        self._keys = list(self._data)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._data[self._keys[key]]
        return self._data[key]

    def __iter__(self):
        return iter(self._data.values())

    def __bool__(self):
        return True

    def __contains__(self, key):
        return key in self._data

    def keys(self):
        return self._keys

    def get(self, key, default=None):
        return self._data.get(key, default)


class _CompatCursor:
    """Курсор-обёртка над psycopg2, совместимый с интерфейсом sqlite3."""

    def __init__(self, pg_cursor):
        self._c = pg_cursor

    def _pre(self, sql: str, params=None):
        is_ignore  = bool(_RE_INSERT_OR_IGNORE.search(sql))
        is_replace = bool(_RE_INSERT_OR_REPLACE.search(sql))
        translated = _translate_sql(sql)
        if (is_ignore or is_replace) and 'ON CONFLICT' not in translated.upper():
            translated = translated.rstrip('; \n') + _build_on_conflict(sql, is_replace)
        return translated, params or ()

    def execute(self, sql, params=None):
        sql2, params2 = self._pre(sql, params)
        self._c.execute(sql2, params2)
        return self

    def executemany(self, sql, seq):
        sql2, _ = self._pre(sql)
        self._c.executemany(sql2, seq)
        return self

    def fetchone(self):
        row = self._c.fetchone()
        return _CompatRow(row) if row is not None else None

    def fetchall(self):
        return [_CompatRow(r) for r in self._c.fetchall()]

    @property
    def lastrowid(self):
        self._c.execute('SELECT lastval()')
        row = self._c.fetchone()
        return list(row.values())[0]

    @property
    def rowcount(self):
        return self._c.rowcount

    def __iter__(self):
        return iter(self._c)


class _CompatConn:
    """Соединение-обёртка: делает psycopg2 похожим на sqlite3."""

    def __init__(self, pg_conn):
        self._conn = pg_conn

    def cursor(self):
        return _CompatCursor(self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor))

    def execute(self, sql, params=None):
        c = self.cursor()
        c.execute(sql, params)
        return c

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._conn.__exit__(*args)


def _get_pg_conn() -> '_CompatConn':
    pg_conn = psycopg2.connect(_DATABASE_URL)
    pg_conn.autocommit = False
    return _CompatConn(pg_conn)


# ==================== get_db ====================

def get_db():
    if _USE_PG:
        return _get_pg_conn()
    conn = sqlite3.connect('manga.db', timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA busy_timeout=30000')
    conn.execute('PRAGMA cache_size=-32000')
    conn.execute('PRAGMA wal_autocheckpoint=1000')
    return conn


# ==================== БАЗА ДАННЫХ: init_db ====================

def init_db():
    """Инициализация базы данных (SQLite-схема + миграции)."""
    conn = sqlite3.connect('manga.db', timeout=30, check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA busy_timeout=30000')
    conn.execute('PRAGMA cache_size=-32000')
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
            pass

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

    c.execute('''CREATE TABLE IF NOT EXISTS cache (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    # ── Геймификация ────────────────────────────────────────────────────────

    c.execute('''CREATE TABLE IF NOT EXISTS user_stats (
        user_id INTEGER PRIMARY KEY,
        xp INTEGER DEFAULT 0,
        coins INTEGER DEFAULT 0,
        level INTEGER DEFAULT 1,
        total_chapters_read INTEGER DEFAULT 0,
        total_pages_read INTEGER DEFAULT 0,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )''')

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

    c.execute('''CREATE TABLE IF NOT EXISTS user_achievements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        achievement_id INTEGER NOT NULL,
        unlocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (achievement_id) REFERENCES achievements(id),
        UNIQUE(user_id, achievement_id)
    )''')

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

    c.execute('''CREATE TABLE IF NOT EXISTS xp_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        reason TEXT NOT NULL,
        ref_id TEXT,
        amount INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

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

    try:
        c.execute('ALTER TABLE users ADD COLUMN last_digest_date TEXT')
        conn.commit()
    except Exception:
        pass

    c.execute('''CREATE TABLE IF NOT EXISTS chapters_read (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        chapter_id TEXT NOT NULL,
        manga_id TEXT NOT NULL,
        read_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id),
        UNIQUE(user_id, chapter_id)
    )''')

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

    c.execute('''CREATE TABLE IF NOT EXISTS coin_purchases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL REFERENCES users(id),
        package_id TEXT NOT NULL,
        stars_paid INTEGER NOT NULL,
        coins_received INTEGER NOT NULL,
        payment_id TEXT UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS premium_purchases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL REFERENCES users(id),
        package_id TEXT NOT NULL,
        payment_id TEXT UNIQUE NOT NULL,
        payment_method TEXT DEFAULT 'yookassa',
        expires_at TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

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

    # Дедупликация квестов
    c.execute('''DELETE FROM user_quests
                 WHERE quest_id NOT IN (SELECT MIN(id) FROM quests GROUP BY title)''')
    c.execute('''DELETE FROM quests
                 WHERE id NOT IN (SELECT MIN(id) FROM quests GROUP BY title)''')
    conn.commit()

    try:
        c.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_quests_title ON quests(title)')
        conn.commit()
    except Exception:
        pass

    QUESTS_SEED = [
        ('Первый комментарий',  'Напиши свой первый комментарий к манге',  '💬', 1,  'comments_posted', 1,   50,  20),
        ('Начало пути',         'Прочитай 5 глав любой манги',             '📖', 1,  'chapters_read',   5,   75,  30),
        ('Подписчик',           'Подпишись на 1 мангу',                    '❤️', 2,  'subscriptions',   1,   100, 50),
        ('Активный читатель',   'Прочитай 20 глав',                        '📚', 2,  'chapters_read',   20,  150, 60),
        ('Болтун',              'Напиши 5 комментариев',                   '🗣️', 3,  'comments_posted', 5,   200, 80),
        ('Библиотека',          'Подпишись на 3 манги',                    '🗂️', 3,  'subscriptions',   3,   200, 80),
        ('Профи',               'Прочитай 100 глав',                       '🔥', 5,  'chapters_read',   100, 500, 200),
        ('Завсегдатай',         'Подпишись на 10 манг',                    '💎', 5,  'subscriptions',   10,  400, 150),
        ('Ветеран',             'Прочитай 500 глав',                       '🏆', 10, 'chapters_read',   500, 1000, 500),
        ('Великий комментатор', 'Напиши 50 комментариев',                  '👑', 10, 'comments_posted', 50,  800, 300),
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

    c.execute('''CREATE TABLE IF NOT EXISTS comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        manga_slug TEXT NOT NULL,
        user_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_comments_manga ON comments(manga_slug, created_at DESC)')

    ACHIEVEMENTS = [
        ('first_chapter', 'Первый шаг',       'Прочитать первую главу',         '📖', 50,   'chapters_read', 1),
        ('reader_10',     'Читатель',          'Прочитать 10 глав',              '📚', 100,  'chapters_read', 10),
        ('reader_50',     'Книголюб',          'Прочитать 50 глав',              '🔖', 200,  'chapters_read', 50),
        ('reader_100',    'Книгочей',          'Прочитать 100 глав',             '🎓', 500,  'chapters_read', 100),
        ('reader_500',    'Запойный читатель', 'Прочитать 500 глав',             '🌟', 1000, 'chapters_read', 500),
        ('reader_1000',   'Маньяк чтения',     'Прочитать 1000 глав',            '👑', 2000, 'chapters_read', 1000),
        ('subscriber_1',  'Фанат',             'Подписаться на 1 мангу',         '❤️', 50,   'subscriptions', 1),
        ('subscriber_5',  'Следопыт',          'Подписаться на 5 манг',          '💫', 150,  'subscriptions', 5),
        ('subscriber_10', 'Коллекционер',      'Подписаться на 10 манг',         '💎', 300,  'subscriptions', 10),
        ('level_5',       'Опытный',           'Достичь 5 уровня',               '⚡', 0,    'level',         5),
        ('level_10',      'Бывалый',           'Достичь 10 уровня',              '🔥', 0,    'level',         10),
        ('level_20',      'Ветеран',           'Достичь 20 уровня',              '🏆', 0,    'level',         20),
        ('level_50',      'Легенда',           'Достичь 50 уровня',              '🌈', 0,    'level',         50),
    ]
    c.executemany(
        '''INSERT OR IGNORE INTO achievements
           (key, name, description, icon, xp_reward, condition_type, condition_value)
           VALUES (?, ?, ?, ?, ?, ?, ?)''',
        ACHIEVEMENTS
    )

    c.execute('''INSERT OR IGNORE INTO shop_items (id, name, description, type, preview_url, css_value, price, is_upload)
                 VALUES (12, 'Загрузка аватара', 'Разблокировать загрузку своего аватара', 'avatar_slot', NULL, NULL, 0, 1)''')
    c.execute('''INSERT OR IGNORE INTO shop_items (id, name, description, type, preview_url, css_value, price, is_upload)
                 VALUES (13, 'Загрузка фона', 'Разблокировать загрузку своего фона', 'bg_slot', NULL, NULL, 1500, 1)''')

    _DAILY_UPDATES = [('Читатель дня', 12), ('Комментатор', 6), ('Исследователь', 6)]
    for _title, _coins in _DAILY_UPDATES:
        try:
            c.execute('UPDATE daily_quests SET coins_reward = ? WHERE title = ?', (_coins, _title))
        except Exception:
            pass

    # ── Миграция: Premium поля ─────────────────────────────────────────────
    for _sql in [
        'ALTER TABLE users ADD COLUMN is_premium INTEGER DEFAULT 0',
        'ALTER TABLE users ADD COLUMN premium_granted_at TIMESTAMP',
        'ALTER TABLE users ADD COLUMN premium_expires_at TIMESTAMP',
        'ALTER TABLE user_items ADD COLUMN is_premium_loan INTEGER DEFAULT 0',
        'ALTER TABLE comments ADD COLUMN parent_id INTEGER REFERENCES comments(id)',
    ]:
        try:
            c.execute(_sql)
            conn.commit()
        except Exception:
            pass

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

    try:
        c.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code)')
        conn.commit()
    except Exception:
        pass

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

    c.execute('''CREATE TABLE IF NOT EXISTS user_season_quests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        season_quest_id INTEGER NOT NULL,
        progress INTEGER DEFAULT 0,
        completed_at TEXT DEFAULT NULL,
        UNIQUE(user_id, season_quest_id),
        FOREIGN KEY (user_id) REFERENCES users(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS comment_likes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        comment_id INTEGER NOT NULL,
        UNIQUE(user_id, comment_id),
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (comment_id) REFERENCES comments(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS reading_wishlist (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        manga_id TEXT NOT NULL,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, manga_id),
        FOREIGN KEY (user_id) REFERENCES users(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS referrals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referrer_id INTEGER NOT NULL,
        referred_id INTEGER NOT NULL UNIQUE,
        rewarded INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (referrer_id) REFERENCES users(id),
        FOREIGN KEY (referred_id) REFERENCES users(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS collection_trophies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        collection_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        iso_week TEXT NOT NULL,
        likes_count INTEGER NOT NULL,
        awarded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(iso_week)
    )''')

    c.execute('CREATE INDEX IF NOT EXISTS idx_user_daily_quests ON user_daily_quests(user_id, date)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_user_season_quests ON user_season_quests(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_comment_likes_comment ON comment_likes(comment_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_wishlist_user ON reading_wishlist(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)')

    c.execute('''CREATE TABLE IF NOT EXISTS user_manga_status (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        manga_id TEXT NOT NULL,
        status TEXT NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, manga_id),
        FOREIGN KEY (user_id) REFERENCES users(id)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_user_manga_status ON user_manga_status(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_collection_trophies_week ON collection_trophies(iso_week)')

    DAILY_QUESTS_SEED = [
        ('Читатель дня',  'Прочитай 3 главы сегодня',        '📖', 'chapters_today', 3, 50, 12),
        ('Комментатор',   'Оставь 1 комментарий сегодня',    '💬', 'comments_today', 1, 30,  6),
        ('Исследователь', 'Открой 2 разные манги сегодня',   '🔍', 'manga_today',    2, 30,  6),
    ]
    c.executemany(
        '''INSERT OR IGNORE INTO daily_quests
           (title, description, icon, condition_type, condition_value, xp_reward, coins_reward)
           VALUES (?, ?, ?, ?, ?, ?, ?)''',
        DAILY_QUESTS_SEED
    )

    c.execute('''INSERT OR IGNORE INTO seasons (id, name, description, icon, starts_at, ends_at, is_active)
                 VALUES (1, 'Весна 2026', 'Сезон цветения — читай, комментируй и побеждай!',
                         '🌸', '2026-03-01', '2026-04-30', 1)''')

    c.execute('''INSERT OR IGNORE INTO shop_items (id, name, description, type, preview_url, css_value, price, is_upload)
                 VALUES (100, 'Рамка «Весна»', 'Сезонная рамка Spring 2026',
                         'frame', NULL,
                         'border: 3px solid #ec4899; box-shadow: 0 0 14px #f9a8d4;',
                         0, 0)''')

    SEASON_QUESTS_SEED = [
        (1, 'Весенний читатель',  'Прочитай 50 глав за сезон',  '📚', 'chapters_read',   50,  200, 100, None),
        (1, 'Болтун сезона',      'Оставь 10 комментариев',     '💬', 'comments_posted', 10,  150,  50, None),
        (1, 'Коллекционер весны', 'Прочитай 100 глав за сезон', '🌸', 'chapters_read',   100, 500, 200, 100),
    ]
    for sq in SEASON_QUESTS_SEED:
        c.execute('''INSERT OR IGNORE INTO season_quests
                     (season_id, title, description, icon, condition_type, condition_value,
                      xp_reward, coins_reward, item_reward_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', sq)

    c.execute('''CREATE TABLE IF NOT EXISTS sent_similar_notifications (
        user_id INTEGER NOT NULL,
        manga_id INTEGER NOT NULL,
        sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (user_id, manga_id)
    )''')

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

    c.execute('''CREATE TABLE IF NOT EXISTS admin_broadcasts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        admin_id INTEGER,
        title TEXT NOT NULL,
        body TEXT,
        url TEXT,
        notif_type TEXT DEFAULT 'admin',
        filter_desc TEXT,
        recipients_count INTEGER DEFAULT 0,
        send_telegram INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    try:
        c.execute('ALTER TABLE users ADD COLUMN digest_hour INTEGER DEFAULT 22')
        conn.commit()
    except Exception:
        pass

    for _sql in [
        'ALTER TABLE shop_items ADD COLUMN duration_days INTEGER DEFAULT NULL',
        'ALTER TABLE user_items ADD COLUMN expires_at TIMESTAMP DEFAULT NULL',
    ]:
        try:
            c.execute(_sql)
            conn.commit()
        except Exception:
            pass

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

    c.execute('''CREATE TABLE IF NOT EXISTS manga_user_ratings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        manga_id TEXT NOT NULL,
        score INTEGER NOT NULL CHECK(score BETWEEN 1 AND 10),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, manga_id),
        FOREIGN KEY (user_id) REFERENCES users(id)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_manga_user_ratings_manga ON manga_user_ratings(manga_id)')

    c.execute('''CREATE VIRTUAL TABLE IF NOT EXISTS manga_fts
                 USING fts5(manga_id UNINDEXED, manga_title, original_name, description,
                            tokenize="unicode61 remove_diacritics 1")''')
    row = conn.execute('SELECT COUNT(*) FROM manga_fts').fetchone()
    if row[0] == 0:
        c.execute('''INSERT INTO manga_fts(manga_id, manga_title, original_name, description)
                     SELECT manga_id, manga_title, COALESCE(original_name,''), COALESCE(description,'')
                     FROM manga''')

    conn.commit()
    conn.close()
    print("✅ База данных инициализирована")


# ==================== БАЗА ДАННЫХ: init_pg_schema ====================

def init_pg_schema():
    """Проверяет/создаёт триггер обновления search_vector в PostgreSQL."""
    conn = get_db()
    try:
        conn.execute(
            "CREATE OR REPLACE FUNCTION manga_search_vector_update() "
            "RETURNS trigger AS $func$ "
            "BEGIN "
            "  NEW.search_vector := to_tsvector('simple', "
            "    COALESCE(NEW.manga_title,'') || ' ' || "
            "    COALESCE(NEW.original_name,'') || ' ' || "
            "    COALESCE(NEW.description,'')); "
            "  RETURN NEW; "
            "END; "
            "$func$ LANGUAGE plpgsql"
        )
        conn.execute("DROP TRIGGER IF EXISTS manga_search_vector_trig ON manga")
        conn.execute(
            "CREATE TRIGGER manga_search_vector_trig "
            "BEFORE INSERT OR UPDATE ON manga "
            "FOR EACH ROW EXECUTE FUNCTION manga_search_vector_update()"
        )
        conn.commit()
        print("✅ PostgreSQL: триггер search_vector установлен")
    except Exception as e:
        logger.warning(f"init_pg_schema: {e}")

    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS manga_user_ratings (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            manga_id TEXT NOT NULL,
            score INTEGER NOT NULL CHECK(score BETWEEN 1 AND 10),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(user_id, manga_id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_manga_user_ratings_manga ON manga_user_ratings(manga_id)')
        conn.commit()
        print("✅ PostgreSQL: таблица manga_user_ratings готова")
    except Exception as e:
        logger.warning(f"init_pg_schema manga_user_ratings: {e}")
    finally:
        conn.close()
