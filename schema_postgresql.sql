-- PostgreSQL schema for BubbleManga
-- Эквивалент init_db() из main.py
-- Применяется один раз перед первым запуском или после migrate_to_pg.py

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ── Манга ───────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS manga (
    id              SERIAL PRIMARY KEY,
    manga_id        TEXT UNIQUE NOT NULL,
    manga_slug      TEXT NOT NULL,
    manga_title     TEXT NOT NULL,
    manga_type      TEXT,
    manga_status    TEXT,
    cover_url       TEXT,
    last_chapter_id     TEXT,
    last_chapter_number TEXT,
    last_chapter_volume TEXT,
    last_chapter_name   TEXT,
    last_chapter_slug   TEXT,
    last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    views           INTEGER DEFAULT 0,
    rating          TEXT DEFAULT 'GENERAL',
    branch_id       TEXT,
    chapters_count  INTEGER DEFAULT 0,
    description     TEXT DEFAULT '',
    score           REAL DEFAULT 0,
    tags            TEXT DEFAULT '[]',
    original_name   TEXT DEFAULT '',
    translation_status TEXT DEFAULT '',
    is_licensed     INTEGER DEFAULT 0,
    formats         TEXT DEFAULT '[]',
    -- PostgreSQL full-text search vector (обновляется триггером)
    search_vector   tsvector
);

CREATE INDEX IF NOT EXISTS idx_manga_slug     ON manga(manga_slug);
CREATE INDEX IF NOT EXISTS idx_manga_fts_gin  ON manga USING GIN(search_vector);
CREATE INDEX IF NOT EXISTS idx_manga_trgm     ON manga USING GIN(manga_title gin_trgm_ops);

-- Триггер для автоматического обновления search_vector
CREATE OR REPLACE FUNCTION manga_search_vector_update() RETURNS trigger AS $$
BEGIN
    NEW.search_vector := to_tsvector('simple',
        COALESCE(NEW.manga_title, '') || ' ' ||
        COALESCE(NEW.original_name, '') || ' ' ||
        COALESCE(NEW.description, ''));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS manga_search_vector_trig ON manga;
CREATE TRIGGER manga_search_vector_trig
    BEFORE INSERT OR UPDATE ON manga
    FOR EACH ROW EXECUTE FUNCTION manga_search_vector_update();

-- ── Главы ───────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS chapters (
    id             SERIAL PRIMARY KEY,
    manga_id       TEXT NOT NULL,
    chapter_id     TEXT UNIQUE NOT NULL,
    chapter_slug   TEXT NOT NULL,
    chapter_number TEXT,
    chapter_volume TEXT,
    chapter_name   TEXT,
    chapter_url    TEXT,
    pages_json     TEXT,
    pages_count    INTEGER DEFAULT 0,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (manga_id) REFERENCES manga(manga_id)
);

-- ── Пользователи ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS users (
    id                    SERIAL PRIMARY KEY,
    telegram_id           BIGINT UNIQUE NOT NULL,
    telegram_username     TEXT,
    telegram_first_name   TEXT,
    telegram_last_name    TEXT,
    login_token           TEXT UNIQUE,
    is_active             INTEGER DEFAULT 1,
    notifications_enabled INTEGER DEFAULT 1,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_digest_date      TEXT,
    is_premium            INTEGER DEFAULT 0,
    premium_expires_at    TIMESTAMP,
    premium_granted_at    TIMESTAMP,
    digest_hour           INTEGER DEFAULT 22,
    referral_code         TEXT UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_users_telegram_id   ON users(telegram_id);
CREATE INDEX IF NOT EXISTS idx_users_login_token   ON users(login_token);
CREATE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code);

-- ── Подписки ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS subscriptions (
    id           SERIAL PRIMARY KEY,
    user_id      INTEGER NOT NULL REFERENCES users(id),
    manga_id     TEXT    NOT NULL REFERENCES manga(manga_id),
    subscribed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, manga_id)
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_user  ON subscriptions(user_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_manga ON subscriptions(manga_id);

-- ── История чтения ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS reading_history (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id),
    manga_id    TEXT    NOT NULL REFERENCES manga(manga_id),
    chapter_id  TEXT    NOT NULL REFERENCES chapters(chapter_id),
    page_number INTEGER DEFAULT 1,
    last_read   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, manga_id)
);

-- ── Прочитанные главы (одна запись на главу) ────────────────────────────────

CREATE TABLE IF NOT EXISTS chapters_read (
    id         SERIAL PRIMARY KEY,
    user_id    INTEGER NOT NULL REFERENCES users(id),
    chapter_id TEXT    NOT NULL,
    manga_id   TEXT    NOT NULL,
    read_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, chapter_id)
);

CREATE INDEX IF NOT EXISTS idx_chapters_read_user_manga ON chapters_read(user_id, manga_id);

-- ── История поиска ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS search_history (
    id         SERIAL PRIMARY KEY,
    user_id    INTEGER REFERENCES users(id),
    query      TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_search_user ON search_history(user_id);

-- ── Кеш ─────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cache (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── Статистика пользователя ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS user_stats (
    user_id              INTEGER PRIMARY KEY REFERENCES users(id),
    xp                   INTEGER DEFAULT 0,
    coins                INTEGER DEFAULT 0,
    level                INTEGER DEFAULT 1,
    total_chapters_read  INTEGER DEFAULT 0,
    total_pages_read     INTEGER DEFAULT 0,
    reading_streak       INTEGER DEFAULT 0,
    max_streak           INTEGER DEFAULT 0,
    last_read_date       TEXT
);

CREATE INDEX IF NOT EXISTS idx_user_stats ON user_stats(xp DESC);

-- ── Лог XP ───────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS xp_log (
    id         SERIAL PRIMARY KEY,
    user_id    INTEGER NOT NULL,
    reason     TEXT    NOT NULL,
    ref_id     TEXT,
    amount     INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_xp_log ON xp_log(user_id, ref_id);

-- ── Достижения ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS achievements (
    id              SERIAL PRIMARY KEY,
    key             TEXT UNIQUE NOT NULL,
    name            TEXT NOT NULL,
    description     TEXT,
    icon            TEXT DEFAULT '🏆',
    xp_reward       INTEGER DEFAULT 0,
    condition_type  TEXT NOT NULL,
    condition_value INTEGER NOT NULL,
    icon_url        TEXT
);

CREATE TABLE IF NOT EXISTS user_achievements (
    id             SERIAL PRIMARY KEY,
    user_id        INTEGER NOT NULL REFERENCES users(id),
    achievement_id INTEGER NOT NULL REFERENCES achievements(id),
    unlocked_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, achievement_id)
);

-- ── Магазин ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS shop_items (
    id            SERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    description   TEXT,
    type          TEXT NOT NULL,
    preview_url   TEXT,
    css_value     TEXT,
    price         INTEGER DEFAULT 0,
    is_upload     INTEGER DEFAULT 0,
    is_animated   INTEGER DEFAULT 0,
    duration_days INTEGER
);

CREATE TABLE IF NOT EXISTS user_items (
    id            SERIAL PRIMARY KEY,
    user_id       INTEGER NOT NULL REFERENCES users(id),
    item_id       INTEGER NOT NULL REFERENCES shop_items(id),
    purchased_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_equipped   INTEGER DEFAULT 0,
    is_premium_loan INTEGER DEFAULT 0,
    expires_at    TIMESTAMP,
    UNIQUE(user_id, item_id)
);

-- ── Профиль ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS user_profile (
    user_id           INTEGER PRIMARY KEY REFERENCES users(id),
    avatar_url        TEXT,
    background_url    TEXT,
    frame_item_id     INTEGER,
    badge_item_id     INTEGER,
    title_item_id     INTEGER,
    bio               TEXT DEFAULT '',
    custom_name       TEXT DEFAULT '',
    custom_avatar_url TEXT,
    name_change_count INTEGER DEFAULT 0,
    name_change_month TEXT
);

-- ── Коллекции ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS collections (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id),
    name        TEXT NOT NULL,
    description TEXT DEFAULT '',
    cover_url   TEXT,
    is_public   INTEGER DEFAULT 1,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_collections_user ON collections(user_id);

CREATE TABLE IF NOT EXISTS collection_items (
    id            SERIAL PRIMARY KEY,
    collection_id INTEGER NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    manga_id      TEXT    NOT NULL,
    added_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(collection_id, manga_id)
);

CREATE INDEX IF NOT EXISTS idx_collection_items ON collection_items(collection_id);

CREATE TABLE IF NOT EXISTS collection_likes (
    id            SERIAL PRIMARY KEY,
    user_id       INTEGER NOT NULL,
    collection_id INTEGER NOT NULL,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, collection_id)
);

CREATE INDEX IF NOT EXISTS idx_collection_likes      ON collection_likes(collection_id);
CREATE INDEX IF NOT EXISTS idx_collection_likes_user ON collection_likes(user_id);

CREATE TABLE IF NOT EXISTS collection_trophies (
    id           SERIAL PRIMARY KEY,
    collection_id INTEGER NOT NULL,
    user_id       INTEGER NOT NULL,
    iso_week      TEXT NOT NULL,
    likes_count   INTEGER NOT NULL,
    awarded_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(iso_week)
);

CREATE INDEX IF NOT EXISTS idx_collection_trophies_week ON collection_trophies(iso_week);

-- ── Очередь уведомлений (дайджест) ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS notification_queue (
    id             SERIAL PRIMARY KEY,
    user_id        INTEGER NOT NULL,
    manga_id       TEXT NOT NULL,
    manga_title    TEXT NOT NULL,
    manga_slug     TEXT NOT NULL,
    chapter_number TEXT,
    chapter_volume TEXT,
    chapter_name   TEXT,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, manga_id, chapter_number)
);

CREATE INDEX IF NOT EXISTS idx_notification_queue_user ON notification_queue(user_id);

-- ── Уведомления на сайте ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS site_notifications (
    id         SERIAL PRIMARY KEY,
    user_id    INTEGER NOT NULL REFERENCES users(id),
    type       TEXT NOT NULL,
    title      TEXT NOT NULL,
    body       TEXT,
    url        TEXT,
    ref_id     TEXT,
    is_read    INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_site_notifications ON site_notifications(user_id, is_read);

CREATE TABLE IF NOT EXISTS admin_broadcasts (
    id               SERIAL PRIMARY KEY,
    admin_id         INTEGER,
    title            TEXT NOT NULL,
    body             TEXT,
    url              TEXT,
    notif_type       TEXT DEFAULT 'admin',
    filter_desc      TEXT,
    recipients_count INTEGER DEFAULT 0,
    send_telegram    INTEGER DEFAULT 0,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── Комментарии ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS comments (
    id         SERIAL PRIMARY KEY,
    manga_slug TEXT    NOT NULL,
    user_id    INTEGER NOT NULL REFERENCES users(id),
    text       TEXT    NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    parent_id  INTEGER REFERENCES comments(id)
);

CREATE INDEX IF NOT EXISTS idx_comments_manga ON comments(manga_slug, created_at DESC);

CREATE TABLE IF NOT EXISTS comment_likes (
    id         SERIAL PRIMARY KEY,
    user_id    INTEGER NOT NULL REFERENCES users(id),
    comment_id INTEGER NOT NULL REFERENCES comments(id),
    UNIQUE(user_id, comment_id)
);

CREATE INDEX IF NOT EXISTS idx_comment_likes_comment ON comment_likes(comment_id);

-- ── Покупки ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS coin_purchases (
    id             SERIAL PRIMARY KEY,
    user_id        INTEGER NOT NULL REFERENCES users(id),
    package_id     TEXT NOT NULL,
    stars_paid     INTEGER NOT NULL,
    coins_received INTEGER NOT NULL,
    payment_id     TEXT UNIQUE NOT NULL,
    payment_method TEXT DEFAULT 'stars',
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS premium_purchases (
    id             SERIAL PRIMARY KEY,
    user_id        INTEGER NOT NULL REFERENCES users(id),
    package_id     TEXT NOT NULL,
    payment_id     TEXT UNIQUE NOT NULL,
    payment_method TEXT DEFAULT 'yookassa',
    expires_at     TEXT NOT NULL,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS premium_gifts (
    id           SERIAL PRIMARY KEY,
    sender_id    INTEGER NOT NULL REFERENCES users(id),
    recipient_id INTEGER NOT NULL REFERENCES users(id),
    days         INTEGER NOT NULL,
    stars_paid   INTEGER NOT NULL,
    payment_id   TEXT UNIQUE NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_premium_gifts_recipient ON premium_gifts(recipient_id);

-- ── Задания (квесты) ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS quests (
    id              SERIAL PRIMARY KEY,
    title           TEXT UNIQUE NOT NULL,
    description     TEXT DEFAULT '',
    icon            TEXT DEFAULT '📋',
    icon_url        TEXT,
    required_level  INTEGER NOT NULL DEFAULT 1,
    condition_type  TEXT NOT NULL,
    condition_value INTEGER NOT NULL DEFAULT 1,
    xp_reward       INTEGER DEFAULT 0,
    coins_reward    INTEGER DEFAULT 0,
    is_active       INTEGER DEFAULT 1,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_quests (
    id           SERIAL PRIMARY KEY,
    user_id      INTEGER NOT NULL REFERENCES users(id),
    quest_id     INTEGER NOT NULL REFERENCES quests(id),
    progress     INTEGER DEFAULT 0,
    completed_at TIMESTAMP,
    UNIQUE(user_id, quest_id)
);

CREATE INDEX IF NOT EXISTS idx_user_quests ON user_quests(user_id);

-- ── Ежедневные задания ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS daily_quests (
    id              SERIAL PRIMARY KEY,
    title           TEXT NOT NULL,
    description     TEXT,
    icon            TEXT DEFAULT '📅',
    condition_type  TEXT NOT NULL,
    condition_value INTEGER NOT NULL,
    xp_reward       INTEGER DEFAULT 0,
    coins_reward    INTEGER DEFAULT 0,
    is_active       INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS user_daily_quests (
    id           SERIAL PRIMARY KEY,
    user_id      INTEGER NOT NULL REFERENCES users(id),
    quest_id     INTEGER NOT NULL,
    date         TEXT NOT NULL,
    progress     INTEGER DEFAULT 0,
    completed_at TEXT,
    UNIQUE(user_id, quest_id, date)
);

CREATE INDEX IF NOT EXISTS idx_user_daily_quests ON user_daily_quests(user_id, date);

-- ── Сезоны ───────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS seasons (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT,
    icon        TEXT DEFAULT '🌸',
    banner_url  TEXT,
    starts_at   TEXT NOT NULL,
    ends_at     TEXT NOT NULL,
    is_active   INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS season_quests (
    id              SERIAL PRIMARY KEY,
    season_id       INTEGER NOT NULL REFERENCES seasons(id),
    title           TEXT NOT NULL,
    description     TEXT,
    icon            TEXT DEFAULT '✨',
    condition_type  TEXT NOT NULL,
    condition_value INTEGER NOT NULL,
    xp_reward       INTEGER DEFAULT 0,
    coins_reward    INTEGER DEFAULT 0,
    item_reward_id  INTEGER
);

CREATE TABLE IF NOT EXISTS user_season_quests (
    id              SERIAL PRIMARY KEY,
    user_id         INTEGER NOT NULL REFERENCES users(id),
    season_quest_id INTEGER NOT NULL,
    progress        INTEGER DEFAULT 0,
    completed_at    TEXT,
    UNIQUE(user_id, season_quest_id)
);

CREATE INDEX IF NOT EXISTS idx_user_season_quests ON user_season_quests(user_id);

-- ── Список "Хочу прочитать" ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS reading_wishlist (
    id       SERIAL PRIMARY KEY,
    user_id  INTEGER NOT NULL REFERENCES users(id),
    manga_id TEXT NOT NULL,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, manga_id)
);

CREATE INDEX IF NOT EXISTS idx_wishlist_user ON reading_wishlist(user_id);

-- ── Личные статусы манги ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS user_manga_status (
    id         SERIAL PRIMARY KEY,
    user_id    INTEGER NOT NULL REFERENCES users(id),
    manga_id   TEXT NOT NULL,
    status     TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, manga_id)
);

CREATE INDEX IF NOT EXISTS idx_user_manga_status ON user_manga_status(user_id);

-- ── Рефералы ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS referrals (
    id          SERIAL PRIMARY KEY,
    referrer_id INTEGER NOT NULL REFERENCES users(id),
    referred_id INTEGER NOT NULL UNIQUE REFERENCES users(id),
    rewarded    INTEGER DEFAULT 0,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id);

-- ── Кураторы ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS curator_follows (
    id          SERIAL PRIMARY KEY,
    follower_id INTEGER NOT NULL REFERENCES users(id),
    author_id   INTEGER NOT NULL REFERENCES users(id),
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(follower_id, author_id)
);

CREATE INDEX IF NOT EXISTS idx_curator_follows          ON curator_follows(author_id);
CREATE INDEX IF NOT EXISTS idx_curator_follows_follower ON curator_follows(follower_id);

-- ── Дедупликация рекомендаций ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS sent_similar_notifications (
    user_id  INTEGER NOT NULL,
    manga_id INTEGER NOT NULL,
    sent_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, manga_id)
);

-- ════════════════════════════════════════════════════════════════════════════
-- SEED DATA
-- ════════════════════════════════════════════════════════════════════════════

-- Достижения
INSERT INTO achievements (key, name, description, icon, xp_reward, condition_type, condition_value) VALUES
    ('first_chapter',  'Первый шаг',        'Прочитать первую главу',    '📖', 50,   'chapters_read', 1),
    ('reader_10',      'Читатель',          'Прочитать 10 глав',         '📚', 100,  'chapters_read', 10),
    ('reader_50',      'Книголюб',          'Прочитать 50 глав',         '🔖', 200,  'chapters_read', 50),
    ('reader_100',     'Книгочей',          'Прочитать 100 глав',        '🎓', 500,  'chapters_read', 100),
    ('reader_500',     'Запойный читатель', 'Прочитать 500 глав',        '🌟', 1000, 'chapters_read', 500),
    ('reader_1000',    'Маньяк чтения',     'Прочитать 1000 глав',       '👑', 2000, 'chapters_read', 1000),
    ('subscriber_1',   'Фанат',             'Подписаться на 1 мангу',    '❤️', 50,   'subscriptions', 1),
    ('subscriber_5',   'Следопыт',          'Подписаться на 5 манг',     '💫', 150,  'subscriptions', 5),
    ('subscriber_10',  'Коллекционер',      'Подписаться на 10 манг',    '💎', 300,  'subscriptions', 10),
    ('level_5',        'Опытный',           'Достичь 5 уровня',          '⚡', 0,    'level',         5),
    ('level_10',       'Бывалый',           'Достичь 10 уровня',         '🔥', 0,    'level',         10),
    ('level_20',       'Ветеран',           'Достичь 20 уровня',         '🏆', 0,    'level',         20),
    ('level_50',       'Легенда',           'Достичь 50 уровня',         '🌈', 0,    'level',         50)
ON CONFLICT (key) DO NOTHING;

-- Квесты
INSERT INTO quests (title, description, icon, required_level, condition_type, condition_value, xp_reward, coins_reward) VALUES
    ('Первый комментарий',  'Напиши свой первый комментарий к манге',  '💬', 1,  'comments_posted', 1,   50,   20),
    ('Начало пути',         'Прочитай 5 глав любой манги',             '📖', 1,  'chapters_read',   5,   75,   30),
    ('Подписчик',           'Подпишись на 1 мангу',                    '❤️', 2,  'subscriptions',   1,   100,  50),
    ('Активный читатель',   'Прочитай 20 глав',                        '📚', 2,  'chapters_read',   20,  150,  60),
    ('Болтун',              'Напиши 5 комментариев',                   '🗣️', 3,  'comments_posted', 5,   200,  80),
    ('Библиотека',          'Подпишись на 3 манги',                    '🗂️', 3,  'subscriptions',   3,   200,  80),
    ('Профи',               'Прочитай 100 глав',                       '🔥', 5,  'chapters_read',   100, 500,  200),
    ('Завсегдатай',         'Подпишись на 10 манг',                    '💎', 5,  'subscriptions',   10,  400,  150),
    ('Ветеран',             'Прочитай 500 глав',                       '🏆', 10, 'chapters_read',   500, 1000, 500),
    ('Великий комментатор', 'Напиши 50 комментариев',                  '👑', 10, 'comments_posted', 50,  800,  300)
ON CONFLICT (title) DO NOTHING;

-- Ежедневные задания
INSERT INTO daily_quests (title, description, icon, condition_type, condition_value, xp_reward, coins_reward) VALUES
    ('Читатель дня',   'Прочитай 3 главы сегодня',       '📖', 'chapters_today', 3, 50, 12),
    ('Комментатор',    'Оставь 1 комментарий сегодня',   '💬', 'comments_today', 1, 30, 6),
    ('Исследователь',  'Открой 2 разные манги сегодня',  '🔍', 'manga_today',    2, 30, 6);

-- Сезон Spring 2026
INSERT INTO seasons (id, name, description, icon, starts_at, ends_at, is_active)
    VALUES (1, 'Весна 2026', 'Сезон цветения — читай, комментируй и побеждай!',
            '🌸', '2026-03-01', '2026-04-30', 1)
ON CONFLICT DO NOTHING;

-- Предметы магазина
INSERT INTO shop_items (name, description, type, preview_url, css_value, price, is_upload) VALUES
    ('Золотая рамка',    'Роскошная золотая рамка для аватара',    'frame',      NULL, 'border: 3px solid #FFD700; box-shadow: 0 0 12px #FFD700;',       1200, 0),
    ('Неоновая рамка',   'Ярко-фиолетовая неоновая рамка',         'frame',      NULL, 'border: 3px solid #a855f7; box-shadow: 0 0 16px #a855f7;',       3000, 0),
    ('Радужная рамка',   'Переливающаяся RGB рамка',               'frame',      NULL, 'border: 3px solid transparent; background: linear-gradient(#141414,#141414) padding-box, linear-gradient(135deg,#f43f5e,#a855f7,#3b82f6) border-box;', 7000, 0),
    ('Аниме рамка',      'Рамка в стиле аниме с сакурой',          'frame',      NULL, 'border: 3px solid #ec4899; box-shadow: 0 0 12px #ec4899;',        1800, 0),
    ('Ночной город',     'Тёмный городской пейзаж',                'background', NULL, 'background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);', 800,  0),
    ('Сакура',           'Нежно-розовый цветочный фон',            'background', NULL, 'background: linear-gradient(135deg, #f8b4d9, #f093fb, #f5576c);', 800,  0),
    ('Космос',           'Звёздное небо',                          'background', NULL, 'background: linear-gradient(135deg, #0d0d1a, #1a1a3e, #0d0d1a); background-size:400% 400%;', 1500, 0),
    ('Океан',            'Глубокий океанский градиент',            'background', NULL, 'background: linear-gradient(135deg, #001f3f, #0074D9, #7FDBFF);', 1000, 0),
    ('VIP',              'Эксклюзивный VIP значок',                'badge',      NULL, '👑 VIP',                                                          8000, 0),
    ('Отаку',            'Значок настоящего отаку',                'badge',      NULL, '🎌 Отаку',                                                        2500, 0),
    ('Манга-гуру',       'Для тех, кто знает толк',                'badge',      NULL, '📖 Манга-гуру',                                                   5000, 0),
    ('Загрузка аватара', 'Разблокировать загрузку своего аватара', 'avatar_slot', NULL, NULL,                                                             0,    1),
    ('Загрузка фона',    'Разблокировать загрузку своего фона',    'bg_slot',    NULL, NULL,                                                              1500, 1)
ON CONFLICT DO NOTHING;

-- Сезонный предмет (id=100 — задаём вручную)
INSERT INTO shop_items (id, name, description, type, preview_url, css_value, price, is_upload)
    VALUES (100, 'Рамка «Весна»', 'Сезонная рамка Spring 2026',
            'frame', NULL, 'border: 3px solid #ec4899; box-shadow: 0 0 14px #f9a8d4;', 0, 0)
ON CONFLICT DO NOTHING;

-- Задания сезона
INSERT INTO season_quests (season_id, title, description, icon, condition_type, condition_value, xp_reward, coins_reward, item_reward_id) VALUES
    (1, 'Весенний читатель',  'Прочитай 50 глав за сезон',   '📚', 'chapters_read',   50,  200, 100, NULL),
    (1, 'Болтун сезона',      'Оставь 10 комментариев',      '💬', 'comments_posted', 10,  150, 50,  NULL),
    (1, 'Коллекционер весны', 'Прочитай 100 глав за сезон',  '🌸', 'chapters_read',   100, 500, 200, 100)
ON CONFLICT DO NOTHING;
