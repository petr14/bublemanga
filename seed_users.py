"""
Генератор 200 фейковых пользователей для BubbleManga.

Что создаётся:
  - 200 users с telegram_id 9_000_001..9_000_200 (префикс 9M — не пересекается с реальными)
  - notifications_enabled = 0  (не получают уведомления)
  - Пометка telegram_username = "bot_seed_XXXXX" (легко найти и удалить)
  - user_stats: XP, level, coins, chapters/pages read
  - user_profile: custom_name, avatar_url, background_url, frame, badge
  - user_items: купленные предметы из shop_items (случайные)
  - subscriptions: 1-15 случайных манг из manga
  - reading_history: последняя прочитанная глава каждой подписки
  - chapters_read: набор прочитанных глав (соответствует уровню XP)

Запуск:
  python seed_users.py            # создать 200 пользователей
  python seed_users.py --count 50 # создать 50
  python seed_users.py --cleanup  # удалить всех seed-пользователей
"""

import argparse
import math
import os
import random
import sqlite3
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), "manga.db")

# ── Имена ─────────────────────────────────────────────────────────────────────
FIRST_NAMES = [
    "Александр","Дмитрий","Максим","Иван","Артём","Михаил","Никита","Кирилл",
    "Андрей","Сергей","Алексей","Антон","Владимир","Егор","Роман","Денис",
    "Анастасия","Мария","Дарья","Екатерина","Ксения","Алина","Юлия","Ольга",
    "Полина","Виктория","Наталья","Елена","Ирина","Татьяна","Вера","Надежда",
    "Тимур","Руслан","Виталий","Павел","Николай","Георгий","Фёдор","Леонид",
    "Aiko","Hana","Yuki","Sora","Rin","Kai","Ren","Shin","Nori","Taka",
    "MangaLord","DarkReader","ShadowFox","NightOwl","StarDust","CoolGuy",
]
LAST_NAMES = [
    "Иванов","Смирнов","Кузнецов","Попов","Васильев","Петров","Соколов",
    "Михайлов","Новиков","Фёдоров","Морозов","Волков","Алексеев","Лебедев",
    "Семёнов","Егоров","Павлов","Козлов","Степанов","Николаев","Орлов",
    "","","","","","",   # часть без фамилии
]
BIOS = [
    "Читаю мангу с 2010 года 📚",
    "Люблю сёнэн и экшн ⚔️",
    "Фанат One Piece с детства",
    "Романтика и слайс оф лайф — моё всё 💕",
    "Isekai — лучший жанр, спорить бесполезно",
    "Просто читаю мангу после работы",
    "Охотник за редкими тайтлами",
    "Топ 1 читатель этого сайта (наверное)",
    "Манга > аниме 🔥",
    "Шото ни цукэтэ ёму",
    "",  # без bio
    "",
    "",
]

# ── Аватары (Dicebear/UI-Avatars стиль — рабочие публичные URL) ───────────────
AVATAR_STYLES = [
    "adventurer", "avataaars", "big-ears", "bottts",
    "croodles", "fun-emoji", "icons", "lorelei",
    "micah", "miniavs", "open-peeps", "personas",
    "pixel-art", "shapes",
]

def random_avatar_url(seed: str) -> str:
    style = random.choice(AVATAR_STYLES)
    return f"https://api.dicebear.com/7.x/{style}/svg?seed={seed}"

# ── Фоны профиля (публичные градиенты через CSS или плейсхолдеры) ────────────
BACKGROUND_URLS = [
    "https://images.unsplash.com/photo-1604076913837-52ab5629fde4?w=800&q=60",
    "https://images.unsplash.com/photo-1519638399535-1b036603ac77?w=800&q=60",
    "https://images.unsplash.com/photo-1465101162946-4377e57745c3?w=800&q=60",
    "https://images.unsplash.com/photo-1502082553048-f009c37129b9?w=800&q=60",
    "https://images.unsplash.com/photo-1531306728370-e2ebd9d7bb99?w=800&q=60",
    "https://images.unsplash.com/photo-1534796636912-3b95b3ab5986?w=800&q=60",
    "https://images.unsplash.com/photo-1506905925346-21bda4d32df4?w=800&q=60",
    "https://images.unsplash.com/photo-1518020382113-a7e8fc38eac9?w=800&q=60",
    "https://images.unsplash.com/photo-1475274047050-1d0c0975c63e?w=800&q=60",
    "https://images.unsplash.com/photo-1507003211169-0a1dd7228f2d?w=800&q=60",
    None, None, None,   # часть без фона
]

# ── XP формула (из main.py) ───────────────────────────────────────────────────
def get_level_from_xp(xp: int) -> int:
    return max(1, int(math.floor(math.sqrt(max(0, xp) / 100))) + 1)

def xp_for_level(level: int) -> int:
    return (level - 1) ** 2 * 100

def random_xp() -> int:
    """Реалистичное распределение: большинство низкого уровня, единицы — высокого."""
    roll = random.random()
    if roll < 0.40:   # 40% — уровни 1-3
        level = random.randint(1, 3)
    elif roll < 0.70: # 30% — уровни 4-8
        level = random.randint(4, 8)
    elif roll < 0.88: # 18% — уровни 9-15
        level = random.randint(9, 15)
    elif roll < 0.97: # 9%  — уровни 16-25
        level = random.randint(16, 25)
    else:             # 3%  — уровни 26-40
        level = random.randint(26, 40)
    base = xp_for_level(level)
    next_base = xp_for_level(level + 1)
    # XP внутри уровня — случайный прогресс
    return base + random.randint(0, next_base - base - 1)

def rand_date(days_ago_max: int, days_ago_min: int = 0) -> str:
    delta = random.randint(days_ago_min, days_ago_max)
    dt = datetime.utcnow() - timedelta(days=delta)
    return dt.isoformat(sep=" ", timespec="seconds")

# ── DB ────────────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

SEED_PREFIX = 9_000_000   # telegram_id для seed-юзеров

def seed(count: int):
    conn = get_db()
    c = conn.cursor()

    # ── Нужны реальные manga_id из БД ────────────────────────────────────────
    c.execute("SELECT manga_id, last_chapter_id, last_chapter_slug FROM manga ORDER BY RANDOM() LIMIT 500")
    mangas = c.fetchall()
    if not mangas:
        print("❌ В manga нет записей. Сначала запусти parse_all_manga.py")
        conn.close()
        return

    # ── Главы для reading_history ─────────────────────────────────────────────
    c.execute("SELECT chapter_id, manga_id, chapter_number FROM chapters ORDER BY RANDOM() LIMIT 2000")
    all_chapters = c.fetchall()

    # ── Предметы магазина ─────────────────────────────────────────────────────
    c.execute("SELECT id, type, preview_url, css_value FROM shop_items WHERE is_upload = 0")
    shop_items = c.fetchall()
    items_by_type = {}
    for it in shop_items:
        items_by_type.setdefault(it["type"], []).append(it)

    # ── Ачивки ────────────────────────────────────────────────────────────────
    c.execute("SELECT id, condition_type, condition_value FROM achievements")
    achievements = c.fetchall()

    created = 0
    skipped = 0

    print(f"🌱 Создаю {count} пользователей...")

    for i in range(1, count + 1):
        tg_id = SEED_PREFIX + i
        uname = f"bot_seed_{i:05d}"
        first = random.choice(FIRST_NAMES)
        last  = random.choice(LAST_NAMES)
        joined = rand_date(730, 7)   # зарегался 7..730 дней назад
        last_login = rand_date(30, 0)

        is_premium = 1 if random.random() < 0.12 else 0   # ~12% premium
        prem_expires = None
        if is_premium:
            days = random.choice([30, 30, 90, 90, 365])
            prem_expires = (datetime.utcnow() + timedelta(days=random.randint(1, days))).isoformat()

        # ── users ─────────────────────────────────────────────────────────────
        try:
            c.execute(
                """INSERT OR IGNORE INTO users
                   (telegram_id, telegram_username, telegram_first_name, telegram_last_name,
                    is_active, notifications_enabled, is_premium, premium_expires_at,
                    created_at, last_login)
                   VALUES (?,?,?,?,1,0,?,?,?,?)""",
                (tg_id, uname, first, last or None,
                 is_premium, prem_expires, joined, last_login)
            )
            if c.rowcount == 0:
                skipped += 1
                continue
            conn.commit()
        except sqlite3.IntegrityError:
            skipped += 1
            continue

        c.execute("SELECT id FROM users WHERE telegram_id = ?", (tg_id,))
        user_id = c.fetchone()["id"]

        # ── XP / stats ────────────────────────────────────────────────────────
        xp = random_xp()
        level = get_level_from_xp(xp)
        coins = random.randint(0, level * 80)
        chapters_read = random.randint(
            max(0, level * 3 - 10),
            level * 25 + random.randint(0, 50)
        )
        pages_read = chapters_read * random.randint(18, 45)

        c.execute(
            """INSERT OR REPLACE INTO user_stats
               (user_id, xp, coins, level, total_chapters_read, total_pages_read)
               VALUES (?,?,?,?,?,?)""",
            (user_id, xp, coins, level, chapters_read, pages_read)
        )

        # ── user_profile ──────────────────────────────────────────────────────
        avatar_url = random_avatar_url(f"{uname}{i}")
        bg_url     = random.choice(BACKGROUND_URLS)
        bio        = random.choice(BIOS)
        display_name = f"{first} {last}".strip() if last and random.random() > 0.4 else first

        # Предметы в профиле (frame, badge — из shop_items)
        frame_id = None
        badge_id = None
        if items_by_type.get("frame") and random.random() > 0.4:
            frame_id = random.choice(items_by_type["frame"])["id"]
        if items_by_type.get("badge") and random.random() > 0.5:
            badge_id = random.choice(items_by_type["badge"])["id"]

        c.execute(
            """INSERT OR REPLACE INTO user_profile
               (user_id, avatar_url, background_url, frame_item_id, badge_item_id, bio, custom_name)
               VALUES (?,?,?,?,?,?,?)""",
            (user_id, avatar_url, bg_url, frame_id, badge_id, bio, display_name)
        )

        # ── user_items (купленные предметы) ───────────────────────────────────
        # Сколько предметов куплено — зависит от уровня
        n_items = min(len(shop_items), random.randint(0, max(1, level // 2)))
        if shop_items and n_items > 0:
            chosen = random.sample(shop_items, n_items)
            for it in chosen:
                is_equipped = 0
                if it["id"] == frame_id or it["id"] == badge_id:
                    is_equipped = 1
                bought_at = rand_date(int((datetime.utcnow() - datetime.fromisoformat(joined)).days), 0)
                c.execute(
                    """INSERT OR IGNORE INTO user_items
                       (user_id, item_id, purchased_at, is_equipped) VALUES (?,?,?,?)""",
                    (user_id, it["id"], bought_at, is_equipped)
                )

        # ── subscriptions ─────────────────────────────────────────────────────
        n_subs = random.randint(1, min(len(mangas), 15))
        sub_mangas = random.sample(mangas, n_subs)
        for m in sub_mangas:
            sub_date = rand_date(int((datetime.utcnow() - datetime.fromisoformat(joined)).days), 0)
            c.execute(
                "INSERT OR IGNORE INTO subscriptions (user_id, manga_id, subscribed_at) VALUES (?,?,?)",
                (user_id, m["manga_id"], sub_date)
            )

        # ── reading_history (последняя прочитанная глава каждой подписки) ─────
        for m in sub_mangas:
            if m["last_chapter_id"]:
                read_at = rand_date(30, 0)
                page = random.randint(1, 40)
                c.execute(
                    """INSERT OR REPLACE INTO reading_history
                       (user_id, manga_id, chapter_id, page_number, last_read)
                       VALUES (?,?,?,?,?)""",
                    (user_id, m["manga_id"], m["last_chapter_id"], page, read_at)
                )

        # ── chapters_read (набор прочитанных глав из общего пула) ────────────
        if all_chapters and chapters_read > 0:
            n_ch = min(len(all_chapters), chapters_read, random.randint(1, 60))
            for ch in random.sample(all_chapters, n_ch):
                read_at = rand_date(365, 0)
                c.execute(
                    "INSERT OR IGNORE INTO chapters_read (user_id, chapter_id, manga_id, read_at) VALUES (?,?,?,?)",
                    (user_id, ch["chapter_id"], ch["manga_id"], read_at)
                )

        # ── user_achievements (по уровню) ─────────────────────────────────────
        for ach in achievements:
            unlocked = False
            ct = ach["condition_type"]
            cv = ach["condition_value"]
            if ct == "chapters_read"   and chapters_read >= cv:  unlocked = True
            if ct == "subscriptions"   and n_subs >= cv:         unlocked = True
            if ct == "comments_posted" and random.random() < 0.15: unlocked = True
            if unlocked:
                unlock_date = rand_date(
                    int((datetime.utcnow() - datetime.fromisoformat(joined)).days), 0
                )
                c.execute(
                    "INSERT OR IGNORE INTO user_achievements (user_id, achievement_id, unlocked_at) VALUES (?,?,?)",
                    (user_id, ach["id"], unlock_date)
                )

        conn.commit()
        created += 1

        if created % 25 == 0:
            print(f"   ... {created}/{count}")

    conn.close()
    print(f"\n✅ Создано: {created} | Пропущено (уже существуют): {skipped}")
    print(f"   telegram_id диапазон: {SEED_PREFIX+1} – {SEED_PREFIX+count}")
    print(f"   Метка: telegram_username LIKE 'bot_seed_%'")
    print(f"   Для удаления: python seed_users.py --cleanup")


def cleanup():
    conn = get_db()
    c = conn.cursor()

    c.execute("SELECT id FROM users WHERE telegram_username LIKE 'bot_seed_%'")
    ids = [r["id"] for r in c.fetchall()]
    if not ids:
        print("Нет seed-пользователей в БД.")
        conn.close()
        return

    ph = ",".join("?" * len(ids))
    for table in ("user_achievements", "chapters_read", "reading_history",
                  "subscriptions", "user_items", "user_profile",
                  "user_stats", "notification_queue"):
        try:
            c.execute(f"DELETE FROM {table} WHERE user_id IN ({ph})", ids)
        except Exception:
            pass

    c.execute(f"DELETE FROM users WHERE id IN ({ph})", ids)
    conn.commit()
    conn.close()
    print(f"✅ Удалено {len(ids)} seed-пользователей и все их данные.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count",   type=int, default=200, help="Кол-во пользователей (default: 200)")
    parser.add_argument("--cleanup", action="store_true",   help="Удалить всех seed-пользователей")
    args = parser.parse_args()

    if not os.path.exists(DB_PATH):
        print(f"❌ БД не найдена: {DB_PATH}")
        raise SystemExit(1)

    if args.cleanup:
        cleanup()
    else:
        seed(args.count)
