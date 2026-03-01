"""
Скрипт проверки работы Telegram-бота BubbleManga.

Что проверяет:
  1. Токен бота валиден (getMe)
  2. Бот может отправить сообщение администратору
  3. Пользователи с подписками получают уведомления (тестовая отправка)
  4. notification_queue — есть ли записи, не отправленные за последние 24ч

Запуск:
  python test_bot.py                  # отправить тестовое сообщение всем админам
  python test_bot.py --uid 123456789  # отправить конкретному Telegram ID
  python test_bot.py --queue          # показать содержимое очереди уведомлений
  python test_bot.py --dry            # только проверить токен, не отправлять
"""

import asyncio
import argparse
import os
import sqlite3
import sys
from datetime import datetime, timedelta

# ── Конфиг (берём из main.py или env) ────────────────────────────────────────
BOT_TOKEN = os.environ.get(
    "BOT_TOKEN",
    "7082209603:AAG97jX6MHgYOywy5hdDl03hduVMD6VBsW0"
)
ADMIN_IDS = [319026942, 649144994]
DB_PATH   = os.path.join(os.path.dirname(__file__), "manga.db")

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def fmt(ok: bool) -> str:
    return "✅" if ok else "❌"


# ── Тест 1: getMe ─────────────────────────────────────────────────────────────
async def test_get_me(bot):
    try:
        me = await bot.get_me()
        print(f"{fmt(True)} getMe: @{me.username} (id={me.id})")
        return True
    except Exception as e:
        print(f"{fmt(False)} getMe: {e}")
        return False


# ── Тест 2: отправка сообщения ────────────────────────────────────────────────
async def test_send(bot, chat_id: int, label: str = ""):
    msg = (
        "🤖 <b>BubbleManga — тест уведомлений</b>\n\n"
        "Если ты видишь это сообщение — бот работает корректно.\n"
        f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
    )
    try:
        await bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
        print(f"{fmt(True)} send_message → {label or chat_id}")
        return True
    except Exception as e:
        print(f"{fmt(False)} send_message → {label or chat_id}: {e}")
        return False


# ── Тест 3: DB — пользователи с подписками ───────────────────────────────────
def test_db_subscribers():
    if not os.path.exists(DB_PATH):
        print(f"❌ БД не найдена: {DB_PATH}")
        return
    conn = get_db()
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM users WHERE is_active = 1")
    total_users = c.fetchone()[0]

    c.execute(
        "SELECT COUNT(DISTINCT user_id) FROM subscriptions"
    )
    subscribed = c.fetchone()[0]

    c.execute(
        "SELECT COUNT(DISTINCT user_id) FROM users "
        "WHERE is_premium = 1 AND is_active = 1"
    )
    premium = c.fetchone()[0]

    c.execute(
        "SELECT COUNT(DISTINCT user_id) FROM users "
        "WHERE notifications_enabled = 1 AND is_active = 1"
    )
    notif_on = c.fetchone()[0]

    conn.close()

    print(f"\n📊 База данных ({DB_PATH}):")
    print(f"   Пользователей активных : {total_users}")
    print(f"   С подписками           : {subscribed}")
    print(f"   Premium                : {premium}")
    print(f"   Уведомления включены   : {notif_on}")


# ── Тест 4: notification_queue ───────────────────────────────────────────────
def test_queue():
    if not os.path.exists(DB_PATH):
        print(f"❌ БД не найдена: {DB_PATH}")
        return
    conn = get_db()
    c = conn.cursor()

    try:
        c.execute(
            "SELECT nq.user_id, u.telegram_id, nq.manga_title, "
            "       nq.chapter_number, nq.created_at "
            "FROM notification_queue nq "
            "JOIN users u ON nq.user_id = u.id "
            "ORDER BY nq.created_at DESC LIMIT 20"
        )
        rows = c.fetchall()
    except sqlite3.OperationalError as e:
        print(f"❌ notification_queue: {e}")
        conn.close()
        return

    conn.close()

    if not rows:
        print("\n📭 notification_queue: пусто")
        return

    print(f"\n📬 notification_queue ({len(rows)} записей, последние 20):")
    for r in rows:
        print(
            f"   tg={r['telegram_id']} | "
            f"{r['manga_title']} гл.{r['chapter_number']} | "
            f"{r['created_at']}"
        )


# ── Тест 5: проверить last_known_chapters через manga.db ─────────────────────
def test_last_chapters():
    if not os.path.exists(DB_PATH):
        return
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "SELECT manga_title, last_chapter_number, last_updated "
        "FROM manga WHERE last_chapter_id IS NOT NULL "
        "ORDER BY last_updated DESC LIMIT 5"
    )
    rows = c.fetchall()
    conn.close()

    if not rows:
        print("\n⚠️  Нет манги с известными главами в БД")
        return

    print("\n📖 Последние известные главы (топ 5 по дате):")
    for r in rows:
        print(f"   {r['manga_title']} — гл.{r['last_chapter_number']} ({r['last_updated']})")


# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser(description="Тест Telegram-бота BubbleManga")
    parser.add_argument("--uid",   type=int, help="Telegram ID для тестовой отправки")
    parser.add_argument("--queue", action="store_true", help="Показать notification_queue")
    parser.add_argument("--dry",   action="store_true", help="Только getMe, без отправки")
    args = parser.parse_args()

    try:
        from telegram import Bot
    except ImportError:
        print("❌ python-telegram-bot не установлен: pip install python-telegram-bot")
        sys.exit(1)

    bot = Bot(token=BOT_TOKEN)

    print("=" * 50)
    print("  BubbleManga — проверка бота")
    print("=" * 50)

    # 1. getMe
    ok = await test_get_me(bot)
    if not ok:
        print("\n🔴 Токен невалиден. Дальнейшие тесты невозможны.")
        sys.exit(1)

    # 2. Отправка сообщений
    if not args.dry:
        targets = [(args.uid, "custom")] if args.uid else [(uid, "admin") for uid in ADMIN_IDS]
        for chat_id, label in targets:
            await test_send(bot, chat_id, label)

    # 3. Статистика БД
    test_db_subscribers()

    # 4. Queue
    if args.queue:
        test_queue()

    # 5. Последние главы
    test_last_chapters()

    print("\n" + "=" * 50)
    await bot.close()


if __name__ == "__main__":
    asyncio.run(main())
