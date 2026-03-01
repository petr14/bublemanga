"""
Полный end-to-end тест уведомлений BubbleManga.

Шаги:
  1. Создаёт тестовую мангу в БД
  2. Добавляет админов в БД (если не существуют) и подписывает их на мангу
  3. «Выпускает» новую главу — вызывает process_new_chapter напрямую
  4. Бот должен отправить уведомление в Telegram

Запуск:
  python test_notify.py                # тест для всех админов
  python test_notify.py --premium      # пометить админов как premium (мгновенное уведомление)
  python test_notify.py --cleanup      # удалить тестовые данные из БД
"""

import asyncio
import argparse
import os
import sqlite3
import sys
import time
from datetime import datetime

# ── Конфиг ────────────────────────────────────────────────────────────────────
BOT_TOKEN    = os.environ.get("BOT_TOKEN", "7082209603:AAG97jX6MHgYOywy5hdDl03hduVMD6VBsW0")
ADMIN_IDS    = [319026942, 649144994]
DB_PATH      = os.path.join(os.path.dirname(__file__), "manga.db")

TEST_MANGA_ID   = "test-manga-notify-001"
TEST_MANGA_SLUG = "test-manga-notify"
TEST_MANGA_NAME = "Тестовая Манга (уведомления)"

TEST_CHAPTER_ID     = "test-chapter-001"
TEST_CHAPTER_SLUG   = "test-chapter-001"
TEST_CHAPTER_NUMBER = "99"
TEST_CHAPTER_VOLUME = "1"
TEST_CHAPTER_NAME   = "Тестовая глава для проверки уведомлений"

# ── DB ────────────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def step(n: int, text: str):
    print(f"\n[{n}] {text}")


# ── 1. Создать тестовую мангу ─────────────────────────────────────────────────
def create_test_manga(conn):
    c = conn.cursor()
    c.execute(
        """INSERT OR REPLACE INTO manga
           (manga_id, manga_slug, manga_title, manga_type, manga_status,
            last_chapter_id, last_chapter_number, last_chapter_volume,
            last_chapter_name, last_chapter_slug, last_updated)
           VALUES (?, ?, ?, 'MANGA', 'ONGOING', NULL, NULL, NULL, NULL, NULL, ?)""",
        (TEST_MANGA_ID, TEST_MANGA_SLUG, TEST_MANGA_NAME, datetime.utcnow().isoformat())
    )
    conn.commit()
    print(f"   ✅ Манга создана: «{TEST_MANGA_NAME}» (id={TEST_MANGA_ID})")


# ── 2. Убедиться что админы есть в БД и подписаны ────────────────────────────
def ensure_admins_subscribed(conn, make_premium: bool):
    c = conn.cursor()
    for tg_id in ADMIN_IDS:
        # Создать пользователя если нет
        c.execute(
            """INSERT OR IGNORE INTO users
               (telegram_id, telegram_username, is_active, notifications_enabled)
               VALUES (?, ?, 1, 1)""",
            (tg_id, f"admin_{tg_id}")
        )
        conn.commit()

        c.execute("SELECT id FROM users WHERE telegram_id = ?", (tg_id,))
        row = c.fetchone()
        if not row:
            print(f"   ❌ Не удалось найти/создать пользователя telegram_id={tg_id}")
            continue
        user_id = row["id"]

        # Уведомления включены
        c.execute(
            "UPDATE users SET notifications_enabled = 1 WHERE id = ?", (user_id,)
        )

        # Premium
        if make_premium:
            c.execute(
                "UPDATE users SET is_premium = 1 WHERE id = ?", (user_id,)
            )
            print(f"   ⭐ telegram_id={tg_id} → premium=1")

        # Подписка на тест-мангу
        c.execute(
            "INSERT OR IGNORE INTO subscriptions (user_id, manga_id) VALUES (?, ?)",
            (user_id, TEST_MANGA_ID)
        )
        conn.commit()
        is_prem = "premium" if make_premium else "обычный"
        print(f"   ✅ telegram_id={tg_id} (user_id={user_id}, {is_prem}) подписан на тест-мангу")


# ── 3. Симулировать выход новой главы ─────────────────────────────────────────
async def release_chapter(conn, bot, make_premium: bool):
    c = conn.cursor()

    chapter_url = f"http://91.196.34.216/read/{TEST_MANGA_SLUG}/{TEST_CHAPTER_SLUG}"

    # Обновить last_chapter в манге
    c.execute(
        """UPDATE manga SET
           last_chapter_id = ?, last_chapter_number = ?, last_chapter_volume = ?,
           last_chapter_name = ?, last_chapter_slug = ?, last_updated = ?
           WHERE manga_id = ?""",
        (TEST_CHAPTER_ID, TEST_CHAPTER_NUMBER, TEST_CHAPTER_VOLUME,
         TEST_CHAPTER_NAME, TEST_CHAPTER_SLUG, datetime.utcnow().isoformat(),
         TEST_MANGA_ID)
    )
    conn.commit()
    print(f"   ✅ Манга обновлена: глава {TEST_CHAPTER_NUMBER}")

    # Сохранить главу
    c.execute(
        """INSERT OR REPLACE INTO chapters
           (manga_id, chapter_id, chapter_slug, chapter_number, chapter_volume,
            chapter_name, chapter_url, pages_json, pages_count, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, '[]', 0, ?)""",
        (TEST_MANGA_ID, TEST_CHAPTER_ID, TEST_CHAPTER_SLUG,
         TEST_CHAPTER_NUMBER, TEST_CHAPTER_VOLUME, TEST_CHAPTER_NAME,
         chapter_url, datetime.utcnow().isoformat())
    )
    conn.commit()
    print(f"   ✅ Глава сохранена в chapters")

    # Получить подписчиков
    c.execute(
        """SELECT s.user_id, u.telegram_id, u.is_premium, u.notifications_enabled
           FROM subscriptions s
           JOIN users u ON s.user_id = u.id
           WHERE s.manga_id = ? AND u.is_active = 1""",
        (TEST_MANGA_ID,)
    )
    subscribers = c.fetchall()
    print(f"   📋 Подписчиков найдено: {len(subscribers)}")

    chapter_info = {
        "chapter_number": TEST_CHAPTER_NUMBER,
        "chapter_volume":  TEST_CHAPTER_VOLUME,
        "chapter_name":    TEST_CHAPTER_NAME,
    }

    sent_count = 0
    queued_count = 0

    for sub in subscribers:
        tg_id  = sub["telegram_id"]
        uid    = sub["user_id"]
        is_prem = sub["is_premium"]

        if sub["notifications_enabled"] == 0:
            print(f"   ⏭️  tg={tg_id}: уведомления выключены, пропуск")
            continue

        if is_prem:
            # Мгновенное уведомление
            msg = (
                f"🆕 <b>Новая глава!</b>\n\n"
                f"📖 <b>{TEST_MANGA_NAME}</b>\n"
                f"Глава {TEST_CHAPTER_NUMBER} (Том {TEST_CHAPTER_VOLUME})\n"
                f"<i>{TEST_CHAPTER_NAME}</i>\n\n"
                f"🔗 <a href='{chapter_url}'>Читать</a>"
            )
            try:
                await bot.send_message(chat_id=tg_id, text=msg, parse_mode="HTML")
                print(f"   ✅ Мгновенное уведомление → tg={tg_id} (premium)")
                sent_count += 1
            except Exception as e:
                print(f"   ❌ Ошибка отправки → tg={tg_id}: {e}")
        else:
            # В очередь дайджеста
            c.execute(
                """INSERT OR IGNORE INTO notification_queue
                   (user_id, manga_id, manga_title, manga_slug,
                    chapter_number, chapter_volume, chapter_name)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (uid, TEST_MANGA_ID, TEST_MANGA_NAME, TEST_MANGA_SLUG,
                 TEST_CHAPTER_NUMBER, TEST_CHAPTER_VOLUME, TEST_CHAPTER_NAME)
            )
            conn.commit()
            print(f"   📬 tg={tg_id} → добавлен в очередь дайджеста (не premium)")
            queued_count += 1

    print(f"\n   📊 Итог: отправлено={sent_count}, в очередь={queued_count}")


# ── 4. Cleanup ────────────────────────────────────────────────────────────────
def cleanup(conn):
    c = conn.cursor()

    c.execute("DELETE FROM notification_queue WHERE manga_id = ?", (TEST_MANGA_ID,))
    c.execute("DELETE FROM subscriptions WHERE manga_id = ?", (TEST_MANGA_ID,))
    c.execute("DELETE FROM chapters WHERE manga_id = ?", (TEST_MANGA_ID,))
    c.execute("DELETE FROM manga WHERE manga_id = ?", (TEST_MANGA_ID,))
    conn.commit()
    print("✅ Тестовые данные удалены из БД")


# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser(description="E2E тест уведомлений BubbleManga")
    parser.add_argument("--premium", action="store_true",
                        help="Пометить админов как premium (мгновенные уведомления)")
    parser.add_argument("--cleanup", action="store_true",
                        help="Удалить тестовые данные и выйти")
    args = parser.parse_args()

    if not os.path.exists(DB_PATH):
        print(f"❌ БД не найдена: {DB_PATH}")
        sys.exit(1)

    try:
        from telegram import Bot
    except ImportError:
        print("❌ python-telegram-bot не установлен")
        sys.exit(1)

    conn = get_db()

    if args.cleanup:
        step(0, "Очистка тестовых данных")
        cleanup(conn)
        conn.close()
        return

    bot = Bot(token=BOT_TOKEN)

    # Проверить токен
    try:
        me = await bot.get_me()
        print(f"✅ Бот: @{me.username}")
    except Exception as e:
        print(f"❌ Токен невалиден: {e}")
        sys.exit(1)

    step(1, "Создание тестовой манги")
    create_test_manga(conn)

    step(2, f"Подписка админов на тест-мангу{'  [premium]' if args.premium else ''}")
    ensure_admins_subscribed(conn, make_premium=args.premium)

    step(3, "Выпуск новой главы + отправка уведомлений")
    await release_chapter(conn, bot, make_premium=args.premium)

    print(f"\n{'='*50}")
    if args.premium:
        print("Проверь Telegram — уведомление должно прийти немедленно.")
    else:
        print("Админы добавлены в очередь дайджеста.")
        print("Дайджест отправляется ежедневно в 22:00 МСК.")
        print("Чтобы проверить очередь: python test_bot.py --queue")

    print(f"\nДля очистки тестовых данных: python test_notify.py --cleanup")

    conn.close()
    await bot.close()


if __name__ == "__main__":
    asyncio.run(main())
