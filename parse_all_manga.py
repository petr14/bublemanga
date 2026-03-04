# -*- coding: utf-8 -*-
"""
Скрипт полного парсинга каталога манг Senkuro.

Использование:
    python parse_all_manga.py [--db manga.db] [--order POPULARITY_SCORE]
                              [--delay 0.3] [--no-hentai] [--batch 50]
                              [--resume]

По умолчанию запускается с параметрами из DEFAULT_* ниже.

Что делает:
  1. Итерирует все страницы fetchMangas через SenkuroAPI.fetch_all_mangas()
  2. Каждую мангу вставляет в таблицу manga (INSERT OR IGNORE — не трогает
     уже существующие записи) или обновляет через INSERT OR REPLACE
     если передан флаг --update
  3. Показывает прогресс: номер страницы, кол-во записей, скорость.
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime

# ── Рабочий каталог — папка самого скрипта ────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from senkuro_api import SenkuroAPI

# ── Настройки по умолчанию ────────────────────────────────────────────────
DEFAULT_DB      = os.path.join(BASE_DIR, "manga.db")
DEFAULT_ORDER   = "POPULARITY_SCORE"   # POPULARITY_SCORE | SCORE | UPDATED_AT
DEFAULT_DIR     = "DESC"
DEFAULT_DELAY   = 0.3                  # секунд между запросами
DEFAULT_BATCH   = 50                   # INSERT-батч

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("parse_all_manga")


# ═══════════════════════════════════════════════════════════════════════════
# БД
# ═══════════════════════════════════════════════════════════════════════════

def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA cache_size=-32000")
    conn.row_factory = sqlite3.Row

    # Создаём таблицу manga если не существует (минимальная схема)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS manga (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            manga_id         TEXT UNIQUE NOT NULL,
            manga_slug       TEXT NOT NULL,
            manga_title      TEXT NOT NULL,
            manga_type       TEXT,
            manga_status     TEXT,
            cover_url        TEXT,
            last_chapter_id   TEXT,
            last_chapter_number TEXT,
            last_chapter_volume TEXT,
            last_chapter_name   TEXT,
            last_chapter_slug   TEXT,
            last_updated     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            views            INTEGER DEFAULT 0,
            rating           TEXT DEFAULT 'GENERAL',
            branch_id        TEXT,
            chapters_count   INTEGER DEFAULT 0,
            description      TEXT DEFAULT '',
            score            REAL DEFAULT 0,
            tags             TEXT DEFAULT '[]',
            original_name    TEXT DEFAULT '',
            translation_status TEXT DEFAULT '',
            is_licensed      INTEGER DEFAULT 0,
            formats          TEXT DEFAULT '[]'
        )
    """)

    # Добавляем колонки которых может не быть (миграция)
    extra = [
        ("description",       "TEXT DEFAULT ''"),
        ("score",             "REAL DEFAULT 0"),
        ("tags",              "TEXT DEFAULT '[]'"),
        ("original_name",     "TEXT DEFAULT ''"),
        ("translation_status","TEXT DEFAULT ''"),
        ("is_licensed",       "INTEGER DEFAULT 0"),
        ("formats",           "TEXT DEFAULT '[]'"),
    ]
    for col, defn in extra:
        try:
            conn.execute(f"ALTER TABLE manga ADD COLUMN {col} {defn}")
        except Exception:
            pass  # уже есть

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_manga_slug ON manga(manga_slug)"
    )
    conn.commit()
    return conn


def build_rows(batch: list[dict]) -> list[tuple]:
    """Превращает список dict (из API) в строки для INSERT."""
    rows = []
    for m in batch:
        rows.append((
            m["manga_id"],
            m["manga_slug"],
            m["manga_title"],
            m.get("manga_type"),
            m.get("manga_status"),
            m.get("cover_url", ""),
            m.get("rating", "GENERAL"),
            m.get("score", 0),
            m.get("original_name", ""),
            m.get("is_licensed", 0),
            json.dumps(m.get("formats") or [], ensure_ascii=False),
        ))
    return rows


INSERT_IGNORE = """
    INSERT OR IGNORE INTO manga
        (manga_id, manga_slug, manga_title, manga_type, manga_status,
         cover_url, rating, score, original_name, is_licensed, formats)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)
"""

INSERT_REPLACE = """
    INSERT OR REPLACE INTO manga
        (manga_id, manga_slug, manga_title, manga_type, manga_status,
         cover_url, rating, score, original_name, is_licensed, formats)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)
"""


def flush(conn: sqlite3.Connection, batch: list[dict], update: bool) -> int:
    """Записать батч в БД, вернуть кол-во реально вставленных строк."""
    if not batch:
        return 0
    rows = build_rows(batch)
    sql  = INSERT_REPLACE if update else INSERT_IGNORE
    cur  = conn.executemany(sql, rows)
    conn.commit()
    return cur.rowcount


# ═══════════════════════════════════════════════════════════════════════════
# Прогресс
# ═══════════════════════════════════════════════════════════════════════════

class Progress:
    def __init__(self):
        self.total   = 0
        self.saved   = 0
        self.pages   = 0
        self.started = time.time()

    def update(self, fetched: int, saved: int):
        self.total += fetched
        self.saved += saved
        self.pages += 1

    def report(self):
        elapsed = time.time() - self.started
        rps     = self.total / elapsed if elapsed > 0 else 0
        logger.info(
            f"📊 Страница {self.pages:>4} | "
            f"всего получено: {self.total:>6} | "
            f"записано/обновлено: {self.saved:>6} | "
            f"скорость: {rps:.1f} манг/с"
        )


# ═══════════════════════════════════════════════════════════════════════════
# main
# ═══════════════════════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(description="Парсинг каталога манг Senkuro")
    p.add_argument("--db",       default=DEFAULT_DB,    help="Путь к manga.db")
    p.add_argument("--order",    default=DEFAULT_ORDER, help="Поле сортировки")
    p.add_argument("--dir",      default=DEFAULT_DIR,   choices=["ASC","DESC"])
    p.add_argument("--delay",    default=DEFAULT_DELAY, type=float,
                   help="Пауза между запросами (сек)")
    p.add_argument("--batch",    default=DEFAULT_BATCH, type=int,
                   help="Размер INSERT-батча")
    p.add_argument("--update",   action="store_true",
                   help="Перезаписывать существующие записи (INSERT OR REPLACE)")
    p.add_argument("--hentai",   action="store_true",
                   help="Включить хентай (по умолчанию исключён)")
    p.add_argument("--limit",    default=0, type=int,
                   help="Остановиться после N манг (0 = без ограничения)")
    return p.parse_args()


def main():
    args = parse_args()

    logger.info("═" * 60)
    logger.info("🚀 Парсинг каталога манг Senkuro")
    logger.info(f"   DB:     {args.db}")
    logger.info(f"   Порядок: {args.order} {args.dir}")
    logger.info(f"   Задержка: {args.delay}s | Батч: {args.batch}")
    logger.info(f"   Режим:  {'REPLACE' if args.update else 'IGNORE (не перезаписывать)'}")
    logger.info(f"   Лимит: {args.limit if args.limit else 'нет'}")
    logger.info("═" * 60)

    conn = open_db(args.db)
    api  = SenkuroAPI()

    progress = Progress()
    batch: list[dict] = []

    try:
        for manga in api.fetch_all_mangas(
            order_field     = args.order,
            order_direction = args.dir,
            exclude_hentai  = not args.hentai,
            delay           = args.delay,
        ):
            batch.append(manga)
            progress.total += 1

            # Сбрасываем батч в БД
            if len(batch) >= args.batch:
                saved = flush(conn, batch, args.update)
                progress.saved += saved
                progress.pages += 1
                progress.report()
                batch.clear()

            # Ограничение по количеству
            if args.limit and progress.total >= args.limit:
                logger.info(f"⏹ Достигнут лимит {args.limit} манг")
                break

        # Финальный батч
        if batch:
            saved = flush(conn, batch, args.update)
            progress.saved += saved
            progress.pages += 1
            batch.clear()

    except KeyboardInterrupt:
        logger.info("⚠️  Прервано пользователем (Ctrl+C)")
        if batch:
            logger.info(f"   Сохраняем незакрытый батч ({len(batch)} манг)…")
            flush(conn, batch, args.update)

    finally:
        conn.close()

    elapsed = time.time() - progress.started
    logger.info("═" * 60)
    logger.info(f"✅ Готово!")
    logger.info(f"   Получено:  {progress.total} манг")
    logger.info(f"   Сохранено: {progress.saved} манг")
    logger.info(f"   Время:     {elapsed:.1f} с")
    logger.info("═" * 60)


if __name__ == "__main__":
    main()
