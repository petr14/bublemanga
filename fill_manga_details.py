# -*- coding: utf-8 -*-
"""
Дозаполнение деталей манг: описание, жанры, обложка, счёт и т.д.

Находит в БД манги у которых отсутствует описание или жанры,
запрашивает полные данные через SenkuroAPI.fetch_manga() и обновляет БД.

Использование:
    python fill_manga_details.py [--db manga.db] [--delay 0.6] [--limit N]
                                 [--force] [--slug some-slug] [--workers N]

Флаги:
    --db       путь к manga.db (по умолчанию рядом со скриптом)
    --delay    пауза между запросами к API (сек, по умолчанию 0.6)
    --limit    остановиться после N обновлений (0 = без ограничения)
    --force    обновить ВСЕ манги, даже у которых уже есть данные
    --slug     обновить одну конкретную мангу по slug
    --workers  число параллельных потоков (по умолчанию 1; ≤4 безопасно)
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from senkuro_api import SenkuroAPI

# ── Логирование ────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("fill_manga_details")

DEFAULT_DB    = os.path.join(BASE_DIR, "manga.db")
DEFAULT_DELAY = 0.6


# ── БД ─────────────────────────────────────────────────────────────────────

def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA cache_size=-32000")
    conn.row_factory = sqlite3.Row
    return conn


def get_incomplete(conn: sqlite3.Connection, force: bool) -> list[dict]:
    """Вернуть список манг которым нужно дозаполнение."""
    if force:
        sql = "SELECT manga_id, manga_slug, manga_title FROM manga ORDER BY manga_title"
    else:
        sql = """
            SELECT manga_id, manga_slug, manga_title FROM manga
            WHERE
                (description IS NULL OR description = '' OR TRIM(description) = '')
                OR (tags IS NULL OR tags = '' OR tags = '[]' OR TRIM(tags) = '[]')
            ORDER BY manga_title
        """
    rows = conn.execute(sql).fetchall()
    return [dict(r) for r in rows]


UPDATE_SQL = """
    UPDATE manga SET
        description        = ?,
        tags               = ?,
        score              = ?,
        cover_url          = CASE WHEN ? != '' THEN ? ELSE cover_url END,
        manga_type         = CASE WHEN ? != '' THEN ? ELSE manga_type END,
        manga_status       = CASE WHEN ? != '' THEN ? ELSE manga_status END,
        chapters_count     = CASE WHEN ? > 0   THEN ? ELSE chapters_count END,
        original_name      = CASE WHEN ? != '' THEN ? ELSE original_name END,
        translation_status = CASE WHEN ? != '' THEN ? ELSE translation_status END,
        branch_id          = CASE WHEN ? != '' THEN ? ELSE branch_id END
    WHERE manga_slug = ?
"""


def apply_update(conn: sqlite3.Connection, lock: Lock, slug: str, d: dict) -> None:
    tags_json = json.dumps(d.get("tags") or [], ensure_ascii=False)
    cover     = d.get("cover_url") or ""
    mtype     = d.get("manga_type") or ""
    status    = d.get("manga_status") or ""
    orig      = d.get("original_name") or ""
    tr_status = d.get("translation_status") or ""
    branch    = str(d.get("branch_id") or "")
    chapters  = int(d.get("chapters_count") or 0)
    score     = float(d.get("score") or 0)
    desc      = d.get("description") or ""

    params = (
        desc,
        tags_json,
        score,
        cover, cover,
        mtype, mtype,
        status, status,
        chapters, chapters,
        orig, orig,
        tr_status, tr_status,
        branch, branch,
        slug,
    )
    with lock:
        conn.execute(UPDATE_SQL, params)
        conn.commit()


# ── Воркер ─────────────────────────────────────────────────────────────────

def process_one(api: SenkuroAPI, conn: sqlite3.Connection, lock: Lock,
                manga: dict, delay: float) -> str:
    """
    Запросить детали одной манги и обновить БД.
    Возвращает строку-статус для лога.
    """
    slug  = manga["manga_slug"]
    title = manga["manga_title"]

    try:
        data = api.fetch_manga(slug)
        if not data:
            return f"⚠️  не найдена в API: {slug}"

        apply_update(conn, lock, slug, data)

        tags_count = len(data.get("tags") or [])
        has_desc   = bool((data.get("description") or "").strip())
        return (
            f"✅ {title[:40]:<40} | "
            f"desc={'✓' if has_desc else '—'} | "
            f"tags={tags_count:>2} | "
            f"score={data.get('score', 0):.1f}"
        )

    except Exception as e:
        return f"❌ ошибка {slug}: {e}"
    finally:
        if delay > 0:
            time.sleep(delay)


# ── Аргументы ──────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Дозаполнение деталей манг")
    p.add_argument("--db",      default=DEFAULT_DB,   help="Путь к manga.db")
    p.add_argument("--delay",   default=DEFAULT_DELAY, type=float,
                   help="Пауза между запросами (сек)")
    p.add_argument("--limit",   default=0, type=int,
                   help="Обновить не более N манг (0 = все)")
    p.add_argument("--force",   action="store_true",
                   help="Обновить все манги, даже с уже заполненными данными")
    p.add_argument("--slug",    default="",
                   help="Обновить одну конкретную мангу по slug")
    p.add_argument("--workers", default=1, type=int,
                   help="Число параллельных потоков (рекомендуется ≤4)")
    return p.parse_args()


# ── main ───────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    logger.info("═" * 65)
    logger.info("🔍 fill_manga_details — дозаполнение описаний и жанров")
    logger.info(f"   DB:      {args.db}")
    logger.info(f"   Задержка: {args.delay}s | Воркеры: {args.workers}")
    logger.info(f"   Режим:   {'FORCE (все манги)' if args.force else 'только пустые'}")
    if args.slug:
        logger.info(f"   Slug:    {args.slug}")
    if args.limit:
        logger.info(f"   Лимит:   {args.limit}")
    logger.info("═" * 65)

    conn = open_db(args.db)
    lock = Lock()
    api  = SenkuroAPI()

    # Список манг для обработки
    if args.slug:
        row = conn.execute(
            "SELECT manga_id, manga_slug, manga_title FROM manga WHERE manga_slug = ?",
            (args.slug,)
        ).fetchone()
        if not row:
            logger.error(f"Манга с slug '{args.slug}' не найдена в БД")
            conn.close()
            sys.exit(1)
        mangas = [dict(row)]
    else:
        mangas = get_incomplete(conn, args.force)

    if not mangas:
        logger.info("✅ Все манги уже имеют описания и жанры. Нечего обновлять.")
        conn.close()
        return

    if args.limit:
        mangas = mangas[:args.limit]

    total = len(mangas)
    logger.info(f"📋 Манг к обновлению: {total}")

    done    = 0
    errors  = 0
    started = time.time()

    # Однопоточный режим
    if args.workers <= 1:
        for manga in mangas:
            status = process_one(api, conn, lock, manga, args.delay)
            done += 1
            is_err = status.startswith("❌") or status.startswith("⚠️")
            if is_err:
                errors += 1
            logger.info(f"[{done:>5}/{total}] {status}")

            if done % 50 == 0:
                elapsed = time.time() - started
                rps = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rps if rps > 0 else 0
                logger.info(
                    f"📊 Прогресс: {done}/{total} | "
                    f"ошибок: {errors} | "
                    f"скорость: {rps:.2f}/с | "
                    f"осталось: {eta/60:.1f} мин"
                )

    # Многопоточный режим
    else:
        workers = min(args.workers, 4)
        logger.info(f"🧵 Запуск с {workers} потоками (delay={args.delay}s на поток)")

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(process_one, api, conn, lock, m, args.delay): m
                for m in mangas
            }
            for fut in as_completed(futures):
                status = fut.result()
                done += 1
                is_err = status.startswith("❌") or status.startswith("⚠️")
                if is_err:
                    errors += 1
                logger.info(f"[{done:>5}/{total}] {status}")

                if done % 50 == 0:
                    elapsed = time.time() - started
                    rps = done / elapsed if elapsed > 0 else 0
                    eta = (total - done) / rps if rps > 0 else 0
                    logger.info(
                        f"📊 Прогресс: {done}/{total} | "
                        f"ошибок: {errors} | "
                        f"скорость: {rps:.2f}/с | "
                        f"осталось: {eta/60:.1f} мин"
                    )

    conn.close()
    elapsed = time.time() - started
    logger.info("═" * 65)
    logger.info(f"✅ Готово! Обновлено: {done - errors}/{total} | "
                f"ошибок: {errors} | время: {elapsed:.1f}с")
    logger.info("═" * 65)


if __name__ == "__main__":
    main()
