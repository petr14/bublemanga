#!/usr/bin/env python3
"""
mangabuff_sync.py — синхронизация глав ManGaBuff с основной БД.

Использование:
  python mangabuff_sync.py --slug one-piece --mb-slug one-pisu
      Задать маппинг и синхронизировать одну мангу.

  python mangabuff_sync.py --slug one-piece
      Синхронизировать мангу по уже сохранённому маппингу.

  python mangabuff_sync.py --limit 50
      Синхронизировать все манги у которых есть маппинг (первые 50).

  python mangabuff_sync.py
      Синхронизировать все манги у которых есть маппинг.

  python mangabuff_sync.py --list-mapped
      Показать все сохранённые маппинги.

  python mangabuff_sync.py --refetch-pages
      Пере-скачать страницы даже для уже синхронизированных глав.
"""

import argparse
import json
import logging
import sys
import time

from database import get_db, _USE_PG
from mangabuff_api import MangaBuffAPI

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://mangabuff.ru"


# ─── DB helpers ─────────────────────────────────────────────────────────────

def ensure_tables(conn):
    from database import _USE_PG
    if _USE_PG:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS mangabuff_chapters (
                id             SERIAL PRIMARY KEY,
                manga_slug     TEXT    NOT NULL,
                chapter_id     TEXT,
                mb_slug        TEXT    NOT NULL,
                chapter_number TEXT    DEFAULT '',
                chapter_volume TEXT    DEFAULT '',
                chapter_name   TEXT    DEFAULT '',
                chapter_url    TEXT    DEFAULT '',
                pages_json     TEXT,
                pages_count    INTEGER DEFAULT 0,
                synced_at      TIMESTAMP DEFAULT NOW(),
                UNIQUE(manga_slug, mb_slug)
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS manga_sources (
                manga_slug   TEXT    NOT NULL,
                source       TEXT    NOT NULL,
                source_slug  TEXT    NOT NULL,
                updated_at   TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (manga_slug, source)
            )
        ''')
    else:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS mangabuff_chapters (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                manga_slug     TEXT    NOT NULL,
                chapter_id     TEXT,
                mb_slug        TEXT    NOT NULL,
                chapter_number TEXT    DEFAULT '',
                chapter_volume TEXT    DEFAULT '',
                chapter_name   TEXT    DEFAULT '',
                chapter_url    TEXT    DEFAULT '',
                pages_json     TEXT,
                pages_count    INTEGER DEFAULT 0,
                synced_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(manga_slug, mb_slug)
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS manga_sources (
                manga_slug   TEXT    NOT NULL,
                source       TEXT    NOT NULL,
                source_slug  TEXT    NOT NULL,
                updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (manga_slug, source)
            )
        ''')
    try:
        conn.commit()
    except Exception:
        pass
    logger.info("✅ Таблицы готовы")


def set_mapping(conn, manga_slug: str, mb_slug: str):
    conn.execute(
        '''INSERT OR REPLACE INTO manga_sources (manga_slug, source, source_slug)
           VALUES (?, 'mangabuff', ?)''',
        (manga_slug, mb_slug)
    )
    conn.commit()
    logger.info(f"  Маппинг сохранён: {manga_slug} → MangaBuff/{mb_slug}")


def get_mapping(conn, manga_slug: str) -> str | None:
    row = conn.execute(
        "SELECT source_slug FROM manga_sources WHERE manga_slug=? AND source='mangabuff'",
        (manga_slug,)
    ).fetchone()
    return row['source_slug'] if row else None


def get_mangas_with_mapping(conn, slug: str = None, limit: int = None):
    if slug:
        rows = conn.execute(
            '''SELECT m.manga_slug, m.manga_title, ms.source_slug as mb_slug
               FROM manga m
               JOIN manga_sources ms ON ms.manga_slug = m.manga_slug AND ms.source='mangabuff'
               WHERE m.manga_slug = ?''',
            (slug,)
        ).fetchall()
    elif limit:
        rows = conn.execute(
            '''SELECT m.manga_slug, m.manga_title, ms.source_slug as mb_slug
               FROM manga m
               JOIN manga_sources ms ON ms.manga_slug = m.manga_slug AND ms.source='mangabuff'
               ORDER BY m.last_updated DESC LIMIT ?''',
            (limit,)
        ).fetchall()
    else:
        rows = conn.execute(
            '''SELECT m.manga_slug, m.manga_title, ms.source_slug as mb_slug
               FROM manga m
               JOIN manga_sources ms ON ms.manga_slug = m.manga_slug AND ms.source='mangabuff'
               ORDER BY m.last_updated DESC'''
        ).fetchall()
    return [dict(r) for r in rows]


def get_chapters_map(conn, manga_slug: str) -> dict:
    """Вернуть {chapter_number: chapter_id} из основной таблицы chapters."""
    rows = conn.execute(
        '''SELECT c.chapter_id, c.chapter_number FROM chapters c
           JOIN manga m ON c.manga_id = m.manga_id
           WHERE m.manga_slug = ?''',
        (manga_slug,)
    ).fetchall()
    result = {}
    for r in rows:
        num = (r["chapter_number"] or "").strip()
        if num:
            result[num] = r["chapter_id"]
    return result


def already_synced(conn, manga_slug: str, mb_slug: str) -> bool:
    row = conn.execute(
        "SELECT pages_count FROM mangabuff_chapters WHERE manga_slug=? AND mb_slug=?",
        (manga_slug, mb_slug)
    ).fetchone()
    return bool(row and row["pages_count"] and row["pages_count"] > 0)


def save_chapter(conn, manga_slug: str, chapter_id, ch: dict, pages: list):
    pages_json = json.dumps(pages) if pages else None
    conn.execute(
        '''INSERT OR REPLACE INTO mangabuff_chapters
               (manga_slug, chapter_id, mb_slug, chapter_number, chapter_volume,
                chapter_name, chapter_url, pages_json, pages_count, synced_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))''',
        (manga_slug, chapter_id, ch["uniq"],
         ch.get("number", ""), ch.get("volume", ""),
         ch.get("title", ""), ch.get("link", ""),
         pages_json, len(pages))
    )
    conn.commit()


# ─── Sync logic ─────────────────────────────────────────────────────────────

def sync_manga(api: MangaBuffAPI, conn, manga_slug: str, manga_title: str,
               mb_slug: str, refetch_pages: bool = False):
    logger.info(f"→ {manga_slug}  ({manga_title})  MangaBuff: {mb_slug}")
    url = f"{BASE_URL}/manga/{mb_slug}"
    details = api.get_manga_details(url)
    if not details or not details.chapters:
        logger.warning(f"  Нет глав на MangaBuff по slug '{mb_slug}'")
        return 0, 0

    chapters_map = get_chapters_map(conn, manga_slug)
    synced = skipped = 0

    for ch in details.chapters:
        ch_mb_slug = ch["uniq"]

        if not refetch_pages and already_synced(conn, manga_slug, ch_mb_slug):
            skipped += 1
            continue

        num = (ch.get("number") or "").strip()
        chapter_id = chapters_map.get(num)

        pages_obj = api.get_chapter_pages(ch["link"])
        pages = pages_obj.pages

        save_chapter(conn, manga_slug, chapter_id, ch, pages)
        synced += 1
        logger.info(f"  ✓ {ch_mb_slug}  гл.{num}  {len(pages)} стр."
                    f"{'  → ' + chapter_id if chapter_id else '  (нет совпадения)'}")

        time.sleep(0.3)

    return synced, skipped


def list_mapped(conn):
    rows = conn.execute(
        '''SELECT ms.manga_slug, ms.source_slug, m.manga_title,
                  COUNT(mc.id) as synced_chapters
           FROM manga_sources ms
           JOIN manga m ON m.manga_slug = ms.manga_slug
           LEFT JOIN mangabuff_chapters mc ON mc.manga_slug = ms.manga_slug
           WHERE ms.source = 'mangabuff'
           GROUP BY ms.manga_slug, ms.source_slug, m.manga_title
           ORDER BY m.manga_title'''
    ).fetchall()
    if not rows:
        print("Нет сохранённых маппингов.")
        return
    print(f"{'Slug':<40} {'MB-Slug':<35} {'Названиe':<35} {'Глав'}")
    print("-" * 115)
    for r in rows:
        print(f"{r['manga_slug']:<40} {r['source_slug']:<35} {r['manga_title'][:34]:<35} {r['synced_chapters']}")


# ─── Entry point ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Синхронизация MangaBuff → БД")
    parser.add_argument("--slug",    help="Слаг манги в основной БД (senkuro slug)")
    parser.add_argument("--mb-slug", help="Слаг манги на MangaBuff (если отличается от slug)")
    parser.add_argument("--limit",   type=int, help="Ограничить количество манг")
    parser.add_argument("--list-mapped",  action="store_true", help="Показать маппинги")
    parser.add_argument("--refetch-pages", action="store_true",
                        help="Пере-скачать страницы даже для синхронизированных глав")
    args = parser.parse_args()

    conn = get_db()
    ensure_tables(conn)

    if args.list_mapped:
        list_mapped(conn)
        conn.close()
        return

    # Если указан --mb-slug — сохраняем/обновляем маппинг
    if args.mb_slug:
        if not args.slug:
            logger.error("--mb-slug требует --slug")
            sys.exit(1)
        set_mapping(conn, args.slug, args.mb_slug)

    mangas = get_mangas_with_mapping(conn, slug=args.slug, limit=args.limit)
    if not mangas:
        if args.slug:
            logger.error(f"Нет маппинга для '{args.slug}'. "
                         f"Задайте его: --slug {args.slug} --mb-slug <mangabuff-slug>")
        else:
            logger.info("Нет манг с маппингом MangaBuff. "
                        "Добавьте: --slug <senkuro-slug> --mb-slug <mangabuff-slug>")
        conn.close()
        sys.exit(0)

    logger.info(f"Манг для синхронизации: {len(mangas)}")
    api = MangaBuffAPI()

    total_synced = total_skipped = 0
    for i, m in enumerate(mangas, 1):
        logger.info(f"[{i}/{len(mangas)}]")
        s, sk = sync_manga(api, conn, m["manga_slug"], m["manga_title"],
                           m["mb_slug"], args.refetch_pages)
        total_synced  += s
        total_skipped += sk
        time.sleep(0.5)

    conn.close()
    logger.info(f"\n✅ Готово. Синхронизировано: {total_synced}, пропущено: {total_skipped}")


if __name__ == "__main__":
    main()
