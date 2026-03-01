# -*- coding: utf-8 -*-
"""
Standalone script to populate the `chapters` table for all manga in manga.db.

Usage examples:
    python parse_all_chapters.py
    python parse_all_chapters.py --skip-existing --limit 100
    python parse_all_chapters.py --only-missing --delay 1.0
    python parse_all_chapters.py --resume some-manga-id
"""

import argparse
import logging
import os
import sqlite3
import sys
import time

# Allow importing senkuro_api from the same directory
sys.path.insert(0, os.path.dirname(__file__))
from senkuro_api import SenkuroAPI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── DB helpers ────────────────────────────────────────────────────────────────

def open_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS chapters (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            manga_id        TEXT NOT NULL,
            chapter_id      TEXT UNIQUE NOT NULL,
            chapter_slug    TEXT NOT NULL,
            chapter_number  TEXT,
            chapter_volume  TEXT,
            chapter_name    TEXT,
            chapter_url     TEXT,
            pages_json      TEXT,
            pages_count     INTEGER DEFAULT 0,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (manga_id) REFERENCES manga(manga_id)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_chapters_manga_number
        ON chapters(manga_id, chapter_number)
    """)
    conn.commit()
    return conn


def has_chapters(conn, manga_id):
    row = conn.execute(
        "SELECT 1 FROM chapters WHERE manga_id = ? LIMIT 1", (manga_id,)
    ).fetchone()
    return row is not None


def save_chapters(conn, chapters, manga_id):
    if not chapters:
        return 0
    rows = [
        (
            manga_id,
            ch["chapter_id"],
            ch["chapter_slug"],
            ch["chapter_number"],
            ch["chapter_volume"],
            ch["chapter_name"],
            ch["chapter_url"],
            ch["created_at"],
        )
        for ch in chapters
    ]
    conn.executemany(
        """INSERT OR IGNORE INTO chapters
           (manga_id, chapter_id, chapter_slug, chapter_number,
            chapter_volume, chapter_name, chapter_url, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.execute(
        "UPDATE manga SET chapters_count = ? WHERE manga_id = ?",
        (len(chapters), manga_id),
    )
    conn.commit()
    return len(chapters)


# ── Chapter fetching ──────────────────────────────────────────────────────────

def paginate_all_chapters(api, branch_id, manga_id, manga_slug, page_delay):
    chapters = []
    after = None
    page_num = 0

    while True:
        page_num += 1
        result = api.fetch_manga_chapters_page(branch_id, after)
        if not result:
            logger.warning(f"  empty response on page {page_num} for {manga_slug}")
            break

        page_info = result.get("pageInfo", {})
        edges = result.get("edges", [])

        for edge in edges:
            node = edge.get("node") or {}
            if not node:
                continue
            chapters.append({
                "chapter_id":     node.get("id"),
                "chapter_slug":   node.get("slug"),
                "chapter_number": node.get("number"),
                "chapter_volume": node.get("volume"),
                "chapter_name":   node.get("name"),
                "created_at":     node.get("createdAt"),
                "chapter_url":    f"/read/{manga_slug}/{node.get('slug')}",
            })

        has_next = page_info.get("hasNextPage", False)
        after = page_info.get("endCursor")

        logger.debug(f"  page {page_num}: {len(edges)} chapters, hasNextPage={has_next}")

        if not has_next or not after:
            break
        if page_delay > 0:
            time.sleep(page_delay)

    return chapters


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_db = os.path.join(script_dir, "manga.db")

    p = argparse.ArgumentParser(description="Populate chapters table for all manga in DB")
    p.add_argument("--db",            default=default_db, help="Path to manga.db")
    p.add_argument("--delay",         type=float, default=0.5,
                   help="Seconds between requests per manga (default 0.5)")
    p.add_argument("--page-delay",    type=float, default=0.2,
                   help="Seconds between chapter pages within one manga (default 0.2)")
    p.add_argument("--skip-existing", action="store_true",
                   help="Skip manga that already have chapters in DB")
    p.add_argument("--only-missing",  action="store_true",
                   help="Only process manga with chapters_count = 0 in manga table")
    p.add_argument("--limit",         type=int, default=0,
                   help="Stop after N manga processed (0 = no limit)")
    p.add_argument("--resume",        default=None,
                   help="Start from this manga_id (skip all rows before it in ordered list)")
    return p.parse_args()


def main():
    args = parse_args()

    if not os.path.exists(args.db):
        logger.error(f"DB not found: {args.db}")
        sys.exit(1)

    conn = open_db(args.db)
    api = SenkuroAPI()

    # Build query
    if args.only_missing:
        rows = conn.execute(
            "SELECT manga_id, manga_slug, branch_id FROM manga WHERE chapters_count = 0 ORDER BY id"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT manga_id, manga_slug, branch_id FROM manga ORDER BY id"
        ).fetchall()

    total_manga = len(rows)
    logger.info(f"Found {total_manga} manga rows to process")

    # Apply --resume: skip until we see the given manga_id
    if args.resume:
        ids = [r["manga_id"] for r in rows]
        if args.resume in ids:
            start = ids.index(args.resume)
            rows = rows[start:]
            logger.info(f"Resuming from manga_id={args.resume} ({len(rows)} remaining)")
        else:
            logger.warning(f"--resume manga_id={args.resume!r} not found, starting from beginning")

    processed = 0
    total_chapters_saved = 0
    start_time = time.time()

    for idx, row in enumerate(rows, 1):
        manga_id   = row["manga_id"]
        manga_slug = row["manga_slug"]
        branch_id  = row["branch_id"]

        # --skip-existing
        if args.skip_existing and has_chapters(conn, manga_id):
            logger.info(f"[{idx}/{len(rows)}] {manga_slug} — skipped (already has chapters)")
            continue

        # Resolve branch_id if missing
        if not branch_id:
            logger.info(f"[{idx}/{len(rows)}] {manga_slug} — fetching branch_id from API...")
            details = api.fetch_manga(manga_slug)
            if not details:
                logger.warning(f"  Could not fetch details for {manga_slug}, skipping")
                continue
            branch_id = details.get("branch_id")
            if branch_id:
                conn.execute(
                    "UPDATE manga SET branch_id = ? WHERE manga_id = ?",
                    (branch_id, manga_id),
                )
                conn.commit()

        if not branch_id:
            logger.warning(f"[{idx}/{len(rows)}] {manga_slug} — no branch_id, skipping")
            continue

        # Fetch all chapters
        chapters = paginate_all_chapters(
            api, branch_id, manga_id, manga_slug, args.page_delay
        )
        saved = save_chapters(conn, chapters, manga_id)
        total_chapters_saved += saved
        processed += 1

        logger.info(f"[{idx}/{len(rows)}] {manga_slug} — {saved} chapters saved")

        if args.limit and processed >= args.limit:
            logger.info(f"Reached --limit {args.limit}, stopping")
            break

        if args.delay > 0:
            time.sleep(args.delay)

    conn.close()

    elapsed = time.time() - start_time
    logger.info(
        f"\nDone. Processed {processed} manga, "
        f"{total_chapters_saved} chapters saved, "
        f"elapsed {elapsed:.1f}s"
    )


if __name__ == "__main__":
    main()
