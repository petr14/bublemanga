#!/usr/bin/env python3
"""
Заполняет description (и другие поля) для манг без описания через Senkuro API.
Использует PostgreSQL БД.
"""

import sys
import time
import json
import psycopg2
from senkuro_api import SenkuroAPI

DB_URL = "postgresql://mangauser:manga2024@localhost/mangadb"
DELAY  = 0.5   # секунд между запросами

def main():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("""
        SELECT id, manga_slug, manga_title
        FROM manga
        WHERE description IS NULL OR description = ''
        ORDER BY views DESC NULLS LAST
    """)
    rows = cur.fetchall()
    total = len(rows)
    print(f"Манг без описания: {total}")

    api = SenkuroAPI()
    ok = 0
    skip = 0
    fail = 0

    for i, (row_id, slug, title) in enumerate(rows, 1):
        print(f"[{i}/{total}] {slug} — {title}", end=" ... ", flush=True)

        data = api.fetch_manga(slug)
        if not data:
            print("❌ нет данных")
            fail += 1
            time.sleep(DELAY)
            continue

        description = data.get("description") or ""
        tags = data.get("tags", [])
        tags_json = json.dumps(tags, ensure_ascii=False) if tags else None

        cur.execute("""
            UPDATE manga SET
                description        = CASE WHEN %s != '' THEN %s ELSE description END,
                score              = COALESCE(NULLIF(%s::real, 0), score),
                rating             = COALESCE(NULLIF(%s, ''), rating),
                original_name      = COALESCE(NULLIF(%s, ''), original_name),
                manga_type         = COALESCE(NULLIF(%s, ''), manga_type),
                manga_status       = COALESCE(NULLIF(%s, ''), manga_status),
                tags               = COALESCE(NULLIF(%s, ''), tags),
                translation_status = COALESCE(NULLIF(%s, ''), translation_status),
                is_licensed        = %s,
                cover_url          = COALESCE(NULLIF(%s, ''), cover_url)
            WHERE id = %s
        """, (
            description, description,
            data.get("score") or 0,
            data.get("rating") or "",
            data.get("original_name") or "",
            data.get("manga_type") or "",
            data.get("manga_status") or "",
            tags_json or "",
            data.get("translation_status") or "",
            1 if data.get("is_licensed") else 0,
            data.get("cover_url") or "",
            row_id,
        ))
        conn.commit()

        if description:
            print(f"✅ [{description[:60]}]")
        else:
            print(f"⚠️  описания нет в API, остальные поля обновлены")
            skip += 1
        ok += 1
        time.sleep(DELAY)

    cur.close()
    conn.close()

    print(f"\nГотово: обновлено={ok}, пустое описание={skip}, ошибки={fail}")

if __name__ == "__main__":
    main()
