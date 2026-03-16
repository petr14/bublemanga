#!/usr/bin/env python3
"""
Сканирует static/wallpaper и static/banner, находит пары webm-файлов
(большой и маленький), обновляет shop_items и user_profile в PostgreSQL.

Логика:
  - full_url  = webm с бо́льшим разрешением  → профиль пользователя
  - preview_url остаётся _thumb.webp          → карточки в магазине/топе
  - user_profile.background_url где лежит _original.* → мигрирует на full_url
"""

import os
import re
import sys
import psycopg2
import psycopg2.extras
from pathlib import Path

# ── Настройки ────────────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://mangauser:Retpoloer2@localhost/mangadb'
)
BASE_DIR   = Path(__file__).parent / 'static'
DIRS = {
    'wallpaper': BASE_DIR / 'wallpaper',
    'banner':    BASE_DIR / 'banner',
}
STATIC_PREFIX = '/static'
DRY_RUN = '--dry-run' in sys.argv   # запусти с --dry-run чтобы только посмотреть

# ── Парсинг разрешения из имени файла ────────────────────────────────────────
RES_RE = re.compile(r'_(\d+)x(\d+)\.(webm|mp4)$', re.IGNORECASE)

def resolution(filename: str):
    """Возвращает (width*height, filename) или None если не распознать."""
    m = RES_RE.search(filename)
    if m:
        return int(m.group(1)) * int(m.group(2))
    return None

# ── Сбор файлов ──────────────────────────────────────────────────────────────
def collect_video_pairs():
    """
    Возвращает список dict:
      { 'dir': 'wallpaper'|'banner',
        'base': 'rappa',
        'large': '/static/wallpaper/rappa_1920x1080.webm',
        'small': '/static/wallpaper/rappa_320x180.webm' }
    """
    pairs = []
    for folder_name, folder_path in DIRS.items():
        if not folder_path.exists():
            print(f"  [!] Папка {folder_path} не найдена, пропускаю")
            continue

        # Группируем webm-файлы по base-имени
        groups: dict[str, list] = {}
        for f in sorted(folder_path.iterdir()):
            if f.suffix.lower() != '.webm':
                continue
            m = RES_RE.search(f.name)
            if not m:
                continue
            # base = всё до _WxH.webm
            base = f.name[:f.name.rfind('_')]
            groups.setdefault(base, []).append(f)

        for base, files in groups.items():
            if len(files) < 2:
                print(f"  [~] {folder_name}/{base}: только {len(files)} webm, пропускаю")
                continue
            # Сортируем по убыванию разрешения
            files.sort(key=lambda f: resolution(f.name) or 0, reverse=True)
            large = STATIC_PREFIX + '/' + folder_name + '/' + files[0].name
            small = STATIC_PREFIX + '/' + folder_name + '/' + files[-1].name
            pairs.append({
                'dir':  folder_name,
                'base': base,
                'large': large,
                'small': small,
            })
            print(f"  [✓] {folder_name}/{base}")
            print(f"       large = {large}")
            print(f"       small = {small}")

    return pairs

# ── Обновление БД ─────────────────────────────────────────────────────────────
def sync_db(pairs):
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    items_updated  = 0
    users_migrated = 0

    for p in pairs:
        thumb_pattern = f"/static/{p['dir']}/{p['base']}_thumb.webp"

        # 1. Найти shop_item по preview_url = _thumb.webp
        cur.execute(
            "SELECT id, name, full_url FROM shop_items WHERE preview_url = %s",
            (thumb_pattern,)
        )
        rows = cur.fetchall()
        if not rows:
            print(f"  [?] shop_item не найден по preview_url={thumb_pattern}")
            continue

        for row in rows:
            old_full = row['full_url']
            new_full = p['large']
            if old_full == new_full:
                print(f"  [=] '{row['name']}' (id={row['id']}) — full_url уже актуален")
            else:
                if not DRY_RUN:
                    cur.execute(
                        "UPDATE shop_items SET full_url = %s WHERE id = %s",
                        (new_full, row['id'])
                    )
                print(f"  [↑] '{row['name']}' (id={row['id']})")
                print(f"       {old_full or '(null)'} → {new_full}")
                items_updated += 1

        # 2. Мигрировать user_profile.background_url
        #    Старый паттерн: _original.webp / _original.jpg / старый webm другого размера
        old_patterns = [
            f"/static/{p['dir']}/{p['base']}_original.webp",
            f"/static/{p['dir']}/{p['base']}_original.jpg",
        ]
        # Также любой webm этого base (на случай другого разрешения)
        cur.execute(
            """SELECT user_id, background_url FROM user_profile
               WHERE background_url LIKE %s AND background_url LIKE '%%.webm'
                 AND background_url != %s""",
            (f"/static/{p['dir']}/{p['base']}%", p['large'])
        )
        webm_wrong = [r['user_id'] for r in cur.fetchall()]

        migrate_users = []
        for old_pat in old_patterns:
            cur.execute(
                "SELECT user_id FROM user_profile WHERE background_url = %s",
                (old_pat,)
            )
            migrate_users += [r['user_id'] for r in cur.fetchall()]
        migrate_users += webm_wrong

        if migrate_users:
            if not DRY_RUN:
                cur.execute(
                    """UPDATE user_profile SET background_url = %s
                       WHERE background_url LIKE %s
                         AND (background_url LIKE '%%_original.%%'
                           OR (background_url LIKE '%%.webm' AND background_url != %s))""",
                    (p['large'],
                     f"/static/{p['dir']}/{p['base']}%",
                     p['large'])
                )
            print(f"  [👤] user_profile мигрировано: {len(migrate_users)} пользователей "
                  f"(id: {migrate_users[:10]}{'...' if len(migrate_users)>10 else ''})")
            users_migrated += len(migrate_users)

    if not DRY_RUN:
        conn.commit()
        print(f"\n✅ Готово: shop_items обновлено={items_updated}, "
              f"user_profile мигрировано={users_migrated}")
    else:
        conn.rollback()
        print(f"\n🔍 DRY RUN: shop_items обновится={items_updated}, "
              f"user_profile мигрируется={users_migrated} (изменений нет)")

    cur.close()
    conn.close()

# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print("=" * 60)
    print("Сканирование видео-файлов…")
    print("=" * 60)
    pairs = collect_video_pairs()

    print()
    print("=" * 60)
    print(f"Обновление БД{'  [DRY RUN]' if DRY_RUN else ''}…")
    print("=" * 60)
    sync_db(pairs)
