#!/usr/bin/env python3
"""
Парсит рамки с Senkuro API и загружает их в BubbleManga.

Запуск:
    DATABASE_URL=postgresql://mangauser:Retpoloer2@localhost/mangadb python scrape_senkuro_frames.py
"""
import os
import sys
import time
import json
import hashlib
import requests
import psycopg2

API_URL    = 'https://api.senkuro.com/graphql'
IMAGES_DIR = '/var/tgbot/manga/static/uploads/frames'
DB_URL     = os.environ.get('DATABASE_URL', 'postgresql://mangauser:Retpoloer2@localhost/mangadb')
HASH       = 'e0035214ce75614fa374dc898b1f73df98868783f02ffb9803972d2b6e2a8b72'

HEADERS = {
    'Content-Type': 'application/json',
    'Origin': 'https://senkuro.com',
    'Referer': 'https://senkuro.com/',
    'User-Agent': 'Mozilla/5.0 (compatible; BubbleManga/1.0)',
}

os.makedirs(IMAGES_DIR, exist_ok=True)


def fetch_page(after=None):
    payload = {
        'operationName': 'fetchCollectibles',
        'variables': {
            'after': after,
            'excludePurchased': False,
            'first': 50,
            'onlyLimited': False,
            'onlyWithSubscription': False,
            'orderBy': {'direction': 'DESC', 'field': 'CREATED_AT'},
            'priceGroup': None,
            'rating': {'exclude': [], 'include': []},
            'type': 'FRAME',
            'visible': True,
        },
        'extensions': {'persistedQuery': {'sha256Hash': HASH, 'version': 1}},
    }
    r = requests.post(API_URL, json=payload, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    if 'errors' in data:
        raise RuntimeError(f"GraphQL errors: {data['errors']}")
    collectibles = data['data']['collectibles']
    return collectibles['edges'], collectibles['pageInfo']


def download_image(url: str, slug: str) -> str | None:
    """Скачивает WebP в IMAGES_DIR, возвращает URL для БД."""
    ext = '.webp' if '.webp' in url else '.png'
    filename = f"{slug}{ext}"
    local_path = os.path.join(IMAGES_DIR, filename)
    if os.path.exists(local_path):
        return f'/static/uploads/frames/{filename}'
    try:
        r = requests.get(url, timeout=30, headers={'User-Agent': HEADERS['User-Agent']})
        r.raise_for_status()
        with open(local_path, 'wb') as f:
            f.write(r.content)
        size_kb = len(r.content) // 1024
        print(f"    ↓ {filename} ({size_kb} KB)")
        return f'/static/uploads/frames/{filename}'
    except Exception as e:
        print(f"    ⚠️  Не удалось скачать {url}: {e}")
        return None


def get_ru_title(titles):
    for t in titles:
        if t.get('lang') == 'RU':
            return t['content']
    for t in titles:
        if t.get('lang') == 'EN':
            return t['content']
    return 'Рамка'


def insert_frame(conn, name: str, preview_url: str, is_animated: int, price: int, slug: str):
    """Вставляет рамку в shop_items если её ещё нет (проверяем по preview_url)."""
    cur = conn.cursor()
    cur.execute("SELECT id FROM shop_items WHERE preview_url = %s", (preview_url,))
    if cur.fetchone():
        cur.close()
        return False  # уже есть
    cur.execute(
        """INSERT INTO shop_items
               (name, description, type, preview_url, css_value, price, is_upload, is_animated)
           VALUES (%s, %s, 'frame', %s, NULL, %s, 0, %s)""",
        (name, f'Рамка «{name}» с Senkuro', preview_url, price, is_animated)
    )
    conn.commit()
    cur.close()
    return True


def main():
    conn = psycopg2.connect(DB_URL)

    total_fetched = 0
    total_new = 0
    total_skipped = 0
    after = None

    print("🎨 Парсинг рамок с Senkuro...\n")

    while True:
        edges, page_info = fetch_page(after)
        if not edges:
            break

        for edge in edges:
            node = edge['node']
            slug = node['slug']
            name = get_ru_title(node.get('titles', []))
            price = node.get('price', 0)
            img = node.get('image', {})
            img_url = img.get('original', {}).get('url', '')
            is_animated = 1 if img.get('animation') else 0

            if not img_url:
                print(f"  ⚠️  {slug}: нет URL изображения, пропуск")
                total_skipped += 1
                continue

            total_fetched += 1
            print(f"  [{total_fetched}] {name} ({slug})")

            local_url = download_image(img_url, slug)
            if not local_url:
                total_skipped += 1
                continue

            added = insert_frame(conn, name, local_url, is_animated, price, slug)
            if added:
                total_new += 1
                anim_mark = ' 🎞' if is_animated else ''
                print(f"       ✅ Добавлено: {name}{anim_mark}, цена={price}")
            else:
                print(f"       · Уже есть: {name}")

            time.sleep(0.1)  # небольшая пауза

        if not page_info.get('hasNextPage'):
            break
        after = page_info['endCursor']
        print(f"\n  → Следующая страница (after={after})...\n")
        time.sleep(0.5)

    conn.close()

    print(f"\n{'='*50}")
    print(f"  Всего обработано : {total_fetched}")
    print(f"  Новых добавлено  : {total_new}")
    print(f"  Пропущено/уже есть: {total_fetched - total_new}")
    print(f"{'='*50}")


if __name__ == '__main__':
    main()
