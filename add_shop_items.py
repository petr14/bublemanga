"""
Добавляет товары в магазин из папок static/avatar, static/banner, static/wallpaper.
Рамки из static/frame пропускаются — они уже в БД через /static/uploads/frames/.
"""
import json, os, psycopg2

DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://mangauser:Retpoloer2@localhost/mangadb')

# ── Ручной перевод wallpaper (нет images_info.json) ──────────────────────────
WALLPAPER_RU = {
    'a-dream-for-david':      ('Мечта о Дэвиде',        'Меланхоличный арт из аниме Cyberpunk: Edgerunners'),
    'david-lucyna':           ('Дэвид и Люцина',         'Романтичный арт из аниме Cyberpunk: Edgerunners'),
    'hatsune-miku-shrek':     ('Хацунэ Мику × Шрек',    'Необычный мемный коллаб'),
    'mitaka-asa':             ('Митака Аса',              'Арт из манги Chainsaw Man'),
    'rappa':                  ('Раппа',                  'Арт из манхвы Solo Leveling'),
    'shiki-ryougi':           ('Шики Рёги',              'Арт из аниме Kara no Kyoukai'),
    'tian-with-a-lollipop':   ('Тянь с леденцом',        'Милый арт аниме-персонажа'),
    'tusich':                 ('Тусич',                  'Атмосферный аниме арт'),
}

# ── Цены по категориям ────────────────────────────────────────────────────────
# Используем хэш имени файла для равномерного распределения цен
def avatar_price(slug: str) -> int:
    tiers = [500, 600, 700, 800, 900, 1000, 1200, 1500, 1800, 2000]
    return tiers[hash(slug) % len(tiers)]

def background_price(slug: str) -> int:
    tiers = [1200, 1500, 1800, 2000, 2200, 2500, 3000]
    return tiers[hash(slug) % len(tiers)]

def description_for_avatar(title_ru: str, title_en: str) -> str:
    return f'Аватар «{title_ru}»'

def description_for_background(title_ru: str, folder: str) -> str:
    kind = 'Баннер' if folder == 'banner' else 'Обои'
    return f'{kind} «{title_ru}»'


def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    inserted = 0

    # ── Аватары ──────────────────────────────────────────────────────────────
    with open('static/avatar/images_info.json') as f:
        avatar_info = json.load(f)

    for item in avatar_info:
        if '_original.' not in item['filename']:
            continue
        slug  = item['filename'].split('_original.')[0]
        title = item.get('title_ru') or item.get('title_en') or slug.replace('-', ' ').title()
        desc  = description_for_avatar(title, item.get('title_en', ''))
        url   = f"/static/avatar/{item['filename']}"
        price = avatar_price(slug)

        cur.execute("""
            INSERT INTO shop_items (name, description, type, preview_url, css_value, price, is_upload)
            VALUES (%s, %s, 'avatar', %s, '', %s, 0)
            ON CONFLICT DO NOTHING
        """, (title, desc, url, price))
        if cur.rowcount:
            inserted += 1
            print(f"[avatar] {title} — {price} шар. → {url}")

    # ── Баннеры (тип background) ─────────────────────────────────────────────
    with open('static/banner/images_info.json') as f:
        banner_info = json.load(f)

    for item in banner_info:
        if '_original.' not in item['filename']:
            continue
        slug  = item['filename'].split('_original.')[0]
        title = item.get('title_ru') or item.get('title_en') or slug.replace('-', ' ').title()
        desc  = description_for_background(title, 'banner')
        url   = f"/static/banner/{item['filename']}"
        price = background_price(slug)

        cur.execute("""
            INSERT INTO shop_items (name, description, type, preview_url, css_value, price, is_upload)
            VALUES (%s, %s, 'background', %s, '', %s, 0)
            ON CONFLICT DO NOTHING
        """, (title, desc, url, price))
        if cur.rowcount:
            inserted += 1
            print(f"[banner] {title} — {price} шар. → {url}")

    # ── Обои (тип background) ────────────────────────────────────────────────
    import re
    for fname in sorted(os.listdir('static/wallpaper')):
        if '_original.' not in fname:
            continue
        slug = fname.split('_original.')[0]
        if slug in WALLPAPER_RU:
            title, desc = WALLPAPER_RU[slug]
        else:
            title = slug.replace('-', ' ').title()
            desc  = description_for_background(title, 'wallpaper')
        url   = f"/static/wallpaper/{fname}"
        price = background_price(slug)

        cur.execute("""
            INSERT INTO shop_items (name, description, type, preview_url, css_value, price, is_upload)
            VALUES (%s, %s, 'background', %s, '', %s, 0)
            ON CONFLICT DO NOTHING
        """, (title, desc, url, price))
        if cur.rowcount:
            inserted += 1
            print(f"[wallpaper] {title} — {price} шар. → {url}")

    conn.commit()
    conn.close()
    print(f"\nГотово! Добавлено {inserted} новых товаров.")


if __name__ == '__main__':
    main()
