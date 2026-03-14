"""
config.py — конфигурация BubbleManga.

Все константы проекта в одном месте. Импортируй отсюда везде.
"""

import os

# ── Telegram ─────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN  = "7082209603:AAG97jX6MHgYOywy5hdDl03hduVMD6VBsW0"
ADMIN_TELEGRAM_IDS: list = [319026942, 649144994]

# ── Сайт ─────────────────────────────────────────────────────────────────────
SITE_URL = 'https://bubblemanga.ru:8443'

# ── База данных ───────────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get('DATABASE_URL', '')   # пусто → SQLite

# ── Redis / кеш ──────────────────────────────────────────────────────────────
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')

# ── ЮКасса (yookassa.ru → Настройки → Ключи API) ─────────────────────────────
YOOKASSA_SHOP_ID = os.environ.get('YOOKASSA_SHOP_ID', '')
YOOKASSA_SECRET  = os.environ.get('YOOKASSA_SECRET', '')

# ── Crypto Cloud (cryptocloud.plus → Проекты → API) ──────────────────────────
CRYPTOCLOUD_API_KEY    = os.environ.get('CRYPTOCLOUD_API_KEY', '')
CRYPTOCLOUD_SECRET_KEY = os.environ.get('CRYPTOCLOUD_SECRET_KEY', '')
CRYPTOCLOUD_SHOP_ID    = os.environ.get('CRYPTOCLOUD_SHOP_ID', '')

# ── Пакеты монет (Telegram Stars) ────────────────────────────────────────────
COIN_PACKAGES = [
    {'id': 'coins_100',  'coins': 100,  'stars': 15,  'rub': 129,  'usd': '1.49',  'label': '100 шариков'},
    {'id': 'coins_300',  'coins': 300,  'stars': 40,  'rub': 329,  'usd': '3.99',  'label': '300 шариков'},
    {'id': 'coins_700',  'coins': 700,  'stars': 85,  'rub': 699,  'usd': '7.99',  'label': '700 шариков'},
    {'id': 'coins_1500', 'coins': 1500, 'stars': 175, 'rub': 1399, 'usd': '15.99', 'label': '1500 шариков'},
]

# ── Пакеты Premium ────────────────────────────────────────────────────────────
PREMIUM_PACKAGES = [
    {'id': 'premium_1m',  'days': 30,  'label': 'Premium на 1 месяц',  'rub': 199,  'usd': '2.49'},
    {'id': 'premium_3m',  'days': 90,  'label': 'Premium на 3 месяца', 'rub': 499,  'usd': '5.99'},
    {'id': 'premium_12m', 'days': 365, 'label': 'Premium на 1 год',    'rub': 1499, 'usd': '17.99'},
]
