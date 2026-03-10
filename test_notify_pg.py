"""
Прямой тест Telegram-уведомлений (PostgreSQL).
Отправляет сообщение напрямую через Bot API без polling.
"""
import asyncio, os
import psycopg2
import psycopg2.extras
from telegram import Bot

BOT_TOKEN = '7082209603:AAG97jX6MHgYOywy5hdDl03hduVMD6VBsW0'
MANGA_ID  = 'TUFOR0E6NjYxNjgyNjkwNjQ2NTkzNjI'
SITE_URL  = 'https://bubblemanga.ru'

async def main():
    conn = psycopg2.connect('postgresql://mangauser:Retpoloer2@localhost/mangadb',
                            cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()

    # Находим подписчиков с уведомлениями
    cur.execute('''
        SELECT s.user_id, u.telegram_id, u.is_premium, u.telegram_username
        FROM subscriptions s
        JOIN users u ON s.user_id = u.id
        WHERE s.manga_id = %s
          AND u.is_active IS NOT FALSE
          AND u.notifications_enabled IS NOT FALSE
          AND u.telegram_id IS NOT NULL
    ''', (MANGA_ID,))
    subs = cur.fetchall()
    conn.close()

    if not subs:
        print('❌ Подписчиков не найдено!')
        return

    print(f'✅ Найдено подписчиков: {len(subs)}')
    for s in subs:
        print(f'  - user_id={s["user_id"]} @{s["telegram_username"]} premium={s["is_premium"]}')

    bot = Bot(token=BOT_TOKEN)
    chapter_url = f'{SITE_URL}/read/wo-you-yizuo-wei-shi-dixiacheng/test-chapter-999'

    message = (
        '🆕 <b>Новая глава!</b>\n\n'
        '📖 <b>Поднятие уровня в подземелье, после конца света</b>\n'
        'Глава: 999 (Том 1)\n'
        'Тест уведомлений\n\n'
        f'🔗 <a href="{chapter_url}">Читать на сайте</a>'
    )

    for sub in subs:
        try:
            await bot.send_message(
                chat_id=sub['telegram_id'],
                text=message,
                parse_mode='HTML'
            )
            print(f'  ✅ Отправлено @{sub["telegram_username"]} ({sub["telegram_id"]})')
        except Exception as e:
            print(f'  ❌ Ошибка для {sub["telegram_id"]}: {e}')

asyncio.run(main())
