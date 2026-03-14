"""
bot.py — Telegram-бот для BubbleManga.

Содержит:
  - Глобалы telegram_app / _bot_loop
  - send_telegram_notification(), send_daily_digest()
  - _revoke_premium_loans()
  - Все async-хендлеры команд и callback-кнопок
  - run_telegram_bot()

Зависимости из main.py импортируются лениво (внутри тел функций),
чтобы не было кругового импорта: main → bot → main.
"""

import asyncio
from config import (
    TELEGRAM_BOT_TOKEN, ADMIN_TELEGRAM_IDS, SITE_URL,
    COIN_PACKAGES, PREMIUM_PACKAGES,
)
import logging
import threading
from datetime import datetime, timedelta

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    LabeledPrice, WebAppInfo,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    PreCheckoutQueryHandler,
    filters,
)

from database import get_db

logger = logging.getLogger(__name__)

# ── Глобальные объекты бота ──────────────────────────────────────────────────
telegram_app = None
_bot_loop = None   # event loop потока Telegram-бота (для run_coroutine_threadsafe)


# ==================== УВЕДОМЛЕНИЯ ====================

async def send_telegram_notification(user_id, manga_title, chapter_info, chapter_url):
    """Отправка мгновенного уведомления через Telegram (только Premium)."""
    global telegram_app

    message = "🆕 <b>Новая глава!</b>\n\n"
    message += f"📖 <b>{manga_title}</b>\n"
    message += f"Глава: {chapter_info.get('chapter_number')}"
    if chapter_info.get('chapter_volume'):
        message += f" (Том {chapter_info.get('chapter_volume')})"
    if chapter_info.get('chapter_name'):
        message += f"\n{chapter_info.get('chapter_name')}"
    message += f"\n\n🔗 <a href='{chapter_url}'>Читать на сайте</a>"

    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('SELECT telegram_id FROM users WHERE id = ?', (user_id,))
        result = c.fetchone()
        conn.close()
        if result:
            await telegram_app.bot.send_message(
                chat_id=result[0],
                text=message,
                parse_mode='HTML',
            )
    except Exception as e:
        logger.error(f"❌ Ошибка отправки уведомления: {e}")


async def send_daily_digest(hour=22):
    """Ежедневный дайджест новых глав для непремиум-пользователей."""
    global telegram_app
    if not telegram_app:
        return
    today = (datetime.utcnow() + timedelta(hours=3)).strftime('%Y-%m-%d')
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            '''SELECT DISTINCT nq.user_id, u.telegram_id
               FROM notification_queue nq
               JOIN users u ON nq.user_id = u.id
               WHERE (u.last_digest_date IS NULL OR u.last_digest_date < ?)
                 AND COALESCE(u.digest_hour, 22) = ?
                 AND u.is_active IS NOT FALSE AND u.notifications_enabled IS NOT FALSE''',
            (today, hour),
        )
        users = c.fetchall()
        conn.close()
    except Exception as e:
        logger.error(f"❌ Ошибка получения очереди дайджестов: {e}")
        return

    for row in users:
        user_id    = row['user_id']
        telegram_id = row['telegram_id']
        conn2 = None
        try:
            conn2 = get_db()
            c2 = conn2.cursor()
            c2.execute(
                '''SELECT manga_title, manga_slug, chapter_number, chapter_volume, chapter_name
                   FROM notification_queue WHERE user_id = ?
                   ORDER BY created_at ASC''',
                (user_id,),
            )
            chapters = c2.fetchall()
            if not chapters:
                continue

            message = "📚 <b>Новые главы из твоих подписок:</b>\n\n"
            for ch in chapters:
                message += f"📖 <b>{ch['manga_title']}</b>"
                if ch['chapter_number']:
                    message += f" — Глава {ch['chapter_number']}"
                if ch['chapter_volume']:
                    message += f" (Том {ch['chapter_volume']})"
                if ch['chapter_name']:
                    message += f"\n    <i>{ch['chapter_name']}</i>"
                message += "\n"
            message += "\n💎 <i>Оформи Premium — получай мгновенные уведомления с прямыми ссылками!</i>"

            await telegram_app.bot.send_message(
                chat_id=telegram_id, text=message, parse_mode='HTML',
            )
            c2.execute('UPDATE users SET last_digest_date = ? WHERE id = ?', (today, user_id))
            c2.execute('DELETE FROM notification_queue WHERE user_id = ?', (user_id,))
            conn2.commit()
        except Exception as e:
            logger.error(f"❌ Ошибка дайджеста для {user_id}: {e}")
        finally:
            if conn2:
                try:
                    conn2.close()
                except Exception:
                    pass


def _revoke_premium_loans(c, user_id):
    """Удалить все временно активированные Premium-предметы пользователя."""
    col_map = {'frame': 'frame_item_id', 'badge': 'badge_item_id', 'title': 'title_item_id'}
    c.execute(
        '''SELECT ui.item_id, si.type FROM user_items ui
           JOIN shop_items si ON ui.item_id = si.id
           WHERE ui.user_id = ? AND ui.is_premium_loan = 1 AND ui.is_equipped = 1''',
        (user_id,),
    )
    for row in c.fetchall():
        col = col_map.get(row['type'])
        if col:
            c.execute(f'UPDATE user_profile SET {col} = NULL WHERE user_id = ?', (user_id,))
    c.execute('DELETE FROM user_items WHERE user_id = ? AND is_premium_loan = 1', (user_id,))


# ==================== КОМАНДЫ БОТА ====================

async def premium_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /premium <user_id> — выдать/снять Premium (только администраторы)."""
    # ADMIN_TELEGRAM_IDS from config.py
    if update.effective_user.id not in ADMIN_TELEGRAM_IDS:
        await update.message.reply_text("❌ У вас нет доступа к этой команде.")
        return

    args = context.args
    if not args:
        await update.message.reply_text(
            "Использование: /premium <user_id> [days]\n"
            "Пример: /premium 42 30\n"
            "По умолчанию 30 дней. Повторная команда снимает Premium."
        )
        return

    target = args[0].lstrip('@')
    days = 30
    if len(args) > 1:
        try:
            days = int(args[1])
        except ValueError:
            await update.message.reply_text("❌ Неверное значение дней")
            return

    conn = get_db()
    c = conn.cursor()
    if target.isdigit():
        c.execute(
            'SELECT id, telegram_first_name, telegram_username, is_premium FROM users WHERE id = ?',
            (int(target),),
        )
    else:
        c.execute(
            'SELECT id, telegram_first_name, telegram_username, is_premium FROM users WHERE telegram_username = ?',
            (target,),
        )
    user = c.fetchone()
    if not user:
        conn.close()
        await update.message.reply_text("❌ Пользователь не найден")
        return

    name = user['telegram_first_name'] or user['telegram_username'] or f"ID {user['id']}"
    if user['is_premium']:
        c.execute('UPDATE users SET is_premium=0, premium_expires_at=NULL WHERE id=?', (user['id'],))
        _revoke_premium_loans(c, user['id'])
        conn.commit()
        conn.close()
        await update.message.reply_text(f"❌ Premium снят для {name} (ID: {user['id']})")
    else:
        expires = (datetime.utcnow() + timedelta(days=days)).isoformat()
        now = datetime.utcnow().isoformat()
        c.execute(
            'UPDATE users SET is_premium=1, premium_granted_at=?, premium_expires_at=? WHERE id=?',
            (now, expires, user['id']),
        )
        conn.commit()
        conn.close()
        await update.message.reply_text(
            f"✅ Premium выдан для {name} (ID: {user['id']}) на {days} дней\n"
            f"Истекает: {expires[:10]}"
        )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start — регистрация/вход."""
    from main import get_or_create_user_by_telegram

    telegram_id = update.effective_user.id
    username    = update.effective_user.username
    first_name  = update.effective_user.first_name
    last_name   = update.effective_user.last_name

    user = get_or_create_user_by_telegram(telegram_id, username, first_name, last_name)
    if not user:
        await update.message.reply_text("❌ Ошибка регистрации. Попробуйте позже.")
        return

    if context.args and context.args[0] == 'buy':
        await buy_command(update, context)
        return

    if context.args and len(context.args[0]) >= 8:
        ref_code = context.args[0].upper()
        try:
            ref_conn = get_db()
            referrer = ref_conn.execute(
                'SELECT id FROM users WHERE referral_code=?', (ref_code,)
            ).fetchone()
            if referrer and referrer['id'] != user['id']:
                existing = ref_conn.execute(
                    'SELECT id FROM referrals WHERE referred_id=?', (user['id'],)
                ).fetchone()
                if not existing:
                    ref_conn.execute(
                        'INSERT OR IGNORE INTO referrals (referrer_id, referred_id, rewarded) VALUES (?,?,1)',
                        (referrer['id'], user['id']),
                    )
                    ref_conn.execute(
                        'UPDATE user_stats SET xp=xp+100, coins=coins+100 WHERE user_id=?',
                        (referrer['id'],),
                    )
                    ref_conn.execute(
                        'INSERT INTO xp_log (user_id, reason, ref_id, amount) VALUES (?,?,?,?)',
                        (referrer['id'], 'referral', str(user['id']), 100),
                    )
                    ref_conn.commit()
            ref_conn.close()
        except Exception as _re:
            logger.warning(f"Referral processing error: {_re}")

    login_url  = f"{SITE_URL}/login/{user['login_token']}"
    webapp_url = SITE_URL

    open_btn = (
        InlineKeyboardButton("📱 Открыть приложение", web_app=WebAppInfo(url=webapp_url))
        if webapp_url.startswith("https://")
        else InlineKeyboardButton("📱 Открыть приложение", url=webapp_url)
    )
    keyboard = [
        [open_btn],
        [InlineKeyboardButton("📝 Войти на сайте", url=login_url)],
        [InlineKeyboardButton("🔍 Поиск манги",  callback_data="search_manga")],
        [InlineKeyboardButton("⭐ Мои подписки", callback_data="my_subscriptions")],
    ]
    message = (
        f"👋 Привет, {first_name or username}!\n\n"
        "🤖 Добро пожаловать в Manga Reader Bot!\n\n"
        "✅ Вы успешно зарегистрированы!\n"
        f"🆔 Ваш ID: {user['id']}\n\n"
        "Нажмите кнопку ниже, чтобы открыть сайт и начать читать мангу."
    )
    await update.message.reply_text(message, reply_markup=InlineKeyboardMarkup(keyboard))


async def search_manga_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /search — поиск манги."""
    from main import get_user_by_telegram_id
    user = get_user_by_telegram_id(update.effective_user.id)
    if not user:
        await update.effective_message.reply_text("❌ Сначала зарегистрируйтесь через /start")
        return
    context.user_data['waiting_for_search'] = True
    await update.effective_message.reply_text("🔍 Введите название манги для поиска:")


async def handle_search_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстовых сообщений (поиск / ввод получателя подарка)."""
    from main import get_user_by_telegram_id, search_manga_api, save_search_history

    text = update.message.text or ''

    if context.user_data.get('waiting_for_gift_username'):
        context.user_data['waiting_for_gift_username'] = False
        username = text.lstrip('@').strip()
        if not username:
            await update.message.reply_text("❌ Укажите username получателя")
            return
        recipient_id, recipient_name = await _resolve_recipient(username)
        if not recipient_id:
            await update.message.reply_text(f"❌ Пользователь @{username} не найден на BubbleManga")
            return
        keyboard = [
            [InlineKeyboardButton("🎁 1 месяц — 50 ⭐",   callback_data=f"gift_pkg:{username}:30")],
            [InlineKeyboardButton("🎁 3 месяца — 130 ⭐", callback_data=f"gift_pkg:{username}:90")],
            [InlineKeyboardButton("🎁 1 год — 450 ⭐",    callback_data=f"gift_pkg:{username}:365")],
        ]
        await update.message.reply_text(
            f"🎁 Подарить Premium пользователю *{recipient_name}*\n\nВыберите период:",
            parse_mode='MarkdownV2',
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if not context.user_data.get('waiting_for_search'):
        return

    user = get_user_by_telegram_id(update.effective_user.id)
    query = update.message.text
    if not user:
        await update.message.reply_text("❌ Ошибка пользователя")
        return
    if len(query) < 2:
        await update.message.reply_text("❌ Введите минимум 2 символа")
        return

    save_search_history(user['id'], query)
    await update.message.reply_text(f"📎 Ищу мангу по запросу: {query}...")

    results = search_manga_api(query, 5)
    if not results:
        await update.message.reply_text("❌ Ничего не найдено")
        context.user_data['waiting_for_search'] = False
        return

    message = f"📚 Найдено манг: {len(results)}\n\n"
    keyboard = []
    for i, manga in enumerate(results[:10], 1):
        message += f"{i}. {manga['manga_title']}\n"
        keyboard.append([InlineKeyboardButton(
            f"{i}. {manga['manga_title'][:20]}...",
            callback_data=f"subscribe_{manga['manga_id']}",
        )])
    keyboard.append([InlineKeyboardButton(
        "🌐 Открыть все результаты на сайте",
        url=f"{SITE_URL}/search?q={query}",
    )])
    keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="back_to_start")])
    await update.message.reply_text(message, reply_markup=InlineKeyboardMarkup(keyboard))
    context.user_data['waiting_for_search'] = False


async def subscribe_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка подписки на мангу."""
    from main import get_user_by_telegram_id, toggle_subscription
    query = update.callback_query
    await query.answer()
    if not query.data.startswith('subscribe_'):
        return
    manga_id = query.data.replace('subscribe_', '')
    user = get_user_by_telegram_id(update.effective_user.id)
    if not user:
        await query.edit_message_text("❌ Ошибка пользователя")
        return
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT manga_title FROM manga WHERE manga_id = ?', (manga_id,))
    manga = c.fetchone()
    conn.close()
    if not manga:
        await query.edit_message_text("❌ Манга не найдена")
        return
    subscribed = toggle_subscription(user['id'], manga_id)
    msg = f"✅ Вы подписались на: {manga['manga_title']}" if subscribed else f"❌ Вы отписались от: {manga['manga_title']}"
    await query.edit_message_text(msg)


async def my_subscriptions_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать подписки пользователя."""
    from main import get_user_by_telegram_id, get_user_subscriptions
    query = update.callback_query
    await query.answer()
    user = get_user_by_telegram_id(update.effective_user.id)
    if not user:
        await query.edit_message_text("❌ Сначала зарегистрируйтесь через /start")
        return
    subscriptions = get_user_subscriptions(user['id'], 10)
    if not subscriptions:
        await query.edit_message_text("🔭 У вас пока нет подписок.\n\nИспользуйте /search для поиска манги.")
        return
    message = f"⭐ Ваши подписки ({len(subscriptions)}):\n\n"
    keyboard = []
    for i, manga in enumerate(subscriptions, 1):
        message += f"{i}. {manga['manga_title']}\n"
        keyboard.append([InlineKeyboardButton(
            f"❌ Отписаться от {manga['manga_title'][:15]}...",
            callback_data=f"unsubscribe_{manga['manga_id']}",
        )])
    keyboard.append([InlineKeyboardButton("🌐 Открыть на сайте", url=f"{SITE_URL}/login/{user['login_token']}")])
    keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="back_to_start")])
    await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))


async def unsubscribe_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка отписки от манги."""
    from main import get_user_by_telegram_id, toggle_subscription
    query = update.callback_query
    await query.answer()
    if not query.data.startswith('unsubscribe_'):
        return
    manga_id = query.data.replace('unsubscribe_', '')
    user = get_user_by_telegram_id(update.effective_user.id)
    if not user:
        await query.edit_message_text("❌ Ошибка пользователя")
        return
    toggle_subscription(user['id'], manga_id)
    await my_subscriptions_callback(update, context)


async def back_to_start_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Вернуться к стартовому меню."""
    from main import get_user_by_telegram_id
    query = update.callback_query
    await query.answer()
    user = get_user_by_telegram_id(update.effective_user.id)
    if not user:
        await query.edit_message_text("❌ Ошибка пользователя")
        return
    keyboard = [
        [InlineKeyboardButton("🌐 Открыть сайт",  url=SITE_URL)],
        [InlineKeyboardButton("📝 Войти на сайте", url=f"{SITE_URL}/login/{user['login_token']}")],
        [InlineKeyboardButton("🔍 Поиск манги",   callback_data="search_manga")],
        [InlineKeyboardButton("⭐ Мои подписки",  callback_data="my_subscriptions")],
    ]
    await query.edit_message_text(
        "👋 С возвращением!\n\nВыберите действие:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def buy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /buy — пакеты монет за Telegram Stars."""
    # COIN_PACKAGES from config.py
    keyboard = [
        [InlineKeyboardButton(f"💰 {p['coins']} монет — {p['stars']} ⭐", callback_data=f"buy_coins:{p['id']}")]
        for p in COIN_PACKAGES
    ]
    text = (
        "⭐ *Купить монеты за Telegram Stars*\n\n"
        "Монеты используются в магазине BubbleManga:\n"
        "🖼 Аватары, рамки, фоны профиля\n"
        "🏷 Значки и другие украшения\n\n"
        "💡 *Как это работает?*\n"
        "1\\. Выберите пакет ниже\n"
        "2\\. Нажмите кнопку *Оплатить* в инвойсе\n"
        "3\\. Монеты зачислятся мгновенно\\!\n\n"
        "Выберите пакет:"
    )
    await update.effective_message.reply_text(
        text, parse_mode='MarkdownV2', reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def pre_checkout_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обязательный ответ на pre_checkout_query."""
    await update.pre_checkout_query.answer(ok=True)


async def _resolve_recipient(username):
    """Ищет пользователя по telegram_username. Возвращает (user_id, display_name) или (None, None)."""
    conn = get_db()
    c = conn.cursor()
    uname = username.lstrip('@')
    row = c.execute(
        'SELECT id, telegram_first_name, telegram_username FROM users WHERE telegram_username=?', (uname,)
    ).fetchone()
    conn.close()
    if not row:
        return None, None
    display = row['telegram_first_name'] or row['telegram_username'] or f'ID {row["id"]}'
    return row['id'], display


async def _send_gift_invoice(msg_or_query, context, sender_id, recipient_id, recipient_name, days):
    """Отправляет Stars invoice для подарочного Premium."""
    label_map = {30: '1 месяц', 90: '3 месяца', 365: '1 год'}
    stars_map = {30: 50, 90: 130, 365: 450}
    label   = label_map.get(days, f'{days} дней')
    stars   = stars_map.get(days, 50)
    payload = f'gift_premium:{recipient_id}:{days}:{sender_id}'
    chat_id = msg_or_query.chat_id if hasattr(msg_or_query, 'chat_id') else msg_or_query.message.chat_id
    await context.bot.send_invoice(
        chat_id=chat_id,
        title=f'Premium на {label} для {recipient_name}',
        description=f'Подарок Premium BubbleManga на {label}',
        payload=payload,
        currency='XTR',
        provider_token='',
        prices=[LabeledPrice(label=f'Premium {label}', amount=stars)],
    )


async def _handle_gift_premium_payment(update, payment, payload, payment_id):
    """Обрабатывает платёж подарочного Premium. Payload: gift_premium:{rid}:{days}:{sid}"""
    from main import _grant_premium, award_xp, create_site_notification
    try:
        _, recipient_id_str, days_str, sender_id_str = payload.split(':', 3)
        recipient_id = int(recipient_id_str)
        days         = int(days_str)
        sender_id    = int(sender_id_str)
    except (ValueError, TypeError):
        await update.message.reply_text("❌ Ошибка формата подарка.")
        return

    conn = get_db()
    c = conn.cursor()
    existing = c.execute('SELECT id FROM premium_gifts WHERE payment_id=?', (payment_id,)).fetchone()
    conn.close()
    if existing:
        await update.message.reply_text("✅ Подарок уже был обработан.")
        return

    conn = get_db()
    c = conn.cursor()
    c.execute(
        'INSERT OR IGNORE INTO premium_gifts (sender_id, recipient_id, days, stars_paid, payment_id) VALUES (?,?,?,?,?)',
        (sender_id, recipient_id, days, payment.total_amount, payment_id),
    )
    conn.commit()
    conn.close()

    pkg_map   = {30: 'premium_1m', 90: 'premium_3m', 365: 'premium_12m'}
    label_map = {30: '1 месяц',    90: '3 месяца',   365: '1 год'}
    pkg_id    = pkg_map.get(days, 'premium_1m')
    label     = label_map.get(days, f'{days} дней')

    _grant_premium(recipient_id, pkg_id, f'gift_{payment_id}', 'stars_gift')
    award_xp(sender_id, 50, 'gift_premium', ref_id=payment_id)
    create_site_notification(recipient_id, 'gift_premium', 'Вам подарили Premium!', f'на {label}', '/shop')

    try:
        conn = get_db()
        c = conn.cursor()
        rec_row = c.execute('SELECT telegram_id FROM users WHERE id=?', (recipient_id,)).fetchone()
        conn.close()
        if rec_row and rec_row['telegram_id'] and _bot_loop and _bot_loop.is_running():
            async def _notify():
                try:
                    await update.get_bot().send_message(
                        chat_id=rec_row['telegram_id'],
                        text=f"🎁 Вам подарили Premium на {label}!\nПриятного чтения на BubbleManga!",
                    )
                except Exception:
                    pass
            asyncio.run_coroutine_threadsafe(_notify(), _bot_loop)
    except Exception:
        pass

    await update.message.reply_text(f"🎁 Подарок отправлен! Premium на {label} зачислен получателю.")


async def gift_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /gift [@username] [30|90|365]."""
    telegram_id = update.effective_user.id
    conn = get_db()
    c = conn.cursor()
    sender_row = c.execute('SELECT id FROM users WHERE telegram_id=?', (telegram_id,)).fetchone()
    conn.close()
    if not sender_row:
        await update.message.reply_text("❌ Сначала войдите на сайт BubbleManga через /start")
        return
    sender_id = sender_row['id']
    args = context.args or []

    if not args:
        context.user_data['waiting_for_gift_username'] = True
        await update.message.reply_text(
            "🎁 *Подарить Premium*\n\nВведите @username получателя:",
            parse_mode='MarkdownV2',
        )
        return

    username = args[0].lstrip('@')
    recipient_id, recipient_name = await _resolve_recipient(username)
    if not recipient_id:
        await update.message.reply_text(f"❌ Пользователь @{username} не найден на BubbleManga")
        return

    if len(args) >= 2:
        try:
            days = int(args[1])
            if days not in (30, 90, 365):
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Укажите период: 30, 90 или 365 дней")
            return
        await _send_gift_invoice(update.message, context, sender_id, recipient_id, recipient_name, days)
    else:
        keyboard = [
            [InlineKeyboardButton("🎁 1 месяц — 50 ⭐",   callback_data=f"gift_pkg:{username}:30")],
            [InlineKeyboardButton("🎁 3 месяца — 130 ⭐", callback_data=f"gift_pkg:{username}:90")],
            [InlineKeyboardButton("🎁 1 год — 450 ⭐",    callback_data=f"gift_pkg:{username}:365")],
        ]
        await update.message.reply_text(
            f"🎁 Подарить Premium пользователю *{recipient_name}*\n\nВыберите период:",
            parse_mode='MarkdownV2',
            reply_markup=InlineKeyboardMarkup(keyboard),
        )


async def successful_payment_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начисляет монеты после успешной оплаты Stars."""
    from main import _credit_coins, _grant_premium
    payment    = update.message.successful_payment
    payload    = payment.invoice_payload
    payment_id = payment.telegram_payment_charge_id

    if payload.startswith('gift_premium:'):
        await _handle_gift_premium_payment(update, payment, payload, payment_id)
        return

    try:
        package_id, user_id_str = payload.rsplit(':', 1)
        user_id = int(user_id_str)
    except (ValueError, AttributeError):
        await update.message.reply_text("Ошибка обработки платежа. Обратитесь к администратору.")
        return

    pkg = next((p for p in COIN_PACKAGES if p['id'] == package_id), None)
    if not pkg:
        await update.message.reply_text("Пакет не найден. Обратитесь к администратору.")
        return

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute(
            'INSERT OR IGNORE INTO coin_purchases (user_id, package_id, stars_paid, coins_received, payment_id) VALUES (?, ?, ?, ?, ?)',
            (user_id, package_id, payment.total_amount, pkg['coins'], payment_id),
        )
        if c.rowcount > 0:
            c.execute('UPDATE user_stats SET coins = coins + ? WHERE user_id = ?', (pkg['coins'], user_id))
        conn.commit()
    finally:
        conn.close()

    await update.message.reply_text(
        f"✅ Оплата прошла успешно!\n\n💰 Начислено {pkg['coins']} монет.\nСпасибо за поддержку!"
    )


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback-кнопок."""
    # COIN_PACKAGES from config.py
    query = update.callback_query
    await query.answer()

    data = query.data
    if data == "my_subscriptions":
        await my_subscriptions_callback(update, context)
    elif data == "search_manga":
        await search_manga_command(update, context)
    elif data.startswith("subscribe_"):
        await subscribe_callback(update, context)
    elif data.startswith("unsubscribe_"):
        await unsubscribe_callback(update, context)
    elif data == "back_to_start":
        await back_to_start_callback(update, context)
    elif data.startswith("buy_coins:"):
        pkg_id = data[len("buy_coins:"):]
        pkg = next((p for p in COIN_PACKAGES if p['id'] == pkg_id), None)
        if not pkg:
            await query.answer("Пакет не найден", show_alert=True)
            return
        telegram_id = query.from_user.id
        conn = get_db()
        c = conn.cursor()
        c.execute('SELECT id FROM users WHERE telegram_id = ?', (telegram_id,))
        row = c.fetchone()
        conn.close()
        if not row:
            await query.answer("Сначала войдите на сайт BubbleManga", show_alert=True)
            return
        user_id = row['id']
        await context.bot.send_invoice(
            chat_id=query.message.chat_id,
            title=pkg['label'],
            description=f"{pkg['coins']} монет для BubbleManga",
            payload=f"{pkg['id']}:{user_id}",
            currency="XTR",
            provider_token="",
            prices=[LabeledPrice(label=pkg['label'], amount=pkg['stars'])],
        )
    elif data.startswith("gift_pkg:"):
        _, username, days_str = data.split(":", 2)
        try:
            days = int(days_str)
        except ValueError:
            await query.answer("Ошибка", show_alert=True)
            return
        telegram_id = query.from_user.id
        conn = get_db()
        c = conn.cursor()
        sender_row = c.execute('SELECT id FROM users WHERE telegram_id=?', (telegram_id,)).fetchone()
        conn.close()
        if not sender_row:
            await query.answer("Сначала войдите на сайт BubbleManga", show_alert=True)
            return
        sender_id = sender_row['id']
        recipient_id, recipient_name = await _resolve_recipient(username)
        if not recipient_id:
            await query.answer(f"Пользователь @{username} не найден", show_alert=True)
            return
        await _send_gift_invoice(query.message, context, sender_id, recipient_id, recipient_name, days)


# ==================== ЗАПУСК БОТА ====================

def run_telegram_bot():
    """Запускает Telegram-бот в отдельном потоке."""
    # TELEGRAM_BOT_TOKEN from config.py

    def _start():
        global _bot_loop, telegram_app
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            _bot_loop = loop

            telegram_app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

            telegram_app.add_handler(CommandHandler("start",   start_command))
            telegram_app.add_handler(CommandHandler("search",  search_manga_command))
            telegram_app.add_handler(CommandHandler("premium", premium_command))
            telegram_app.add_handler(CommandHandler("buy",     buy_command))
            telegram_app.add_handler(CommandHandler("gift",    gift_command))

            telegram_app.add_handler(CallbackQueryHandler(handle_callback))
            telegram_app.add_handler(PreCheckoutQueryHandler(pre_checkout_handler))
            telegram_app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_handler))
            telegram_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search_message))

            print("🤖 Telegram бот запущен!")
            loop.run_until_complete(telegram_app.initialize())
            loop.run_until_complete(telegram_app.start())
            loop.run_until_complete(telegram_app.updater.start_polling(drop_pending_updates=True))
            print("🤖 Бот запущен и работает...")
            loop.run_forever()
        except Exception as e:
            import traceback
            print(f"❌ Ошибка запуска Telegram бота: {e}")
            traceback.print_exc()

    t = threading.Thread(target=_start, daemon=True, name="TelegramBot")
    t.start()
    return t
