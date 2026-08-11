"""
bot.py — Telegram-бот Манговой.

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
import secrets
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

async def send_telegram_notification(user_id, manga_title, chapter_info, chapter_url, cover_url=None, chapter_slug=None):
    """Отправка мгновенного уведомления через Telegram (только Premium)."""
    global telegram_app

    caption = "🆕 <b>Новая глава!</b>\n\n"
    caption += f"📖 <b>{manga_title}</b>\n"
    caption += f"Глава: {chapter_info.get('chapter_number')}"
    if chapter_info.get('chapter_volume'):
        caption += f" (Том {chapter_info.get('chapter_volume')})"
    if chapter_info.get('chapter_name'):
        caption += f"\n{chapter_info.get('chapter_name')}"

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("📖 Читать", web_app=WebAppInfo(url=chapter_url))
    ]])

    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('SELECT telegram_id FROM users WHERE id = ?', (user_id,))
        result = c.fetchone()
        conn.close()
        if result:
            chat_id = result[0]
            sent_msg = None
            has_photo = False
            if cover_url:
                try:
                    sent_msg = await telegram_app.bot.send_photo(
                        chat_id=chat_id,
                        photo=cover_url,
                        caption=caption,
                        parse_mode='HTML',
                        reply_markup=keyboard,
                    )
                    has_photo = True
                except Exception:
                    sent_msg = None  # fallback на текст если фото не загрузилось
            if sent_msg is None:
                sent_msg = await telegram_app.bot.send_message(
                    chat_id=chat_id,
                    text=caption,
                    parse_mode='HTML',
                    reply_markup=keyboard,
                )

            # Запоминаем сообщение, чтобы потом отметить "✅ Прочитано"
            if chapter_slug and sent_msg:
                try:
                    conn3 = get_db()
                    conn3.execute(
                        '''INSERT INTO chapter_notifications
                           (user_id, chapter_slug, chat_id, message_id, has_photo, caption)
                           VALUES (?, ?, ?, ?, ?, ?)''',
                        (user_id, chapter_slug, chat_id, sent_msg.message_id, has_photo, caption)
                    )
                    conn3.commit()
                    conn3.close()
                except Exception as e_log:
                    logger.warning(f"⚠️ Не удалось сохранить chapter_notifications: {e_log}")
    except Exception as e:
        logger.error(f"❌ Ошибка отправки уведомления: {e}")


async def mark_chapter_notification_read(user_id, chapter_slug):
    """Отмечает уведомление(я) о главе как прочитанное — редактирует сообщение в Telegram."""
    global telegram_app
    if not telegram_app:
        return
    try:
        conn = get_db()
        rows = conn.execute(
            '''SELECT id, chat_id, message_id, has_photo, caption FROM chapter_notifications
               WHERE user_id = ? AND chapter_slug = ? AND is_read = FALSE''',
            (user_id, chapter_slug)
        ).fetchall()
        conn.close()
    except Exception as e:
        logger.warning(f"⚠️ mark_chapter_notification_read lookup: {e}")
        return

    for row in rows:
        notif_id   = row['id']
        chat_id    = row['chat_id']
        message_id = row['message_id']
        has_photo  = row['has_photo']
        caption    = row['caption']

        new_caption = caption + "\n\n✅ <b>Прочитано</b>"
        new_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("↩️ Снять отметку «прочитано»", callback_data=f"unread_notif:{notif_id}")]
        ])
        try:
            if has_photo:
                await telegram_app.bot.edit_message_caption(
                    chat_id=chat_id, message_id=message_id,
                    caption=new_caption, parse_mode='HTML', reply_markup=new_keyboard
                )
            else:
                await telegram_app.bot.edit_message_text(
                    chat_id=chat_id, message_id=message_id,
                    text=new_caption, parse_mode='HTML', reply_markup=new_keyboard
                )
            conn2 = get_db()
            conn2.execute('UPDATE chapter_notifications SET is_read = TRUE WHERE id = ?', (notif_id,))
            conn2.commit()
            conn2.close()
        except Exception as e:
            logger.warning(f"⚠️ Не удалось отредактировать уведомление {notif_id}: {e}")


async def unread_notification_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Кнопка «Снять отметку «прочитано»» — возвращает сообщение в исходный вид."""
    query = update.callback_query
    await query.answer("Отметка снята")
    try:
        notif_id = int(query.data.split(':', 1)[1])
    except (IndexError, ValueError):
        return

    conn = get_db()
    row = conn.execute(
        'SELECT chat_id, message_id, has_photo, caption, chapter_slug FROM chapter_notifications WHERE id = ?',
        (notif_id,)
    ).fetchone()
    if not row:
        conn.close()
        return

    chat_id      = row['chat_id']
    message_id   = row['message_id']
    has_photo    = row['has_photo']
    caption      = row['caption']
    chapter_slug = row['chapter_slug']

    conn.execute('UPDATE chapter_notifications SET is_read = FALSE WHERE id = ?', (notif_id,))
    conn.commit()

    # Восстанавливаем ссылку на главу для кнопки "Читать"
    chapter_url = None
    ch_row = conn.execute(
        '''SELECT c.chapter_slug, m.manga_slug FROM chapters c
           JOIN manga m ON m.manga_id = c.manga_id WHERE c.chapter_slug = ?''',
        (chapter_slug,)
    ).fetchone()
    conn.close()
    if ch_row:
        chapter_url = f"{SITE_URL}/read/{ch_row['manga_slug']}/{ch_row['chapter_slug']}"

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("📖 Читать", web_app=WebAppInfo(url=chapter_url))
    ]]) if chapter_url else None

    try:
        if has_photo:
            await context.bot.edit_message_caption(
                chat_id=chat_id, message_id=message_id,
                caption=caption, parse_mode='HTML', reply_markup=keyboard
            )
        else:
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id,
                text=caption, parse_mode='HTML', reply_markup=keyboard
            )
    except Exception as e:
        logger.warning(f"⚠️ Не удалось вернуть уведомление {notif_id} в исходный вид: {e}")


async def send_daily_digest():
    """Ежедневный дайджест в 22:00 МСК — список новых глав для непремиум-пользователей."""
    global telegram_app
    if not telegram_app:
        return
    today = (datetime.utcnow() + timedelta(hours=3)).strftime('%Y-%m-%d')
    try:
        conn = get_db()
        c = conn.cursor()
        # Все непремиум-пользователи у кого есть накопившиеся уведомления и telegram_id
        c.execute(
            '''SELECT DISTINCT nq.user_id, u.telegram_id
               FROM notification_queue nq
               JOIN users u ON nq.user_id = u.id
               WHERE u.telegram_id IS NOT NULL
                 AND (u.last_digest_date IS NULL OR u.last_digest_date < ?)
                 AND u.is_active IS NOT FALSE
                 AND u.notifications_enabled IS NOT FALSE
                 AND COALESCE(u.is_premium, 0) = 0
                 AND u.is_bot IS NOT TRUE''',
            (today,),
        )
        users = c.fetchall()
        conn.close()
    except Exception as e:
        logger.error(f"❌ Ошибка получения очереди дайджестов: {e}")
        return

    for row in users:
        user_id     = row['user_id']
        telegram_id = row['telegram_id']
        conn2 = None
        try:
            conn2 = get_db()
            c2 = conn2.cursor()
            c2.execute(
                '''SELECT manga_title, chapter_number, chapter_volume, chapter_name
                   FROM notification_queue WHERE user_id = ?
                   ORDER BY created_at ASC''',
                (user_id,),
            )
            chapters = c2.fetchall()
            if not chapters:
                continue

            message = "📚 <b>Новые главы из твоих подписок:</b>\n\n"
            for ch in chapters:
                line = f"📖 <b>{ch['manga_title']}</b>"
                if ch['chapter_number']:
                    line += f" — Глава {ch['chapter_number']}"
                if ch['chapter_volume']:
                    line += f" (Том {ch['chapter_volume']})"
                if ch['chapter_name']:
                    line += f"\n    <i>{ch['chapter_name']}</i>"
                message += line + "\n"
            message += "\n💎 <i>Оформи Premium — получай мгновенные уведомления!</i>"

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
        [InlineKeyboardButton("📚 Подборки",       callback_data="catalog")],
        [InlineKeyboardButton("🔍 Поиск манги",    callback_data="search_manga")],
        [InlineKeyboardButton("📝 Войти на сайте", url=login_url)],
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
            await update.message.reply_text(f"❌ Пользователь @{username} не найден на Манговой")
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
        [InlineKeyboardButton("📱 Открыть приложение", web_app=WebAppInfo(url=SITE_URL))],
        [InlineKeyboardButton("📚 Подборки",           callback_data="catalog")],
        [InlineKeyboardButton("🔍 Поиск манги",        callback_data="search_manga")],
        [InlineKeyboardButton("📝 Войти на сайте",     url=f"{SITE_URL}/login/{user['login_token']}")],
    ]
    # Если текущее сообщение — фото, удаляем и шлём текст
    try:
        await query.edit_message_text(
            "👋 С возвращением!\n\nВыберите действие:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Exception:
        try:
            await query.message.delete()
        except Exception:
            pass
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="👋 С возвращением!\n\nВыберите действие:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )


# ══════════════════════════════════════════════════════
#  ПОДБОРКИ

async def _safe_edit_text(query, context, text: str, keyboard, parse_mode="Markdown"):
    """edit_message_text, но если текущее сообщение — фото, сначала удаляет его."""
    if query.message.photo:
        try:
            await query.message.delete()
        except Exception:
            pass
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=text, reply_markup=keyboard, parse_mode=parse_mode,
        )
        return
    try:
        await query.edit_message_text(text, reply_markup=keyboard, parse_mode=parse_mode)
    except Exception:
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=text, reply_markup=keyboard, parse_mode=parse_mode,
        )
# ══════════════════════════════════════════════════════

def _catalog_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔥 Самое читаемое",      callback_data="cat_popular")],
        [InlineKeyboardButton("🆕 Последние обновления", callback_data="cat_new")],
        [InlineKeyboardButton("⭐ Мои подписки",         callback_data="cat_subs")],
        [InlineKeyboardButton("📖 История чтения",       callback_data="cat_history")],
        [InlineKeyboardButton("◀️ Назад",                callback_data="back_to_start")],
    ])


async def catalog_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await _safe_edit_text(query, context, "📚 *Подборки*\n\nВыберите раздел:", _catalog_keyboard())


async def cat_popular_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Самое читаемое."""
    query = update.callback_query
    await query.answer()
    conn = get_db()
    rows = conn.execute(
        'SELECT manga_id, manga_title, views FROM manga '
        'WHERE views IS NOT NULL ORDER BY views DESC LIMIT 10'
    ).fetchall()
    conn.close()

    keyboard = []
    for r in rows:
        title = r['manga_title'][:32]
        views = r['views'] or 0
        views_str = f"{views // 1000}к" if views >= 1000 else str(views)
        keyboard.append([InlineKeyboardButton(f"👁 {views_str}  {title}", callback_data=f"manga_{r['manga_id']}")])

    context.user_data['catalog_back'] = 'cat_popular'
    keyboard.append([InlineKeyboardButton("◀️ К подборкам", callback_data="catalog")])
    await _safe_edit_text(query, context, "🔥 *Самое читаемое:*", InlineKeyboardMarkup(keyboard))


async def cat_new_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Последние обновления."""
    query = update.callback_query
    await query.answer()
    conn = get_db()
    rows = conn.execute(
        '''SELECT m.manga_id, m.manga_title, MAX(c.created_at) as last_ch
           FROM manga m JOIN chapters c ON c.manga_id = m.manga_id
           GROUP BY m.manga_id, m.manga_title
           ORDER BY last_ch DESC NULLS LAST LIMIT 10'''
    ).fetchall()
    conn.close()

    keyboard = []
    for r in rows:
        title = r['manga_title'][:34]
        keyboard.append([InlineKeyboardButton(f"🆕 {title}", callback_data=f"manga_{r['manga_id']}")])

    context.user_data['catalog_back'] = 'cat_new'
    keyboard.append([InlineKeyboardButton("◀️ К подборкам", callback_data="catalog")])
    await _safe_edit_text(query, context, "🆕 *Последние обновления:*", InlineKeyboardMarkup(keyboard))


async def cat_subs_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Мои подписки."""
    from main import get_user_by_telegram_id, get_user_subscriptions
    query = update.callback_query
    await query.answer()
    user = get_user_by_telegram_id(update.effective_user.id)
    if not user:
        await _safe_edit_text(query, context, "❌ Сначала зарегистрируйтесь через /start", None)
        return
    subs = get_user_subscriptions(user['id'], 15)
    back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ К подборкам", callback_data="catalog")]])
    if not subs:
        await _safe_edit_text(query, context,
            "⭐ *Мои подписки*\n\nУ вас пока нет подписок.\nИспользуйте /search для поиска манги.", back_kb)
        return

    keyboard = []
    for m in subs:
        keyboard.append([InlineKeyboardButton(f"⭐ {m['manga_title'][:34]}", callback_data=f"manga_{m['manga_id']}")])
    context.user_data['catalog_back'] = 'cat_subs'
    keyboard.append([InlineKeyboardButton("◀️ К подборкам", callback_data="catalog")])
    await _safe_edit_text(query, context, f"⭐ *Мои подписки* ({len(subs)}):", InlineKeyboardMarkup(keyboard))


async def cat_history_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """История чтения."""
    from main import get_user_by_telegram_id, get_user_reading
    query = update.callback_query
    await query.answer()
    user = get_user_by_telegram_id(update.effective_user.id)
    if not user:
        await _safe_edit_text(query, context, "❌ Сначала зарегистрируйтесь через /start", None)
        return
    history = get_user_reading(user['id'], 15)
    back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ К подборкам", callback_data="catalog")]])
    if not history:
        await _safe_edit_text(query, context, "📖 *История чтения*\n\nВы ещё ничего не читали.", back_kb)
        return

    keyboard = []
    for item in history:
        title = item['manga_title'][:28]
        ch = f" · Гл.{item['last_chapter_number']}" if item.get('last_chapter_number') else ""
        keyboard.append([InlineKeyboardButton(f"📖 {title}{ch}", callback_data=f"manga_{item['manga_id']}")])
    context.user_data['catalog_back'] = 'cat_history'
    keyboard.append([InlineKeyboardButton("◀️ К подборкам", callback_data="catalog")])
    await _safe_edit_text(query, context, f"📖 *История чтения* ({len(history)}):", InlineKeyboardMarkup(keyboard))


def _manga_caption(manga: dict, subscribed: bool) -> tuple[str, InlineKeyboardMarkup]:
    """Возвращает (caption, keyboard) для карточки манги."""
    title        = manga['manga_title']
    manga_id     = manga['manga_id']
    manga_slug   = manga['manga_slug']
    manga_type   = manga.get('manga_type') or ''
    manga_status = manga.get('manga_status') or ''
    rating       = manga.get('rating')
    views        = manga.get('views') or 0
    description  = (manga.get('description') or '').strip()

    type_map   = {'MANGA': 'Манга', 'MANHWA': 'Манхва', 'MANHUA': 'Маньхуа', 'NOVEL': 'Новелла'}
    status_map = {'ONGOING': '🟢 Онгоинг', 'COMPLETED': '✅ Завершена', 'HIATUS': '⏸ Хиатус', 'ANNOUNCED': '📢 Анонс'}

    caption = f"*{title}*\n"
    meta = []
    if manga_type:   meta.append(type_map.get(manga_type, manga_type))
    if manga_status: meta.append(status_map.get(manga_status, manga_status))
    if meta: caption += " · ".join(meta) + "\n"
    info = []
    try:
        if rating: info.append(f"⭐ {float(rating):.1f}")
    except (ValueError, TypeError):
        pass
    if views: info.append(f"👁 {views // 1000}к" if views >= 1000 else f"👁 {views}")
    if info: caption += " · ".join(info) + "\n"
    if description:
        caption += "\n" + description[:280] + ("…" if len(description) > 280 else "")

    back_cb  = f"_back_manga_{manga_id}"
    sub_text = "❌ Отписаться" if subscribed else "⭐ Подписаться"
    sub_cb   = f"unsub_manga_{manga_id}" if subscribed else f"sub_manga_{manga_id}"
    manga_url = f"{SITE_URL}/manga/{manga_slug}"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Список глав", callback_data=f"chs_{manga_id}_0")],
        [InlineKeyboardButton(sub_text, callback_data=sub_cb),
         InlineKeyboardButton("🌐 На сайте", web_app=WebAppInfo(url=manga_url))],
        [InlineKeyboardButton("◀️ Назад", callback_data=back_cb)],
    ])
    return caption, keyboard


async def _edit_or_send_photo(
    context, query, chat_id: int,
    cover_url: str, caption: str, keyboard: InlineKeyboardMarkup
):
    """
    Плавно обновляет сообщение:
    - если текущее — фото: редактирует caption+keyboard (без мигания)
    - если текущее — текст и cover есть: удаляет + шлёт фото
    - иначе: редактирует текст
    """
    is_photo = bool(query.message.photo)

    if is_photo:
        # Самый плавный путь — просто меняем caption и кнопки
        try:
            await query.edit_message_caption(caption=caption, reply_markup=keyboard, parse_mode="Markdown")
            return
        except Exception as e:
            logger.warning(f"edit_message_caption failed: {e}")

    if cover_url.startswith("http"):
        # Переход от текста к фото — удаляем и шлём фото
        try:
            await query.message.delete()
        except Exception:
            pass
        try:
            await context.bot.send_photo(
                chat_id=chat_id, photo=cover_url,
                caption=caption, parse_mode="Markdown", reply_markup=keyboard,
            )
            return
        except Exception as e:
            logger.warning(f"send_photo failed: {e}")

    # Fallback: текстовое сообщение
    try:
        await query.edit_message_text(caption, reply_markup=keyboard, parse_mode="Markdown")
    except Exception:
        await context.bot.send_message(chat_id=chat_id, text=caption, reply_markup=keyboard, parse_mode="Markdown")


async def manga_card_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, manga_id: str):
    """Карточка манги с обложкой."""
    from main import get_user_by_telegram_id, is_subscribed as _is_subscribed
    query = update.callback_query
    await query.answer()

    conn = get_db()
    manga = conn.execute('SELECT * FROM manga WHERE manga_id = ?', (manga_id,)).fetchone()
    conn.close()
    if not manga:
        await query.edit_message_text("❌ Манга не найдена")
        return

    manga = dict(manga)
    user = get_user_by_telegram_id(update.effective_user.id)
    subscribed = _is_subscribed(user['id'], manga_id) if user else False

    # Сохраняем контекст для кнопки «Назад» и обложки в списке глав
    context.user_data[f'manga_{manga_id}'] = manga
    context.user_data['current_manga_id'] = manga_id

    caption, keyboard = _manga_caption(manga, subscribed)
    cover_url = manga.get('cover_url') or ''
    await _edit_or_send_photo(context, query, query.message.chat_id, cover_url, caption, keyboard)


async def chapters_list_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, manga_id: str, page: int):
    """Список глав с пагинацией — фото остаётся, меняется только caption и кнопки."""
    query = update.callback_query
    await query.answer()

    PER_PAGE = 8
    conn = get_db()
    manga_row = conn.execute('SELECT manga_title, manga_slug, cover_url FROM manga WHERE manga_id = ?', (manga_id,)).fetchone()
    if not manga_row:
        conn.close()
        await query.edit_message_text("❌ Манга не найдена")
        return

    total = conn.execute('SELECT COUNT(*) as cnt FROM chapters WHERE manga_id = ?', (manga_id,)).fetchone()['cnt']
    offset = page * PER_PAGE
    chapters = conn.execute(
        'SELECT chapter_slug, chapter_number, chapter_volume, chapter_name FROM chapters '
        'WHERE manga_id = ? ORDER BY CAST(chapter_number AS FLOAT) ASC LIMIT ? OFFSET ?',
        (manga_id, PER_PAGE, offset)
    ).fetchall()
    conn.close()

    manga_slug  = manga_row['manga_slug']
    manga_title = manga_row['manga_title']
    cover_url   = manga_row.get('cover_url') or ''
    total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)

    keyboard = []
    for ch in chapters:
        vol  = f"Т{ch['chapter_volume']} " if ch.get('chapter_volume') else ""
        num  = f"Гл.{ch['chapter_number']}" if ch.get('chapter_number') else ""
        name = f" — {ch['chapter_name']}" if ch.get('chapter_name') else ""
        label = f"▶ {vol}{num}{name}"[:38]
        ch_url = f"{SITE_URL}/read/{manga_slug}/{ch['chapter_slug']}"
        keyboard.append([InlineKeyboardButton(label, web_app=WebAppInfo(url=ch_url))])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀", callback_data=f"chs_{manga_id}_{page - 1}"))
    nav.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("▶", callback_data=f"chs_{manga_id}_{page + 1}"))
    if nav:
        keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("◀️ К манге", callback_data=f"manga_{manga_id}")])

    caption = f"📋 *{manga_title[:40]}*\nГлав: {total} · страница {page + 1}/{total_pages}"
    kb = InlineKeyboardMarkup(keyboard)

    # Если сообщение — фото, просто меняем caption (плавно, без мигания)
    if query.message.photo:
        try:
            await query.edit_message_caption(caption=caption, reply_markup=kb, parse_mode="Markdown")
            return
        except Exception as e:
            logger.warning(f"chapters edit_message_caption failed: {e}")

    # Переход от текста: удаляем и шлём фото
    if cover_url.startswith("http"):
        try:
            await query.message.delete()
        except Exception:
            pass
        try:
            await context.bot.send_photo(
                chat_id=query.message.chat_id, photo=cover_url,
                caption=caption, parse_mode="Markdown", reply_markup=kb,
            )
            return
        except Exception as e:
            logger.warning(f"chapters send_photo failed: {e}")

    try:
        await query.edit_message_text(caption, reply_markup=kb, parse_mode="Markdown")
    except Exception:
        await context.bot.send_message(
            chat_id=query.message.chat_id, text=caption, reply_markup=kb, parse_mode="Markdown"
        )


async def sub_manga_toggle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, manga_id: str):
    """Подписаться/отписаться — плавно меняет только caption и кнопки."""
    from main import get_user_by_telegram_id, toggle_subscription, is_subscribed as _is_subscribed
    query = update.callback_query
    user = get_user_by_telegram_id(update.effective_user.id)
    if not user:
        await query.answer("Сначала зарегистрируйтесь через /start", show_alert=True)
        return
    toggle_subscription(user['id'], manga_id)
    now_subscribed = _is_subscribed(user['id'], manga_id)
    await query.answer("✅ Подписались!" if now_subscribed else "❌ Отписались")

    # Берём мангу из кеша user_data или из БД
    manga = context.user_data.get(f'manga_{manga_id}')
    if not manga:
        conn = get_db()
        row = conn.execute('SELECT * FROM manga WHERE manga_id = ?', (manga_id,)).fetchone()
        conn.close()
        if not row:
            return
        manga = dict(row)
        context.user_data[f'manga_{manga_id}'] = manga

    caption, keyboard = _manga_caption(manga, now_subscribed)

    # Обновляем только caption + keyboard — без мигания
    if query.message.photo:
        try:
            await query.edit_message_caption(caption=caption, reply_markup=keyboard, parse_mode="Markdown")
            return
        except Exception as e:
            logger.warning(f"sub_toggle edit_caption failed: {e}")
    try:
        await query.edit_message_text(caption, reply_markup=keyboard, parse_mode="Markdown")
    except Exception:
        pass


async def buy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /buy — пакеты монет за Telegram Stars."""
    # COIN_PACKAGES from config.py
    keyboard = [
        [InlineKeyboardButton(f"💰 {p['coins']} монет — {p['stars']} ⭐", callback_data=f"buy_coins:{p['id']}")]
        for p in COIN_PACKAGES
    ]
    text = (
        "⭐ *Купить монеты за Telegram Stars*\n\n"
        "Монеты используются в магазине Манговой:\n"
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
        description=f'Подарок Premium Манговая на {label}',
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
                        text=f"🎁 Вам подарили Premium на {label}!\nПриятного чтения на Манговой!",
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
        await update.message.reply_text("❌ Сначала войдите на сайт Манговой через /start")
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
        await update.message.reply_text(f"❌ Пользователь @{username} не найден на Манговой")
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
    # ── Подборки ──────────────────────────────────────────
    if data == "catalog":
        await catalog_callback(update, context)
    elif data == "cat_popular":
        await cat_popular_callback(update, context)
    elif data == "cat_new":
        await cat_new_callback(update, context)
    elif data == "cat_subs":
        await cat_subs_callback(update, context)
    elif data == "cat_history":
        await cat_history_callback(update, context)
    elif data.startswith("manga_"):
        await manga_card_callback(update, context, data[len("manga_"):])
    elif data.startswith("chs_"):
        # chs_<manga_id>_<page>
        parts = data.split("_", 2)  # ["chs", manga_id_part, page]
        # manga_id может содержать _, поэтому берём последний элемент как страницу
        raw = data[len("chs_"):]
        page_str = raw.rsplit("_", 1)[-1]
        manga_id = raw.rsplit("_", 1)[0]
        await chapters_list_callback(update, context, manga_id, int(page_str))
    elif data.startswith("sub_manga_"):
        await sub_manga_toggle_callback(update, context, data[len("sub_manga_"):])
    elif data.startswith("unsub_manga_"):
        await sub_manga_toggle_callback(update, context, data[len("unsub_manga_"):])
    elif data.startswith("_back_manga_"):
        back_cb = context.user_data.get('catalog_back', 'catalog')
        if back_cb == 'cat_popular':
            await cat_popular_callback(update, context)
        elif back_cb == 'cat_new':
            await cat_new_callback(update, context)
        elif back_cb == 'cat_subs':
            await cat_subs_callback(update, context)
        elif back_cb == 'cat_history':
            await cat_history_callback(update, context)
        else:
            await catalog_callback(update, context)
    elif data == "noop":
        pass  # кнопка-счётчик страниц
    elif data.startswith("unread_notif:"):
        await unread_notification_callback(update, context)
    # ── Прочее ────────────────────────────────────────────
    elif data == "my_subscriptions":
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
            await query.answer("Сначала войдите на сайт Манговой", show_alert=True)
            return
        user_id = row['id']
        await context.bot.send_invoice(
            chat_id=query.message.chat_id,
            title=pkg['label'],
            description=f"{pkg['coins']} монет для Манговой",
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
            await query.answer("Сначала войдите на сайт Манговой", show_alert=True)
            return
        sender_id = sender_row['id']
        recipient_id, recipient_name = await _resolve_recipient(username)
        if not recipient_id:
            await query.answer(f"Пользователь @{username} не найден", show_alert=True)
            return
        await _send_gift_invoice(query.message, context, sender_id, recipient_id, recipient_name, days)


async def link_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/link — сгенерировать одноразовую ссылку для привязки Telegram к email-аккаунту."""
    telegram_id = update.effective_user.id
    username    = update.effective_user.username
    first_name  = update.effective_user.first_name
    last_name   = update.effective_user.last_name

    conn = get_db()
    # Если этот Telegram уже привязан к аккаунту — нет смысла в /link
    existing = conn.execute(
        'SELECT id FROM users WHERE telegram_id=?', (telegram_id,)
    ).fetchone()
    if existing:
        conn.close()
        await update.message.reply_text(
            "Ваш Telegram уже привязан к аккаунту Манговой.\n"
            "Нет необходимости в привязке."
        )
        return

    token = secrets.token_urlsafe(32)
    expires_at = (datetime.utcnow() + timedelta(minutes=15)).isoformat()
    conn.execute(
        '''INSERT INTO telegram_link_tokens
           (token, telegram_id, tg_username, tg_first_name, tg_last_name, expires_at)
           VALUES (?, ?, ?, ?, ?, ?)''',
        (token, telegram_id, username, first_name, last_name, expires_at)
    )
    conn.commit()
    conn.close()

    link_url = f"{SITE_URL}/auth/link-telegram/{token}"
    await update.message.reply_text(
        "🔗 *Привязка Telegram к аккаунту Манговой*\n\n"
        "1\\. Войдите на сайт через email/пароль\n"
        f"2\\. Перейдите по ссылке \\(действует 15 минут\\):\n"
        f"`{link_url}`\n\n"
        "После перехода аккаунты будут объединены\\.",
        parse_mode="MarkdownV2"
    )


# ==================== ПОДДЕРЖКА ====================

async def notify_admins_new_ticket(ticket_id, user_display, subject, text):
    """Уведомить всех adminов о новом тикете поддержки."""
    global telegram_app
    if not telegram_app:
        return
    msg = (
        f"🎫 <b>Новая заявка #{ticket_id}</b>\n"
        f"👤 <b>От:</b> {user_display}\n"
        f"📋 <b>Тема:</b> {subject}\n\n"
        f"{text[:500]}"
    )
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔍 Открыть панель", url=f"{SITE_URL}/admin?section=support&ticket={ticket_id}")
    ]])
    for tg_id in ADMIN_TELEGRAM_IDS:
        try:
            await telegram_app.bot.send_message(
                chat_id=tg_id, text=msg, parse_mode='HTML', reply_markup=keyboard
            )
        except Exception as e:
            logger.warning(f"notify_admins_new_ticket: cannot send to {tg_id}: {e}")


async def support_reply_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/reply <ticket_id> <текст> — ответ на тикет от имени поддержки (только admin)."""
    if update.effective_user.id not in ADMIN_TELEGRAM_IDS:
        await update.message.reply_text("❌ Нет доступа")
        return
    args = context.args
    if not args or len(args) < 2:
        await update.message.reply_text("Использование: /reply <id_заявки> <текст ответа>")
        return
    try:
        ticket_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ ID заявки должен быть числом")
        return
    reply_text = ' '.join(args[1:])

    from main import create_site_notification, send_tg_to_user
    conn = get_db()
    try:
        ticket = conn.execute(
            'SELECT t.id, t.subject, t.user_id, t.status, '
            'COALESCE(u.telegram_first_name, u.telegram_username, u.id::text) AS display_name '
            'FROM support_tickets t JOIN users u ON u.id = t.user_id WHERE t.id = ?',
            (ticket_id,)
        ).fetchone()
        if not ticket:
            await update.message.reply_text(f"❌ Заявка #{ticket_id} не найдена")
            return
        if ticket['status'] == 'closed':
            await update.message.reply_text(f"⚠️ Заявка #{ticket_id} уже закрыта")
            return

        # Находим admin user_id по telegram_id
        admin_row = conn.execute(
            'SELECT id FROM users WHERE telegram_id = ?', (update.effective_user.id,)
        ).fetchone()
        admin_db_id = admin_row['id'] if admin_row else None

        conn.execute(
            'INSERT INTO support_messages (ticket_id, sender_id, is_admin, text) VALUES (?,?,?,?)',
            (ticket_id, admin_db_id, True, reply_text)
        )
        conn.execute(
            'UPDATE support_tickets SET updated_at = NOW() WHERE id = ?', (ticket_id,)
        )
        conn.commit()
    finally:
        conn.close()

    # Уведомление на сайте
    create_site_notification(
        ticket['user_id'], 'support',
        f'Манговая · Ответ по заявке #{ticket_id}',
        body=reply_text[:300],
        url=f'/support?ticket={ticket_id}',
        ref_id=f'support_{ticket_id}',
    )
    # TG юзеру
    send_tg_to_user(
        ticket['user_id'],
        f'💬 <b>Манговая</b> · ответ по заявке #{ticket_id}\n'
        f'<b>Тема:</b> {ticket["subject"]}\n\n'
        f'{reply_text}\n\n'
        f'<a href="{SITE_URL}/support?ticket={ticket_id}">Открыть заявку на сайте</a>'
    )
    await update.message.reply_text(f"✅ Ответ на заявку #{ticket_id} отправлен пользователю {ticket['display_name']}")


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
            telegram_app.add_handler(CommandHandler("link",    link_command))
            telegram_app.add_handler(CommandHandler("reply",   support_reply_command))

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


# ==================== REDIS-МОСТ (вызовы из Flask-процесса) ====================
# Flask (bubblemanga.service) и бот (bubblemanga-worker.service) — теперь
# разные ОС-процессы, поэтому telegram_app/_bot_loop этого модуля недоступны
# из Flask напрямую. routes.py/main.py кладут задание в Redis через
# bot_bridge.submit_job(), а обработчики ниже выполняют его здесь, в
# процессе, где бот реально живёт. Регистрируются в BOT_BRIDGE_HANDLERS и
# разбираются циклом bot_bridge.run_listener(), запущенным из worker.py.

async def _job_mark_chapter_read(payload):
    await mark_chapter_notification_read(payload['user_id'], payload['chapter_slug'])
    return {'ok': True}


async def _job_create_stars_invoice(payload):
    url = await telegram_app.bot.create_invoice_link(
        title=payload['title'],
        description=payload['description'],
        payload=payload['payload'],
        currency='XTR',
        provider_token='',
        prices=[LabeledPrice(label=payload['price_label'], amount=payload['amount'])],
    )
    return {'ok': True, 'url': url}


async def _job_suggest_similar_manga(payload):
    try:
        await telegram_app.bot.send_message(
            chat_id=payload['chat_id'], text=payload['message'],
            parse_mode='HTML', disable_web_page_preview=True,
        )
    except Exception as e:
        logger.warning(f"_job_suggest_similar_manga: {e}")
    return {'ok': True}


async def _job_admin_broadcast(payload):
    msg = payload['message']
    sent = 0
    for chat_id in payload['chat_ids']:
        try:
            await telegram_app.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
            sent += 1
        except Exception:
            pass
    return {'ok': True, 'sent': sent}


async def _job_admin_alert(payload):
    text = payload['text']
    for admin_id in ADMIN_TELEGRAM_IDS:
        try:
            await telegram_app.bot.send_message(
                chat_id=admin_id, text=f'⚠️ <b>Security Alert</b>\n{text}', parse_mode='HTML',
            )
        except Exception:
            pass
    return {'ok': True}


BOT_BRIDGE_HANDLERS = {
    'mark_chapter_read':     _job_mark_chapter_read,
    'create_stars_invoice':  _job_create_stars_invoice,
    'suggest_similar_manga': _job_suggest_similar_manga,
    'admin_broadcast':       _job_admin_broadcast,
    'admin_alert':           _job_admin_alert,
}


def get_bot_loop():
    """Геттер, а не значение: _bot_loop создаётся в потоке бота уже ПОСЛЕ
    старта слушателя моста, значение на момент импорта было бы None навсегда."""
    return _bot_loop
