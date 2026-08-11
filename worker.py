#!/usr/bin/env python3
"""
worker.py — фоновый воркер BubbleManga.
Запускается отдельным systemd-сервисом (bubblemanga-worker).
Не поднимает HTTP-сервер.

Процессы:
  - Telegram-бот (polling)
  - Фоновый чекер глав, дайджест, premium expiry
  - DonatePay поллер
  - Redis-мост bot_bridge: разбирает задания, которые Flask-процесс
    (bubblemanga.service) кладёт в очередь, т.к. он не видит event loop
    бота напрямую (другой процесс)
"""
import os
import sys
import threading
import logging
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger('worker')


def main():
    # ── Инициализация схемы БД ─────────────────────────────────────────────
    from database import _USE_PG, init_pg_schema, init_db
    if _USE_PG:
        init_pg_schema()
    else:
        init_db()
    logger.info('DB schema OK')

    # ── DonatePay поллер ───────────────────────────────────────────────────
    from config import DONATEPAY_API_KEY
    if DONATEPAY_API_KEY:
        from dp_poller import run_poller
        t_dp = threading.Thread(target=run_poller, daemon=True, name='dp-poller')
        t_dp.start()
        logger.info('DonatePay poller thread started')
    else:
        logger.warning('DONATEPAY_API_KEY not set — poller disabled')

    # ── Фоновый чекер глав ────────────────────────────────────────────────
    # Импорт main запускает Flask-app в памяти (без HTTP-сервера) и
    # регистрирует blueprint. Это нормально — сервер не стартует.
    from main import background_checker
    t_checker = threading.Thread(target=background_checker, daemon=True, name='bg-checker')
    t_checker.start()
    logger.info('Background checker thread started')

    # ── Telegram-бот (блокирующий запуск в своём потоке) ────────────────────
    import bot as _bot_module
    logger.info('Starting Telegram bot...')
    bot_thread = _bot_module.run_telegram_bot()

    # ── Redis-мост: слушатель заданий от Flask-процесса ─────────────────────
    from bot_bridge import run_listener
    t_bridge = threading.Thread(
        target=run_listener,
        args=(_bot_module.BOT_BRIDGE_HANDLERS, _bot_module.get_bot_loop),
        daemon=True, name='bot-bridge-listener'
    )
    t_bridge.start()
    logger.info('Bot bridge listener thread started')

    # Держим main-поток живым
    try:
        while True:
            # Перезапускаем бота если упал (run_telegram_bot daemon=True)
            if not bot_thread.is_alive():
                logger.warning('Bot thread died, restarting...')
                bot_thread = _bot_module.run_telegram_bot()
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info('Worker stopped')


if __name__ == '__main__':
    main()
