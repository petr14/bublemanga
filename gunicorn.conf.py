bind = '127.0.0.1:8000'
workers = 1
threads = 8
worker_class = 'gthread'
timeout = 120
keepalive = 5
reload = True

def post_fork(server, worker):
    import threading
    import sys
    sys.path.insert(0, '/var/tgbot/manga')
    from main import run_telegram_bot, background_checker, init_pg_schema, _USE_PG, init_db

    if not _USE_PG:
        init_db()
    else:
        init_pg_schema()

    checker_thread = threading.Thread(target=background_checker, daemon=True)
    checker_thread.start()

    run_telegram_bot()
