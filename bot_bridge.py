"""
bot_bridge.py — Redis-мост между Flask/gunicorn-процессами и worker.py.

С момента разделения на systemd-сервисы (bubblemanga.service — gunicorn/Flask,
bubblemanga-worker.service — worker.py) Telegram-бот и его event loop
(telegram_app / _bot_loop в bot.py) живут ТОЛЬКО в процессе worker.py.
Flask-хендлеры больше не могут звать asyncio.run_coroutine_threadsafe()
на _bot_loop напрямую — это глобал чужого процесса.

Вместо этого Flask кладёт задание в очередь Redis (submit_job), а слушатель
внутри worker.py (run_listener) разбирает очередь и выполняет задание в
своём, живом event loop бота. Для случаев, когда Flask-хендлеру нужен
результат (например, ссылка на Stars-инвойс), submit_job(wait=True) ждёт
ответ через отдельный reply-ключ (RPC поверх Redis-списков).
"""

import asyncio
import json
import logging
import time
import uuid

import redis

from config import REDIS_URL

logger = logging.getLogger(__name__)

JOBS_KEY = 'bot_bridge:jobs'
REPLY_PREFIX = 'bot_bridge:reply:'
REPLY_TTL = 30  # сек: подчищает reply-ключ, если ответ так и не забрали

_client = None


def _redis():
    global _client
    if _client is None:
        _client = redis.from_url(REDIS_URL, socket_connect_timeout=2, decode_responses=True)
    return _client


def submit_job(job_type: str, payload: dict = None, wait: bool = False, timeout: float = 10.0):
    """
    Ставит задание для процесса-воркера (там живёт Telegram-бот).

    wait=False — fire-and-forget: True если задание успешно положено в очередь,
                 False при ошибке связи с Redis.
    wait=True  — ждёт ответ воркера (RPC): dict с результатом, либо None при
                 таймауте/ошибке связи — вызывающий код должен трактовать это
                 так же, как раньше трактовал "бот недоступен".
    """
    r = _redis()
    job = {'type': job_type, 'payload': payload or {}}
    reply_key = None
    if wait:
        request_id = uuid.uuid4().hex
        job['request_id'] = request_id
        reply_key = REPLY_PREFIX + request_id

    try:
        r.rpush(JOBS_KEY, json.dumps(job))
    except Exception as e:
        logger.warning(f"bot_bridge.submit_job: RPUSH failed ({job_type}): {e}")
        return None if wait else False

    if not wait:
        return True

    try:
        item = r.blpop(reply_key, timeout=timeout)
    except Exception as e:
        logger.warning(f"bot_bridge.submit_job: BLPOP failed ({job_type}): {e}")
        return None
    if not item:
        logger.warning(f"bot_bridge.submit_job: timeout waiting for reply ({job_type})")
        return None
    try:
        return json.loads(item[1])
    except Exception as e:
        logger.warning(f"bot_bridge.submit_job: bad reply payload ({job_type}): {e}")
        return None


def _reply(request_id: str, result: dict):
    if not request_id:
        return
    r = _redis()
    reply_key = REPLY_PREFIX + request_id
    try:
        pipe = r.pipeline()
        pipe.rpush(reply_key, json.dumps(result))
        pipe.expire(reply_key, REPLY_TTL)
        pipe.execute()
    except Exception as e:
        logger.warning(f"bot_bridge._reply: failed to push reply: {e}")


def run_listener(handlers: dict, bot_loop_getter, poll_timeout: float = 1.0):
    """
    Блокирующий цикл — запускать в daemon-потоке процесса worker.py (там,
    где реально живёт event loop бота).

    handlers: {job_type: async def handler(payload) -> dict}
    bot_loop_getter: callable без аргументов, возвращает текущий asyncio-loop
                      бота. Именно callable, а не значение — поток бота
                      создаёт _bot_loop уже ПОСЛЕ старта этого слушателя.
    """
    r = _redis()
    logger.info("bot_bridge: listener started")
    while True:
        try:
            item = r.blpop(JOBS_KEY, timeout=poll_timeout)
        except Exception as e:
            logger.warning(f"bot_bridge.run_listener: BLPOP failed: {e}")
            time.sleep(1)
            continue
        if not item:
            continue

        try:
            job = json.loads(item[1])
        except Exception as e:
            logger.warning(f"bot_bridge.run_listener: bad job payload: {e}")
            continue

        job_type   = job.get('type')
        payload    = job.get('payload') or {}
        request_id = job.get('request_id')

        handler = handlers.get(job_type)
        if handler is None:
            logger.warning(f"bot_bridge.run_listener: unknown job type {job_type!r}")
            _reply(request_id, {'ok': False, 'error': 'unknown_job_type'})
            continue

        loop = bot_loop_getter()
        if not loop or not loop.is_running():
            logger.warning(f"bot_bridge.run_listener: bot loop not ready, dropping {job_type!r}")
            _reply(request_id, {'ok': False, 'error': 'bot_not_ready'})
            continue

        try:
            future = asyncio.run_coroutine_threadsafe(handler(payload), loop)
            result = future.result(timeout=20)
            if not isinstance(result, dict):
                result = {'ok': True, 'result': result}
            _reply(request_id, result)
        except Exception as e:
            logger.warning(f"bot_bridge.run_listener: handler {job_type!r} failed: {e}")
            _reply(request_id, {'ok': False, 'error': str(e)})
