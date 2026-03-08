#!/usr/bin/env python3
"""
Скрипт миграции данных SQLite → PostgreSQL для BubbleManga.

Использование:
    DATABASE_URL=postgresql://user:pass@localhost/dbname python migrate_to_pg.py

Предварительно:
    1. pip install psycopg2-binary
    2. Создать БД и пользователя PostgreSQL
    3. Остановить основной сервис (чтобы SQLite не менялся во время миграции)
"""
import os
import sys
import sqlite3
import psycopg2
import psycopg2.extras

SQLITE_DB    = os.path.join(os.path.dirname(__file__), 'manga.db')
SCHEMA_SQL   = os.path.join(os.path.dirname(__file__), 'schema_postgresql.sql')
DATABASE_URL = os.environ.get('DATABASE_URL', '')

if not DATABASE_URL:
    print("❌ Укажи DATABASE_URL=postgresql://user:pass@host/db")
    sys.exit(1)

# Таблицы в порядке FK-зависимостей (родитель всегда раньше ребёнка)
TABLES = [
    'manga',
    'chapters',
    'users',
    'user_stats',
    'user_profile',
    'achievements',
    'shop_items',
    'subscriptions',
    'reading_history',
    'chapters_read',
    'search_history',
    'cache',
    'user_achievements',
    'user_items',
    'collections',
    'collection_items',
    'collection_likes',
    'collection_trophies',
    'notification_queue',
    'site_notifications',
    'admin_broadcasts',
    'comments',
    'comment_likes',
    'coin_purchases',
    'premium_purchases',
    'premium_gifts',
    'quests',
    'user_quests',
    'daily_quests',
    'user_daily_quests',
    'seasons',
    'season_quests',
    'user_season_quests',
    'reading_wishlist',
    'referrals',
    'curator_follows',
    'sent_similar_notifications',
    'xp_log',
]

# Колонки, которые есть только в PostgreSQL — пропускаем при копировании
PG_ONLY_COLS = {'manga': {'search_vector'}}


def get_sqlite_cols(sqlite_conn: sqlite3.Connection, table: str) -> list:
    c = sqlite_conn.cursor()
    try:
        c.execute(f'PRAGMA table_info({table})')
        return [row[1] for row in c.fetchall()]
    except Exception:
        return []


def get_pg_col_info(pg_conn, table: str) -> dict:
    """Returns {col_name: data_type} for all columns in the PostgreSQL table."""
    pg_cur = pg_conn.cursor()
    try:
        pg_cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = %s AND table_schema = 'public'",
            (table,)
        )
        return {row[0]: row[1] for row in pg_cur.fetchall()}
    except Exception:
        return {}
    finally:
        pg_cur.close()


def migrate_table(sqlite_conn: sqlite3.Connection, pg_conn, table: str) -> tuple[int, int]:
    """Копирует таблицу из SQLite в PostgreSQL.

    Коммитит СРАЗУ после вставки, чтобы FK следующих таблиц видели данные.
    Строки с FK-нарушениями пропускаются (вставляются остальные).

    Returns:
        (inserted, skipped)
    """
    cols = get_sqlite_cols(sqlite_conn, table)
    if not cols:
        print(f"  ⚠️  {table}: не найдена в SQLite, пропускаем")
        return 0, 0

    pg_only = PG_ONLY_COLS.get(table, set())
    pg_col_types = get_pg_col_info(pg_conn, table)

    # Keep only columns that exist in both SQLite and PostgreSQL
    if pg_col_types:
        cols = [c for c in cols if c not in pg_only and c in pg_col_types]
    else:
        cols = [c for c in cols if c not in pg_only]

    # Which columns are BOOLEAN in PostgreSQL (SQLite stores them as 0/1 int)
    bool_cols = {c for c in cols if pg_col_types.get(c) == 'boolean'}

    sqlite_c = sqlite_conn.cursor()
    try:
        sqlite_c.execute(f'SELECT COUNT(*) FROM {table}')
        total = sqlite_c.fetchone()[0]
    except Exception:
        print(f"  ⚠️  {table}: не удалось получить кол-во строк")
        return 0, 0

    if total == 0:
        print(f"  ·  {table}: пусто")
        return 0, 0

    cols_sql   = ', '.join(cols)
    placeholders = ', '.join(['%s'] * len(cols))
    insert_sql = (
        f'INSERT INTO {table} ({cols_sql}) VALUES ({placeholders}) '
        f'ON CONFLICT DO NOTHING'
    )

    sqlite_c.execute(f'SELECT {cols_sql} FROM {table}')
    inserted = 0
    skipped  = 0
    other_errors: list[str] = []

    for row in sqlite_c:
        # Coerce SQLite int 0/1 → Python bool for PostgreSQL BOOLEAN columns
        if bool_cols:
            row = tuple(
                bool(v) if (col in bool_cols and isinstance(v, int)) else v
                for col, v in zip(cols, row)
            )
        else:
            row = tuple(row)

        # Каждая строка — отдельный SAVEPOINT, чтобы FK-нарушение не убивало транзакцию
        pg_cur = pg_conn.cursor()
        pg_cur.execute("SAVEPOINT row_sp")
        try:
            pg_cur.execute(insert_sql, row)
            pg_cur.execute("RELEASE SAVEPOINT row_sp")
            inserted += 1
        except psycopg2.errors.ForeignKeyViolation:
            pg_cur.execute("ROLLBACK TO SAVEPOINT row_sp")
            skipped += 1
        except Exception as e:
            pg_cur.execute("ROLLBACK TO SAVEPOINT row_sp")
            skipped += 1
            err = str(e).split('\n')[0]
            if err not in other_errors:
                other_errors.append(err)
                if len(other_errors) <= 3:
                    print(f"    ⚠️  ошибка вставки: {err}")
        finally:
            pg_cur.close()

    # Коммитим таблицу СРАЗУ — следующие таблицы должны видеть эти данные через FK
    pg_conn.commit()

    status = f"  ✅ {table}: {inserted}/{total}"
    if skipped:
        status += f" (пропущено {skipped} из-за FK/конфликтов)"
    print(status)
    return inserted, skipped


def reset_sequences(pg_conn, tables: list):
    """Сбрасывает sequences после вставки с явными id."""
    pg_cur = pg_conn.cursor()
    for table in tables:
        try:
            pg_cur.execute(
                f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "
                f"COALESCE(MAX(id), 1)) FROM {table}"
            )
        except Exception:
            pg_conn.rollback()
    pg_conn.commit()
    pg_cur.close()


def populate_search_vectors(pg_conn):
    print("\n🔍 Заполняю search_vector для манги...")
    pg_cur = pg_conn.cursor()
    pg_cur.execute("""
        UPDATE manga SET search_vector = to_tsvector('simple',
            COALESCE(manga_title, '') || ' ' ||
            COALESCE(original_name, '') || ' ' ||
            COALESCE(description, ''))
    """)
    pg_conn.commit()
    pg_cur.close()
    print("  ✅ search_vector обновлён")


def main():
    print(f"📦 SQLite:     {SQLITE_DB}")
    safe_url = DATABASE_URL[:DATABASE_URL.rfind('@') + 1] + '***' if '@' in DATABASE_URL else DATABASE_URL
    print(f"🐘 PostgreSQL: {safe_url}")
    print()

    sqlite_conn = sqlite3.connect(SQLITE_DB)
    sqlite_conn.row_factory = sqlite3.Row

    pg_conn = psycopg2.connect(DATABASE_URL)
    pg_conn.autocommit = False

    # Применяем схему
    print("📝 Применяю schema_postgresql.sql...")
    with open(SCHEMA_SQL, 'r', encoding='utf-8') as f:
        schema_sql = f.read()
    try:
        pg_cur = pg_conn.cursor()
        pg_cur.execute(schema_sql)
        pg_conn.commit()
        pg_cur.close()
        print("  ✅ Схема применена\n")
    except Exception as e:
        print(f"  ❌ Ошибка схемы: {e}")
        pg_conn.rollback()
        pg_conn.close()
        sqlite_conn.close()
        sys.exit(1)

    # Мигрируем данные
    print("📋 Копирую данные...\n")
    total_inserted = 0
    total_skipped  = 0
    for table in TABLES:
        ins, skp = migrate_table(sqlite_conn, pg_conn, table)
        total_inserted += ins
        total_skipped  += skp

    # Обновляем sequences
    print("\n🔢 Обновляю sequences...")
    reset_sequences(pg_conn, TABLES)
    print("  ✅ Готово")

    # Заполняем поисковые векторы
    populate_search_vectors(pg_conn)

    pg_conn.close()
    sqlite_conn.close()

    print(f"\n🎉 Миграция завершена!")
    print(f"   Перенесено строк:  {total_inserted}")
    if total_skipped:
        print(f"   Пропущено строк:   {total_skipped} (FK-нарушения или конфликты)")
    print()
    print("Дальнейшие шаги:")
    print("  1. Пропиши DATABASE_URL в переменные окружения сервиса")
    print("  2. Запусти: systemctl start <service-name>")
    print("  3. Проверь /top, /catalog, поиск")


if __name__ == '__main__':
    main()
