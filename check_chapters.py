# -*- coding: utf-8 -*-
"""
Скрипт проверки пропущенных глав в манге.

Сравнивает главы из API Senkuro с тем, что есть в manga.db,
и выявляет пропуски в нумерации.

Использование:
    python check_chapters.py "перерождение ублюдка из клана меча"
    python check_chapters.py --slug rebirth-of-the-sword-clan-bastard
    python check_chapters.py --slug rebirth-of-the-sword-clan-bastard --no-db
    python check_chapters.py --all          # проверить ВСЕ манги в БД
    python check_chapters.py --all --gaps-only  # только манги с пропусками
"""

import argparse
import logging
import os
import sqlite3
import sys
import time
from collections import defaultdict

# Принудительно UTF-8 на Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.dirname(__file__))
from senkuro_api import SenkuroAPI

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB  = os.path.join(SCRIPT_DIR, "manga.db")


# ── DB helpers ────────────────────────────────────────────────────────────────

def open_db(db_path):
    if not os.path.exists(db_path):
        return None
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _has_table(conn, table):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def get_manga_by_slug(conn, slug):
    if not _has_table(conn, "manga"):
        return None
    return conn.execute(
        "SELECT manga_id, manga_slug, manga_title, branch_id, chapters_count "
        "FROM manga WHERE manga_slug = ?", (slug,)
    ).fetchone()


def get_db_chapters(conn, manga_id):
    if not _has_table(conn, "chapters"):
        return []
    rows = conn.execute(
        "SELECT chapter_number, chapter_volume, chapter_name, chapter_slug "
        "FROM chapters WHERE manga_id = ? ORDER BY CAST(chapter_number AS REAL)",
        (manga_id,)
    ).fetchall()
    return [dict(r) for r in rows]


# ── Chapter number helpers ────────────────────────────────────────────────────

def parse_number(raw):
    """Парсим номер главы в float. Возвращает None если не удаётся."""
    if raw is None:
        return None
    try:
        return float(str(raw).strip())
    except (ValueError, TypeError):
        return None


def find_gaps(numbers):
    """
    Ищет пропуски в списке номеров глав.

    Логика:
    - Сортируем уникальные целые части номеров
    - Если между двумя соседними целыми N и M нет ни одной главы
      с номером в диапазоне (N, M) — это пропуск.
    - Дробные главы (1.5, 2.5) НЕ считаются пропусками сами по себе,
      но закрывают "дырку" между соседними целыми.

    Возвращает список (from_chapter, to_chapter) пропусков.
    """
    valid = sorted(set(n for n in numbers if n is not None))
    if len(valid) < 2:
        return []

    gaps = []
    for i in range(len(valid) - 1):
        cur = valid[i]
        nxt = valid[i + 1]
        # Пропуск если следующий номер больше текущего более чем на 1.0
        # и разрыв не заполнен дробными главами в этом диапазоне
        if nxt - cur > 1.0 + 1e-9:
            # Проверяем: нет ли промежуточных глав которые уже есть
            # (они уже в valid, так что если gap > 1, значит реально пропуск)
            missing_start = cur
            missing_end   = nxt
            gaps.append((missing_start, missing_end))

    return gaps


def describe_gap(gap_start, gap_end):
    """Человекочитаемое описание пропуска."""
    # Определяем конкретные пропущенные целые числа
    start_int = int(gap_start) + 1
    end_int   = int(gap_end)
    if gap_end == int(gap_end):
        end_int = int(gap_end) - 1

    missing = list(range(start_int, end_int + 1))
    if len(missing) == 1:
        return f"Гл. {missing[0]}"
    elif len(missing) <= 5:
        return f"Гл. {', '.join(str(m) for m in missing)}"
    else:
        return f"Гл. {missing[0]}–{missing[-1]} ({len(missing)} глав)"


# ── Fetching from API ─────────────────────────────────────────────────────────

def fetch_all_chapters_from_api(api, branch_id, manga_slug, delay=0.2):
    """Загружает ВСЕ главы манги из API через пагинацию."""
    chapters = []
    after = None
    page  = 0

    while True:
        page += 1
        result = api.fetch_manga_chapters_page(branch_id, after)
        if not result:
            break

        edges     = result.get("edges", []) or []
        page_info = result.get("pageInfo", {}) or {}

        for edge in edges:
            node = edge.get("node") or {}
            if not node:
                continue
            chapters.append({
                "chapter_id":     node.get("id"),
                "chapter_slug":   node.get("slug"),
                "chapter_number": node.get("number"),
                "chapter_volume": node.get("volume"),
                "chapter_name":   node.get("name"),
                "created_at":     node.get("createdAt"),
            })

        if not page_info.get("hasNextPage") or not page_info.get("endCursor"):
            break
        after = page_info["endCursor"]

        if delay > 0:
            time.sleep(delay)

    return chapters


# ── Analysis ──────────────────────────────────────────────────────────────────

def analyze_manga(api, conn, manga_slug, branch_id, manga_id=None,
                  no_db=False, delay=0.2):
    """
    Анализирует главы одной манги.
    Возвращает dict с результатами анализа.
    """
    # Получаем главы из API
    api_chapters = fetch_all_chapters_from_api(api, branch_id, manga_slug, delay)
    api_numbers  = [parse_number(ch["chapter_number"]) for ch in api_chapters]
    api_valid    = sorted(set(n for n in api_numbers if n is not None))

    result = {
        "manga_slug":    manga_slug,
        "api_count":     len(api_chapters),
        "api_numbers":   api_valid,
        "api_gaps":      find_gaps(api_valid),
        "db_count":      0,
        "db_missing":    [],   # в API есть, в DB нет
        "db_extra":      [],   # в DB есть, в API нет (устаревшие)
        "no_number":     [],   # главы без номера
    }

    # Главы без номера
    no_num = [ch for ch in api_chapters if parse_number(ch["chapter_number"]) is None]
    result["no_number"] = no_num

    # Сравнение с DB
    if not no_db and conn and manga_id:
        db_chapters = get_db_chapters(conn, manga_id)
        db_numbers  = sorted(set(
            n for n in (parse_number(ch["chapter_number"]) for ch in db_chapters)
            if n is not None
        ))
        result["db_count"] = len(db_chapters)

        api_set = set(api_valid)
        db_set  = set(db_numbers)
        result["db_missing"] = sorted(api_set - db_set)
        result["db_extra"]   = sorted(db_set - api_set)

    return result


def print_report(result, manga_title=""):
    """Выводит читаемый отчёт по результатам анализа."""
    slug  = result["manga_slug"]
    title = manga_title or slug

    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"  slug: {slug}")
    print(f"{'='*60}")
    print(f"  Глав в API:  {result['api_count']}")
    if result["db_count"] > 0:
        print(f"  Глав в DB:   {result['db_count']}")

    # Пропуски в нумерации (API)
    gaps = result["api_gaps"]
    if gaps:
        print(f"\n  [!] ПРОПУСКИ В НУМЕРАЦИИ ({len(gaps)} шт.):")
        for g in gaps:
            gap_start, gap_end = g
            missing = _list_missing(gap_start, gap_end, result["api_numbers"])
            print(f"      {gap_start:.4g} → {gap_end:.4g}:  {describe_gap(gap_start, gap_end)}")
            if missing:
                preview = ', '.join(str(m) for m in missing[:20])
                print(f"      Пропущено: {preview}" + (" ..." if len(missing) > 20 else ""))
    else:
        print("\n  [OK] Пропусков в нумерации нет")

    # Главы без номера
    if result["no_number"]:
        print(f"\n  [?] Глав без номера: {len(result['no_number'])}")
        for ch in result["no_number"][:5]:
            print(f"      slug={ch['chapter_slug']}  name={ch['chapter_name']!r}")
        if len(result["no_number"]) > 5:
            print(f"      ... и ещё {len(result['no_number']) - 5}")

    # Главы в API, которых нет в DB
    if result["db_missing"]:
        print(f"\n  [!] В API есть, в DB ОТСУТСТВУЕТ ({len(result['db_missing'])} глав):")
        nums = result["db_missing"]
        preview = [str(int(n) if n == int(n) else n) for n in nums[:20]]
        print(f"      {', '.join(preview)}" + (" ..." if len(nums) > 20 else ""))

    # Лишние в DB
    if result["db_extra"]:
        print(f"\n  [?] В DB есть, но нет в API ({len(result['db_extra'])} глав):")
        nums = result["db_extra"]
        preview = [str(int(n) if n == int(n) else n) for n in nums[:10]]
        print(f"      {', '.join(preview)}" + (" ..." if len(nums) > 10 else ""))

    print()


def _list_missing(gap_start, gap_end, existing_numbers):
    """Возвращает список целых номеров, которых нет в existing_numbers."""
    existing_set = set(existing_numbers)
    start_int = int(gap_start) + 1
    end_int   = int(gap_end)
    if gap_end == int(gap_end):
        end_int = int(gap_end) - 1

    return [n for n in range(start_int, end_int + 1) if float(n) not in existing_set]


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Проверка пропущенных глав в манге"
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("title",   nargs="?", default=None,
                       help="Название манги для поиска (на любом языке)")
    group.add_argument("--slug",  default=None,
                       help="Точный slug манги (без поиска)")
    group.add_argument("--all",   action="store_true",
                       help="Проверить все манги из БД")

    p.add_argument("--db",         default=DEFAULT_DB,
                   help=f"Путь к manga.db (по умолчанию: {DEFAULT_DB})")
    p.add_argument("--no-db",      action="store_true",
                   help="Не сравнивать с DB, только анализировать API")
    p.add_argument("--delay",      type=float, default=0.2,
                   help="Пауза между запросами страниц (сек, default=0.2)")
    p.add_argument("--gaps-only",  action="store_true",
                   help="При --all показывать только манги с пропусками")
    p.add_argument("--verbose",    action="store_true",
                   help="Показывать все номера глав")
    return p.parse_args()


def resolve_manga(api, conn, args):
    """
    Возвращает список [(manga_slug, branch_id, manga_id, manga_title), ...]
    """
    entries = []

    if args.all:
        if not conn or not _has_table(conn, "manga"):
            print("❌ DB не найдена или пустая, --all требует заполненную manga.db")
            sys.exit(1)
        rows = conn.execute(
            "SELECT manga_id, manga_slug, manga_title, branch_id FROM manga ORDER BY id"
        ).fetchall()
        for r in rows:
            entries.append((r["manga_slug"], r["branch_id"], r["manga_id"], r["manga_title"]))
        return entries

    slug = args.slug

    if not slug:
        # Поиск по названию
        title = args.title
        print(f"🔍 Поиск по запросу: «{title}»...")
        results = api.search(title, max_results=5)
        if not results:
            print("❌ Ничего не найдено")
            sys.exit(1)
        if len(results) == 1:
            slug = results[0]["manga_slug"]
            print(f"✅ Найдено: {results[0]['manga_title']} ({slug})")
        else:
            print("Найдено несколько вариантов, выберите:")
            for i, r in enumerate(results):
                print(f"  [{i+1}] {r['manga_title']} ({r['manga_slug']})")
            try:
                choice = int(input("Номер (Enter=1): ").strip() or "1") - 1
            except (ValueError, KeyboardInterrupt):
                choice = 0
            slug = results[choice]["manga_slug"]

    # Получаем branch_id
    manga_id    = None
    manga_title = slug

    if conn:
        row = get_manga_by_slug(conn, slug)
        if row:
            manga_id    = row["manga_id"]
            manga_title = row["manga_title"] or slug
            branch_id   = row["branch_id"]
            if branch_id:
                entries.append((slug, branch_id, manga_id, manga_title))
                return entries

    # Если branch_id не в DB — идём в API
    print(f"⬇️  Получаем данные из API для «{slug}»...")
    details = api.fetch_manga(slug)
    if not details:
        print(f"❌ Не удалось получить данные для slug={slug!r}")
        sys.exit(1)

    if manga_id is None:
        manga_id    = details.get("manga_id")
    manga_title = details.get("manga_title") or manga_title
    branch_id   = details.get("branch_id")

    # Сохраняем branch_id в DB если его не было
    if conn and manga_id and branch_id and _has_table(conn, "manga"):
        conn.execute("UPDATE manga SET branch_id=? WHERE manga_id=?", (branch_id, manga_id))
        conn.commit()

    entries.append((slug, branch_id, manga_id, manga_title))
    return entries


def main():
    args = parse_args()

    conn = open_db(args.db) if not args.no_db else None
    api  = SenkuroAPI()

    entries = resolve_manga(api, conn, args)
    print(f"\n📚 Манг для проверки: {len(entries)}")

    total_gaps    = 0
    total_missing = 0
    mangas_with_issues = []

    for i, (slug, branch_id, manga_id, manga_title) in enumerate(entries, 1):
        if len(entries) > 1:
            print(f"\r[{i}/{len(entries)}] {slug}...", end="", flush=True)

        if not branch_id:
            if len(entries) == 1:
                print(f"❌ Не найден branch_id для {slug}")
            continue

        result = analyze_manga(
            api, conn, slug, branch_id, manga_id,
            no_db=args.no_db, delay=args.delay
        )
        result["manga_title"] = manga_title

        has_issues = bool(result["api_gaps"] or result["db_missing"] or result["no_number"])
        total_gaps    += len(result["api_gaps"])
        total_missing += len(result["db_missing"])

        if args.gaps_only and not has_issues:
            continue

        if len(entries) > 1 and not has_issues:
            continue  # При --all тихо пропускаем чистые манги

        print()  # newline after \r
        print_report(result, manga_title)
        if has_issues:
            mangas_with_issues.append(manga_title or slug)

        if args.verbose and result["api_numbers"]:
            nums = result["api_numbers"]
            preview = [f"{n:.4g}" for n in nums]
            print(f"  Все номера ({len(nums)}): {', '.join(preview)}")

        # Небольшая пауза между манга при --all
        if args.all and i < len(entries):
            time.sleep(args.delay)

    # Итог при --all
    if args.all:
        print(f"\n{'='*60}")
        print(f"ИТОГ: проверено {len(entries)} манг")
        print(f"  Манг с пропусками в нумерации: {len(mangas_with_issues)}")
        print(f"  Всего пропусков: {total_gaps}")
        print(f"  Глав отсутствующих в DB: {total_missing}")
        if mangas_with_issues:
            print("\nМанги с проблемами:")
            for t in mangas_with_issues:
                print(f"  • {t}")

    if conn:
        conn.close()


if __name__ == "__main__":
    main()
