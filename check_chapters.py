# -*- coding: utf-8 -*-
"""
Скрипт проверки и исправления пропущенных глав в манге.

Сравнивает главы из API Senkuro с тем, что есть в PostgreSQL БД,
и выявляет пропуски в нумерации.

Использование:
    python check_chapters.py "перерождение ублюдка из клана меча"
    python check_chapters.py --slug regressing-as-the-reincarnated-bastard-of-the-sword-clan
    python check_chapters.py --slug ... --no-db
    python check_chapters.py --all          # проверить ВСЕ манги в БД
    python check_chapters.py --all --gaps-only  # только манги с пропусками
    python check_chapters.py --all --from 15          # начать с 15-й манги
    python check_chapters.py --all --from some-slug --to other-slug
    python check_chapters.py --slug some-slug --fix   # проверить и добавить пропущенные
    python check_chapters.py --all --fix              # исправить все манги с пропусками
"""

import argparse
import json
import logging
import os
import sys
import time

# Принудительно UTF-8 на Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from senkuro_api import SenkuroAPI
from database import get_db          # PostgreSQL через CompatConn

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_manga_by_slug(slug):
    conn = get_db()
    try:
        return conn.execute(
            "SELECT manga_id, manga_slug, manga_title, branch_id, chapters_count "
            "FROM manga WHERE manga_slug = %s", (slug,)
        ).fetchone()
    finally:
        conn.close()


def get_db_chapters(manga_id):
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT chapter_number, chapter_volume, chapter_name, chapter_slug "
            "FROM chapters WHERE manga_id = %s ORDER BY CAST(chapter_number AS FLOAT)",
            (manga_id,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_all_manga():
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT manga_id, manga_slug, manga_title, branch_id FROM manga ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def save_branch_id(manga_id, branch_id):
    conn = get_db()
    try:
        conn.execute(
            "UPDATE manga SET branch_id = %s WHERE manga_id = %s", (branch_id, manga_id)
        )
        conn.commit()
    finally:
        conn.close()


# ── Chapter number helpers ────────────────────────────────────────────────────

def parse_number(raw):
    """Парсит номер главы в float. Возвращает None если не удаётся."""
    if raw is None:
        return None
    try:
        return float(str(raw).strip())
    except (ValueError, TypeError):
        return None


def find_gaps(numbers):
    """
    Ищет пропуски в списке номеров глав.
    Дробные главы (1.5, 2.5) закрывают дырки между целыми.
    Возвращает список (from_chapter, to_chapter) пропусков.
    """
    valid = sorted(set(n for n in numbers if n is not None))
    if len(valid) < 2:
        return []

    gaps = []
    for i in range(len(valid) - 1):
        cur = valid[i]
        nxt = valid[i + 1]
        if nxt - cur > 1.0 + 1e-9:
            gaps.append((cur, nxt))
    return gaps


def describe_gap(gap_start, gap_end):
    """Человекочитаемое описание пропуска."""
    start_int = int(gap_start) + 1
    end_int   = int(gap_end) if gap_end != int(gap_end) else int(gap_end) - 1
    missing   = list(range(start_int, end_int + 1))
    if len(missing) == 1:
        return f"Гл. {missing[0]}"
    elif len(missing) <= 5:
        return f"Гл. {', '.join(str(m) for m in missing)}"
    else:
        return f"Гл. {missing[0]}-{missing[-1]} ({len(missing)} глав)"


def _list_missing(gap_start, gap_end, existing_numbers):
    """Список конкретных целых номеров, отсутствующих в existing_numbers."""
    existing_set = set(existing_numbers)
    start_int    = int(gap_start) + 1
    end_int      = int(gap_end) if gap_end != int(gap_end) else int(gap_end) - 1
    return [n for n in range(start_int, end_int + 1) if float(n) not in existing_set]


# ── API helpers ───────────────────────────────────────────────────────────────

def fetch_all_chapters_from_api(api, branch_id, manga_slug, delay=2):
    """Загружает ВСЕ главы манги из API через пагинацию."""
    chapters = []
    after    = None

    while True:
        result = api.fetch_manga_chapters_page(branch_id, after)
        if not result:
            break

        edges     = result.get("edges") or []
        page_info = result.get("pageInfo") or {}

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

def analyze_manga(api, manga_slug, branch_id, manga_id=None, no_db=False, delay=2):
    api_chapters = fetch_all_chapters_from_api(api, branch_id, manga_slug, delay)
    api_numbers  = [parse_number(ch["chapter_number"]) for ch in api_chapters]
    api_valid    = sorted(set(n for n in api_numbers if n is not None))

    result = {
        "manga_slug":    manga_slug,
        "api_count":     len(api_chapters),
        "api_numbers":   api_valid,
        "api_gaps":      find_gaps(api_valid),
        "db_count":      0,
        "db_missing":    [],
        "db_extra":      [],
        "no_number":     [ch for ch in api_chapters if parse_number(ch["chapter_number"]) is None],
        "_api_chapters": api_chapters,  # для --fix
    }

    if not no_db and manga_id:
        db_chapters = get_db_chapters(manga_id)
        db_numbers  = sorted(set(
            n for n in (parse_number(ch["chapter_number"]) for ch in db_chapters)
            if n is not None
        ))
        result["db_count"]   = len(db_chapters)
        result["db_missing"] = sorted(set(api_valid) - set(db_numbers))
        result["db_extra"]   = sorted(set(db_numbers) - set(api_valid))

    return result


def print_report(result, manga_title=""):
    slug  = result["manga_slug"]
    title = manga_title or slug

    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"  slug: {slug}")
    print(f"{'='*60}")
    print(f"  Глав в API:  {result['api_count']}")
    if result["db_count"] > 0:
        print(f"  Глав в DB:   {result['db_count']}")

    gaps = result["api_gaps"]
    if gaps:
        print(f"\n  [!] ПРОПУСКИ В НУМЕРАЦИИ ({len(gaps)} шт.):")
        for gap_start, gap_end in gaps:
            missing = _list_missing(gap_start, gap_end, result["api_numbers"])
            print(f"      {gap_start:.4g} -> {gap_end:.4g}:  {describe_gap(gap_start, gap_end)}")
            if missing:
                preview = ', '.join(str(m) for m in missing[:20])
                print(f"      Пропущено: {preview}" + (" ..." if len(missing) > 20 else ""))
    else:
        print("\n  [OK] Пропусков в нумерации нет")

    if result["no_number"]:
        print(f"\n  [?] Глав без номера: {len(result['no_number'])}")
        for ch in result["no_number"][:5]:
            print(f"      slug={ch['chapter_slug']}  name={ch['chapter_name']!r}")
        if len(result["no_number"]) > 5:
            print(f"      ... и ещё {len(result['no_number']) - 5}")

    if result["db_missing"]:
        print(f"\n  [!] В API есть, в DB ОТСУТСТВУЕТ ({len(result['db_missing'])} глав):")
        nums    = result["db_missing"]
        preview = [str(int(n) if n == int(n) else n) for n in nums[:20]]
        print(f"      {', '.join(preview)}" + (" ..." if len(nums) > 20 else ""))

    if result["db_extra"]:
        print(f"\n  [?] В DB есть, но нет в API ({len(result['db_extra'])} глав):")
        nums    = result["db_extra"]
        preview = [str(int(n) if n == int(n) else n) for n in nums[:10]]
        print(f"      {', '.join(preview)}" + (" ..." if len(nums) > 10 else ""))

    print()


# ── Resolve manga entries ─────────────────────────────────────────────────────

def resolve_manga(api, args):
    """Возвращает список [(manga_slug, branch_id, manga_id, manga_title), ...]"""
    entries = []

    if args.all:
        rows = get_all_manga()
        if not rows:
            print("Таблица manga пустая или БД недоступна")
            sys.exit(1)
        return [(r["manga_slug"], r["branch_id"], r["manga_id"], r["manga_title"]) for r in rows]

    slug = args.slug

    if not slug:
        print(f"Поиск: «{args.title}»...")
        results = api.search(args.title, max_results=5)
        if not results:
            print("Ничего не найдено")
            sys.exit(1)
        if len(results) == 1:
            slug = results[0]["manga_slug"]
            print(f"Найдено: {results[0]['manga_title']} ({slug})")
        else:
            print("Найдено несколько вариантов, выберите:")
            for i, r in enumerate(results):
                print(f"  [{i+1}] {r['manga_title']} ({r['manga_slug']})")
            try:
                choice = int(input("Номер (Enter=1): ").strip() or "1") - 1
            except (ValueError, KeyboardInterrupt):
                choice = 0
            slug = results[choice]["manga_slug"]

    manga_id    = None
    manga_title = slug
    branch_id   = None

    if not args.no_db:
        row = get_manga_by_slug(slug)
        if row:
            manga_id    = row["manga_id"]
            manga_title = row["manga_title"] or slug
            branch_id   = row["branch_id"]

    if not branch_id:
        print(f"Получаем данные из API для «{slug}»...")
        details = api.fetch_manga(slug)
        if not details:
            print(f"Не удалось получить данные для slug={slug!r}")
            sys.exit(1)
        if manga_id is None:
            manga_id = details.get("manga_id")
        manga_title = details.get("manga_title") or manga_title
        branch_id   = details.get("branch_id")
        if manga_id and branch_id and not args.no_db:
            save_branch_id(manga_id, branch_id)

    entries.append((slug, branch_id, manga_id, manga_title))
    return entries


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Проверка пропущенных глав (PostgreSQL)")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("title",  nargs="?", default=None,
                       help="Название манги для поиска")
    group.add_argument("--slug", default=None,
                       help="Точный slug манги")
    group.add_argument("--all",  action="store_true",
                       help="Проверить все манги из БД")

    p.add_argument("--no-db",     action="store_true",
                   help="Только анализировать API, без сравнения с БД")
    p.add_argument("--delay",     type=float, default=1,
                   help="Пауза между запросами (сек, минимум 0.9)")
    p.add_argument("--gaps-only", action="store_true",
                   help="При --all показывать только манги с пропусками")
    p.add_argument("--verbose",   action="store_true",
                   help="Показывать все номера глав")
    p.add_argument("--from",  dest="from_manga", default=None,
                   help="При --all: с какой манги начать (slug, часть названия или номер)")
    p.add_argument("--to",    dest="to_manga",   default=None,
                   help="При --all: на какой манге закончить (slug, часть названия или номер)")
    p.add_argument("--fix",   action="store_true",
                   help="Скачать и добавить в БД главы, которые есть в API но отсутствуют в DB")
    return p.parse_args()


# ── DB write ──────────────────────────────────────────────────────────────────

def fix_missing_chapters(api, manga_slug, manga_id, manga_title, db_missing_numbers, api_chapters, delay):
    """Скачивает страницы пропущенных глав и вставляет их в БД."""

    api_by_number = {}
    for ch in api_chapters:
        n = parse_number(ch["chapter_number"])
        if n is not None:
            api_by_number[n] = ch

    to_fix = [api_by_number[n] for n in db_missing_numbers if n in api_by_number]
    not_found = [n for n in db_missing_numbers if n not in api_by_number]

    if not_found:
        print(f"  [?] Не найдены в API: {', '.join(str(int(n) if n == int(n) else n) for n in not_found[:10])}")

    if not to_fix:
        print("  Нечего добавлять.")
        return 0

    print(f"  Добавляю {len(to_fix)} глав...")
    inserted = 0

    for ch in to_fix:
        chapter_slug = ch["chapter_slug"]
        num_label    = ch["chapter_number"]

        pages_raw = api.fetch_chapter_pages(chapter_slug)
        page_urls = [
            p.get("image", {}).get("compress", {}).get("url", "")
            for p in pages_raw
            if p.get("image", {}).get("compress", {}).get("url")
        ]

        chapter_url = f"/read/{manga_slug}/{chapter_slug}"

        conn = get_db()
        try:
            conn.execute(
                """INSERT INTO chapters
                       (manga_id, chapter_id, chapter_slug, chapter_number, chapter_volume,
                        chapter_name, chapter_url, pages_json, pages_count, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (chapter_id) DO NOTHING""",
                (
                    manga_id, ch["chapter_id"], chapter_slug,
                    ch["chapter_number"], ch.get("chapter_volume"), ch.get("chapter_name"),
                    chapter_url, json.dumps(page_urls), len(page_urls),
                    ch.get("created_at"),
                )
            )
            conn.commit()
            inserted += 1
            print(f"    ✓ Гл. {num_label} — {len(page_urls)} стр.")
        except Exception as e:
            print(f"    ✗ Гл. {num_label}: {e}")
        finally:
            conn.close()

        time.sleep(delay)

    # Обновляем manga_tracker до актуальной последней главы из API,
    # чтобы background_checker не счёл только что добавленные главы «новыми»
    # и не разослал уведомления подписчикам.
    if inserted > 0 and manga_id and api_chapters:
        latest = max(api_chapters, key=lambda ch: parse_number(ch.get("chapter_number")) or 0)
        latest_id  = latest.get("chapter_id")
        latest_num = parse_number(latest.get("chapter_number"))
        if latest_id and latest_num is not None:
            conn = get_db()
            try:
                conn.execute(
                    """INSERT INTO manga_tracker
                           (manga_id, last_chapter_id, last_chapter_number, last_checked_at)
                       VALUES (%s, %s, %s, NOW())
                       ON CONFLICT (manga_id) DO UPDATE SET
                           last_chapter_id     = EXCLUDED.last_chapter_id,
                           last_chapter_number = EXCLUDED.last_chapter_number,
                           last_checked_at     = NOW()
                       WHERE manga_tracker.last_chapter_number IS NULL
                          OR EXCLUDED.last_chapter_number >= manga_tracker.last_chapter_number""",
                    (manga_id, latest_id, latest_num)
                )
                conn.commit()
                print(f"  ✓ Трекер обновлён → гл. {latest_num:.4g}")
            except Exception as e:
                print(f"  [!] Не удалось обновить manga_tracker: {e}")
            finally:
                conn.close()

    return inserted


def _find_entry_index(entries, key, label):
    """Ищет индекс манги по номеру, slug или части названия."""
    if key is None:
        return None
    # По номеру (1-based)
    try:
        idx = int(key) - 1
        if 0 <= idx < len(entries):
            return idx
        print(f"[!] Номер {key} вне диапазона 1–{len(entries)}, игнорируется --{label}")
        return None
    except ValueError:
        pass
    # По slug или части названия (без учёта регистра)
    key_lower = key.lower()
    for i, (slug, _, _, title) in enumerate(entries):
        if key_lower == slug.lower() or key_lower in (title or "").lower():
            return i
    print(f"[!] --{label}={key!r}: манга не найдена, игнорируется")
    return None


def main():
    args    = parse_args()
    # Минимальный перерыв между запросами — 0.9 сек
    args.delay = max(0.9, args.delay)

    if args.fix and args.no_db:
        print("[!] --fix несовместим с --no-db, игнорируется")
        args.fix = False

    api     = SenkuroAPI()
    entries = resolve_manga(api, args)

    if args.all and (args.from_manga or args.to_manga):
        from_idx = _find_entry_index(entries, args.from_manga, "from")
        to_idx   = _find_entry_index(entries, args.to_manga,   "to")
        start = from_idx if from_idx is not None else 0
        end   = (to_idx + 1) if to_idx is not None else len(entries)
        entries = entries[start:end]
        if from_idx is not None or to_idx is not None:
            s = entries[0][0] if entries else "?"
            e = entries[-1][0] if entries else "?"
            print(f"Диапазон: [{start+1}] {s} → [{start+len(entries)}] {e}")

    print(f"\nМанг для проверки: {len(entries)}")

    total_gaps    = 0
    total_missing = 0
    mangas_with_issues = []

    for i, (slug, branch_id, manga_id, manga_title) in enumerate(entries, 1):
        if len(entries) > 1:
            print(f"\r[{i}/{len(entries)}] {slug}...", end="", flush=True)

        if not branch_id:
            if len(entries) == 1:
                print(f"Не найден branch_id для {slug}")
            continue

        result = analyze_manga(api, slug, branch_id, manga_id,
                               no_db=args.no_db, delay=args.delay)
        result["manga_title"] = manga_title

        has_issues = bool(result["api_gaps"] or result["db_missing"] or result["no_number"])
        total_gaps    += len(result["api_gaps"])
        total_missing += len(result["db_missing"])

        if args.gaps_only and not has_issues:
            continue
        if len(entries) > 1 and not has_issues:
            continue

        print()
        print_report(result, manga_title)
        if has_issues:
            mangas_with_issues.append(manga_title or slug)

        if args.fix and result["db_missing"] and manga_id:
            fixed = fix_missing_chapters(
                api, slug, manga_id, manga_title or slug,
                result["db_missing"], result.get("_api_chapters", []),
                args.delay,
            )
            total_missing -= fixed
            print(f"  → Добавлено: {fixed} глав")

        if args.verbose and result["api_numbers"]:
            nums = result["api_numbers"]
            print(f"  Все номера ({len(nums)}): {', '.join(f'{n:.4g}' for n in nums)}")

        if args.all and i < len(entries):
            time.sleep(args.delay)

    if args.all:
        print(f"\n{'='*60}")
        print(f"ИТОГ: проверено {len(entries)} манг")
        print(f"  С пропусками в нумерации: {len(mangas_with_issues)}")
        print(f"  Всего пропусков: {total_gaps}")
        print(f"  Глав отсутствующих в DB: {total_missing}")
        if mangas_with_issues:
            print("\nМанги с проблемами:")
            for t in mangas_with_issues:
                print(f"  - {t}")


if __name__ == "__main__":
    main()
