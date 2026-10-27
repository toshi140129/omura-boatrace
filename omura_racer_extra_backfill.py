"""
omura_racer_extra_backfill.py — 支部・出身地・プロペラ・部品交換・安定板の過去分取得

omura_extra_fetch.py が読んでいる出走表（racelist）と直前情報（beforeinfo）のページに
元から載っている項目だが、これまで CSV に落としていなかったものを過去全期間分そろえる。
取得した HTML は extra_html/ に残すので、解析だけやり直すときは再取得しない。

使い方（omura-boatrace フォルダで）
  python omura_racer_extra_backfill.py                 # 12R を全日付分 取得→解析→omura_racers_extra.csv に書き込み
  python omura_racer_extra_backfill.py --races 10 11 12  # 10〜12R
  python omura_racer_extra_backfill.py --parse-only    # 取得せず extra_html/ の HTML を解析し直す
  python omura_racer_extra_backfill.py --dates 2026-10-27 2026-10-26  # 指定日のみ

出力
  omura_racers_extra.csv       1行=1艇（日付,R,枠,登録番号,選手名,支部,出身地,プロペラ,部品交換,安定板）
  extra_backfill_check.csv     1行=1レース の検算用（安定板の判定根拠、支部が空欄の艇数、部品交換あり艇数）
  extra_samples/               検算用に数日分の HTML を残す（コミット対象）
"""
import argparse
import csv
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

import omura_extra_fetch as ef

HERE = os.path.dirname(os.path.abspath(__file__))
HTML_DIR = os.path.join(HERE, "extra_html")
SAMPLE_DIR = os.path.join(HERE, "extra_samples")
CHECK_CSV = os.path.join(HERE, "extra_backfill_check.csv")
CHECK_HEADER = ["日付", "R", "安定板", "安定板根拠", "支部空欄艇数", "部品交換あり艇数", "プロペラあり艇数", "出走表取得", "直前情報取得"]
SAMPLE_DATES = ["2026-10-27", "2026-10-26", "2026-04-09", "2026-04-08", "2025-06-15", "2023-01-01"]
THREADS = 32


def html_path(date_str, page, rno):
    return os.path.join(HTML_DIR, f"{date_str.replace('-', '')}_{page}_{rno}.html")


def fetch_one(key):
    date_str, page, rno = key
    path = html_path(date_str, page, rno)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return key, "cached"
    html = ef.fetch_html(ef.url_for(page, rno, date_str.replace("-", "")))
    if not html:
        return key, "fail"
    os.makedirs(HTML_DIR, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return key, "ok"


def fetch_all(dates, races):
    keys = [(d, page, rno) for d in dates for rno in races for page in ("racelist", "beforeinfo")]
    todo = [k for k in keys if not os.path.exists(html_path(*k))]
    print(f"取得: 対象{len(keys)}ページ 取得済み{len(keys) - len(todo)} → 今回{len(todo)}", flush=True)
    if not todo:
        return
    t0 = time.time()
    fails = 0
    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        for i, (key, st) in enumerate(ex.map(fetch_one, todo), start=1):
            if st == "fail":
                fails += 1
            if i % 50 == 0 or i == len(todo):
                el = time.time() - t0
                print(f"  [{i}/{len(todo)}] {el:.0f}s 失敗{fails} 残り目安 {el / i * (len(todo) - i):.0f}s", flush=True)
    print(f"取得完了 ({time.time() - t0:.0f}s, 失敗{fails})", flush=True)


def read_html(date_str, page, rno):
    path = html_path(date_str, page, rno)
    if not os.path.exists(path):
        return ""
    with open(path, encoding="utf-8") as f:
        return f.read()


def parse_all(dates, races):
    rows, checks = [], []
    for d in dates:
        for rno in races:
            rl = read_html(d, "racelist", rno)
            bi = read_html(d, "beforeinfo", rno)
            if ef.no_data(rl) and ef.no_data(bi):
                continue
            _t, _title, _g, boats = ef.parse_racelist(rl)
            binfo, _starts = ef.parse_beforeinfo(bi)
            stab, why = ("", "")
            if not ef.no_data(bi):
                stab, why = ef.parse_stabilizer_flag(BeautifulSoup(bi, "html.parser"))
            blank_branch = parts = props = 0
            for waku in "123456":
                info = dict(boats.get(waku, {}))
                info.update(binfo.get(waku, {}))
                info["安定板"] = stab
                rows.append([d, rno, waku] + [info.get(k, "") for k in ef.RACERS_EXTRA_HEADER[3:]])
                blank_branch += 0 if info.get("支部") else 1
                parts += 1 if info.get("部品交換") else 0
                props += 1 if info.get("プロペラ") else 0
            checks.append([d, rno, stab, why, blank_branch, parts, props,
                           "0" if ef.no_data(rl) else "1", "0" if ef.no_data(bi) else "1"])
    return rows, checks


def save_samples(races):
    os.makedirs(SAMPLE_DIR, exist_ok=True)
    for d in SAMPLE_DATES:
        for rno in races:
            for page in ("racelist", "beforeinfo"):
                src = html_path(d, page, rno)
                if os.path.exists(src):
                    shutil.copy(src, os.path.join(SAMPLE_DIR, os.path.basename(src)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--races", nargs="*", type=int, default=[12])
    ap.add_argument("--dates", nargs="*")
    ap.add_argument("--parse-only", action="store_true")
    args = ap.parse_args()
    dates = sorted(args.dates) if args.dates else ef.results_dates()
    races = args.races
    if not args.parse_only:
        fetch_all(dates, races)
    rows, checks = parse_all(dates, races)
    print(f"解析: {len(checks)}レース {len(rows)}艇", flush=True)
    ef.append_rows(ef.RACERS_EXTRA_CSV, ef.RACERS_EXTRA_HEADER, rows)
    ef.sort_csv(ef.RACERS_EXTRA_CSV, ef.RACERS_EXTRA_HEADER, (0, 1, 2))
    with open(CHECK_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(CHECK_HEADER)
        w.writerows(checks)
    save_samples(races)
    # 検算のまとめ
    n = len(checks)
    stab1 = sum(1 for c in checks if c[2] == "1")
    blank = sum(1 for c in checks if c[4] == 6)
    parts = sum(1 for c in checks if c[5] > 0)
    print(f"検算: 安定板あり {stab1}/{n}レース, 支部が全艇空欄 {blank}レース, 部品交換あり {parts}レース", flush=True)
    print("書き込み:", ef.RACERS_EXTRA_CSV, CHECK_CSV, SAMPLE_DIR, flush=True)


if __name__ == "__main__":
    main()
