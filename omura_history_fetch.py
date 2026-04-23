"""
指定日付範囲（デフォルト 2023-01-01 〜 今日）の大村ボートレース R10/R11/R12
結果（着順・3連単払戻・人気）を並列HTTPで取得し、既存 omura_results.csv に
マージして16列形式で書き戻す。

- 重複する日付はスキップ
- 「データがありません」や欠損日（未開催）は自動で除外
- 並列ワーカー 16 で約20-30分（対象800日前後の場合）
"""
import csv
import os
import re
import sys
import time
import unicodedata
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta

from bs4 import BeautifulSoup

HERE = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(HERE, "omura_results.csv")
JYOJO = "24"
MAX_WORKERS = 16
START = date(2023, 1, 1)
END = date.today()

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

HEADER = [
    "日付",
    "10R_1着", "10R_2着", "10R_3着", "10R_払戻",
    "11R_1着", "11R_2着", "11R_3着", "11R_払戻",
    "12R_1着", "12R_2着", "12R_3着", "12R_払戻",
    "10R_人気", "11R_人気", "12R_人気",
]


def fetch_html(date_str, race_no, retries=3):
    url = (
        f"https://www.boatrace.jp/owpc/pc/race/raceresult?"
        f"rno={race_no}&jcd={JYOJO}&hd={date_str}"
    )
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as res:
                return res.read().decode("utf-8", errors="replace")
        except Exception as e:
            if attempt == retries - 1:
                print(f"  FETCH FAIL {url}: {e}", file=sys.stderr, flush=True)
                return ""
            time.sleep(1)
    return ""


def parse_race(html):
    """Return (p1,p2,p3,pay,rank). All '' if no data."""
    if not html or "データがありません" in html:
        return ("", "", "", "", "")
    try:
        soup = BeautifulSoup(html, "html.parser")
        tables = soup.select("table.is-w495")
        p1 = p2 = p3 = pay = rank = ""
        if tables:
            for tr in tables[0].select("tbody tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    rnk = unicodedata.normalize("NFKC", tds[0].get_text(strip=True))
                    boat = tds[1].get_text(strip=True)
                    if rnk == "1":
                        p1 = boat
                    elif rnk == "2":
                        p2 = boat
                    elif rnk == "3":
                        p3 = boat
        if len(tables) >= 3:
            for tr in tables[2].select("tbody tr"):
                tds = tr.find_all("td")
                if len(tds) >= 3 and "3連単" in tds[0].get_text():
                    pay = (
                        tds[2].get_text(strip=True)
                        .replace("¥", "")
                        .replace(",", "")
                        .replace("円", "")
                        .strip()
                    )
                    if len(tds) >= 4:
                        rank = unicodedata.normalize(
                            "NFKC", tds[3].get_text(strip=True)
                        )
                    break
        return (p1, p2, p3, pay, rank)
    except Exception as e:
        print(f"  parse error: {e}", file=sys.stderr, flush=True)
        return ("", "", "", "", "")


def fetch_one(args):
    date_str, rno = args
    html = fetch_html(date_str, rno)
    return (date_str, rno, parse_race(html))


def read_existing():
    if not os.path.exists(CSV_PATH):
        return HEADER, {}
    with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    if not rows:
        return HEADER, {}
    header = rows[0]
    data = {}
    for r in rows[1:]:
        if r and r[0].strip():
            # pad to 16 cols if legacy
            padded = r[:16] + [""] * max(0, 16 - len(r))
            data[r[0].strip()] = padded
    return header, data


def write_csv(rows_by_date):
    with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(HEADER)
        for d in sorted(rows_by_date.keys()):
            writer.writerow(rows_by_date[d])


def main():
    print(f"既存CSVを読み込み: {CSV_PATH}", flush=True)
    _, existing = read_existing()
    print(f"既存日数: {len(existing)}", flush=True)

    # Build date list
    all_dates = []
    d = START
    while d <= END:
        all_dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    target_dates = [d for d in all_dates if d not in existing]
    print(
        f"期間: {START} 〜 {END}  全日数={len(all_dates)}  既存={len(existing)}  "
        f"取得対象={len(target_dates)}",
        flush=True,
    )

    if not target_dates:
        print("取得対象なし。終了", flush=True)
        return

    # Fetch R10/R11/R12 in parallel
    tasks = []
    for dl in target_dates:
        ds = dl.replace("-", "")
        for rno in (10, 11, 12):
            tasks.append((ds, rno))

    print(f"フェッチ件数: {len(tasks)} (並列 {MAX_WORKERS})", flush=True)
    start_t = time.time()

    results = {}
    done_count = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for date_str, rno, parsed in ex.map(fetch_one, tasks):
            results[(date_str, rno)] = parsed
            done_count += 1
            if done_count % 120 == 0 or done_count == len(tasks):
                elapsed = time.time() - start_t
                rate = done_count / elapsed if elapsed > 0 else 0
                eta = (len(tasks) - done_count) / rate if rate > 0 else 0
                print(
                    f"  [{done_count}/{len(tasks)}] elapsed={elapsed:.0f}s "
                    f"rate={rate:.2f}/s eta={eta:.0f}s",
                    flush=True,
                )

    print(f"全フェッチ完了: {time.time()-start_t:.0f}s", flush=True)

    # Compose new rows; skip dates with no races at all
    added = 0
    skipped_empty = 0
    for dl in target_dates:
        ds = dl.replace("-", "")
        r10 = results.get((ds, 10), ("", "", "", "", ""))
        r11 = results.get((ds, 11), ("", "", "", "", ""))
        r12 = results.get((ds, 12), ("", "", "", "", ""))
        if not (r10[0] or r11[0] or r12[0]):
            skipped_empty += 1
            continue
        row = [
            dl,
            r10[0], r10[1], r10[2], r10[3],
            r11[0], r11[1], r11[2], r11[3],
            r12[0], r12[1], r12[2], r12[3],
            r10[4], r11[4], r12[4],
        ]
        existing[dl] = row
        added += 1

    print(f"新規追加: {added}日  開催なしスキップ: {skipped_empty}日", flush=True)

    write_csv(existing)
    print(f"CSV書き出し完了: 合計{len(existing)}日分 → {CSV_PATH}", flush=True)


if __name__ == "__main__":
    main()
