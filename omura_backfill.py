"""
既存 omura_results.csv の全行について、各レースの3連単人気順をboatrace.jpから
HTTP取得して追記する一回限りのバックフィルスクリプト。

出力: omura_results.csv を「日付,10R_1着..12R_払戻,10R_人気,11R_人気,12R_人気」の
16列形式に書き換える。既に16列ある行はスキップ。

並列フェッチ（boatrace.jpは1件9秒前後だが並列耐性あり）で約2-3分で完了。
"""
import csv
import os
import re
import sys
import time
import unicodedata
import urllib.request
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(HERE, "omura_results.csv")
JYOJO = "24"  # 大村
MAX_WORKERS = 16

NEW_HEADER = [
    "日付",
    "10R_1着", "10R_2着", "10R_3着", "10R_払戻",
    "11R_1着", "11R_2着", "11R_3着", "11R_払戻",
    "12R_1着", "12R_2着", "12R_3着", "12R_払戻",
    "10R_人気", "11R_人気", "12R_人気",
]

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


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


def extract_popularity(html):
    if not html or "データがありません" in html:
        return ""
    m = re.search(r"<td[^>]*>\s*3連単\s*</td>(.*?)</tr>", html, re.DOTALL)
    if not m:
        return ""
    tail = m.group(1)
    tds = re.findall(r"<td[^>]*>(.*?)</td>", tail, re.DOTALL)
    if len(tds) < 3:
        return ""
    txt = re.sub(r"<[^>]+>", "", tds[2])
    txt = unicodedata.normalize("NFKC", txt).strip()
    return txt


def fetch_one(args):
    date_str, rno = args
    html = fetch_html(date_str, rno)
    return (date_str, rno, extract_popularity(html))


def read_csv_rows():
    with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
        return list(csv.reader(f))


def write_csv(rows):
    with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerows(rows)


def main():
    rows = read_csv_rows()
    if not rows:
        print("CSV空", flush=True)
        return

    data_rows = rows[1:]
    total = len(data_rows)
    print(f"対象日数: {total}", flush=True)

    # Build fetch tasks
    tasks = []
    for row in data_rows:
        if not row or not row[0].strip():
            continue
        date_str = row[0].replace("-", "")
        for rno in (10, 11, 12):
            tasks.append((date_str, rno))

    print(f"フェッチ件数: {len(tasks)} (並列 {MAX_WORKERS})", flush=True)
    start = time.time()

    results = {}
    done_count = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for date_str, rno, pop in ex.map(fetch_one, tasks):
            results[(date_str, rno)] = pop
            done_count += 1
            if done_count % 60 == 0:
                elapsed = time.time() - start
                rate = done_count / elapsed if elapsed > 0 else 0
                eta = (len(tasks) - done_count) / rate if rate > 0 else 0
                print(
                    f"  [{done_count}/{len(tasks)}] elapsed={elapsed:.1f}s "
                    f"rate={rate:.1f}/s eta={eta:.0f}s",
                    flush=True,
                )

    print(f"全フェッチ完了: {time.time()-start:.1f}s", flush=True)

    # Build output
    out = [NEW_HEADER]
    for row in data_rows:
        if not row or not row[0].strip():
            continue
        date_str = row[0].replace("-", "")
        base = row[:13] + [""] * max(0, 13 - len(row))
        p10 = results.get((date_str, 10), "")
        p11 = results.get((date_str, 11), "")
        p12 = results.get((date_str, 12), "")
        out.append(base + [p10, p11, p12])

    write_csv(out)
    empty = sum(1 for r in out[1:] if not all(r[13:16]))
    print(f"書き出し完了: {len(out)-1}行 (人気取得失敗: {empty}行)", flush=True)


if __name__ == "__main__":
    main()
