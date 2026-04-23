"""
既存 omura_results.csv の各行について、欠落している気象列
（10R_風速/10R_風向/10R_波高 〜 12R_風速/12R_風向/12R_波高）を
beforeinfo ページから並列HTTP取得して埋める。

- ヘッダーが13列・16列・25列のいずれでも対応
- すでに気象3列すべて埋まっている行はスキップ
- 並列 16（約20分）
"""
import csv
import os
import re
import sys
import time
import unicodedata
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

HERE = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(HERE, "omura_results.csv")
JYOJO = "24"
MAX_WORKERS = 16

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

HEADER = [
    "日付",
    "10R_1着", "10R_2着", "10R_3着", "10R_払戻",
    "11R_1着", "11R_2着", "11R_3着", "11R_払戻",
    "12R_1着", "12R_2着", "12R_3着", "12R_払戻",
    "10R_人気", "11R_人気", "12R_人気",
    "10R_風速", "10R_風向", "10R_波高",
    "11R_風速", "11R_風向", "11R_波高",
    "12R_風速", "12R_風向", "12R_波高",
]


def fetch_html(url, retries=3):
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


def parse_weather(html):
    if not html or "データがありません" in html:
        return ("", "", "")
    try:
        soup = BeautifulSoup(html, "html.parser")
        w = soup.select_one(".weather1")
        if not w:
            return ("", "", "")
        wind_speed = wind_dir = wave = ""
        node = w.select_one(".is-wind .weather1_bodyUnitLabelData")
        if node:
            m = re.search(
                r"(\d+)", unicodedata.normalize("NFKC", node.get_text(strip=True))
            )
            if m:
                wind_speed = m.group(1)
        node = w.select_one(".is-wave .weather1_bodyUnitLabelData")
        if node:
            m = re.search(
                r"(\d+)", unicodedata.normalize("NFKC", node.get_text(strip=True))
            )
            if m:
                wave = m.group(1)
        node = w.select_one(".is-windDirection .weather1_bodyUnitImage")
        if node:
            for cls in node.get("class", []):
                m = re.match(r"is-wind(\d+)", cls)
                if m:
                    wind_dir = m.group(1)
                    break
        return (wind_speed, wind_dir, wave)
    except Exception as e:
        print(f"  parse error: {e}", file=sys.stderr, flush=True)
        return ("", "", "")


def fetch_one(args):
    date_str, rno = args
    url = (
        f"https://www.boatrace.jp/owpc/pc/race/beforeinfo?"
        f"rno={rno}&jcd={JYOJO}&hd={date_str}"
    )
    return (date_str, rno, parse_weather(fetch_html(url)))


def read_rows():
    with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
        return list(csv.reader(f))


def write_rows(rows):
    with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerows(rows)


def main():
    rows = read_rows()
    if not rows:
        print("CSV空", flush=True)
        return

    data_rows = rows[1:]
    print(f"対象日数: {len(data_rows)}", flush=True)

    tasks = []
    for row in data_rows:
        if not row or not row[0].strip():
            continue
        # Pad row to 25 columns
        padded = row + [""] * max(0, len(HEADER) - len(row))
        # weather columns are 16..24 in new layout
        if all(padded[16:25]):
            continue
        date_str = row[0].replace("-", "")
        for rno in (10, 11, 12):
            tasks.append((date_str, rno))

    print(f"フェッチ件数: {len(tasks)} (並列 {MAX_WORKERS})", flush=True)
    if not tasks:
        print("取得対象なし", flush=True)
        return

    start_t = time.time()
    results = {}
    done_count = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for date_str, rno, weather in ex.map(fetch_one, tasks):
            results[(date_str, rno)] = weather
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

    # Build output rows
    out = [HEADER]
    filled = 0
    missing_any = 0
    for row in data_rows:
        if not row or not row[0].strip():
            continue
        padded = row[: len(HEADER)] + [""] * max(0, len(HEADER) - len(row))
        date_str = row[0].replace("-", "")
        w10 = results.get((date_str, 10), (padded[16], padded[17], padded[18]))
        w11 = results.get((date_str, 11), (padded[19], padded[20], padded[21]))
        w12 = results.get((date_str, 12), (padded[22], padded[23], padded[24]))
        padded[16], padded[17], padded[18] = w10[0] or padded[16], w10[1] or padded[17], w10[2] or padded[18]
        padded[19], padded[20], padded[21] = w11[0] or padded[19], w11[1] or padded[20], w11[2] or padded[21]
        padded[22], padded[23], padded[24] = w12[0] or padded[22], w12[1] or padded[23], w12[2] or padded[24]
        if all(padded[16:25]):
            filled += 1
        else:
            missing_any += 1
        out.append(padded)

    write_rows(out)
    print(
        f"CSV更新完了: 気象全列埋まり={filled}日  一部欠損={missing_any}日 → {CSV_PATH}",
        flush=True,
    )


if __name__ == "__main__":
    main()
