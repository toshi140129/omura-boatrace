"""
omura_results.csv に「12R_天気」「12R_気温」「12R_水温」の3列をバックフィル。

- 12R beforeinfo ページから天気テキスト(晴/曇り/雨/雪)・気温・水温を取得
- 既存27列CSVを30列に拡張して書き戻す
- 並列ワーカー16
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
WORKERS = 16
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

NEW_HEADER = [
    "日付",
    "10R_1着", "10R_2着", "10R_3着", "10R_払戻",
    "11R_1着", "11R_2着", "11R_3着", "11R_払戻",
    "12R_1着", "12R_2着", "12R_3着", "12R_払戻",
    "10R_人気", "11R_人気", "12R_人気",
    "10R_風速", "10R_風向", "10R_波高",
    "11R_風速", "11R_風向", "11R_波高",
    "12R_風速", "12R_風向", "12R_波高",
    "節日数", "開催種別",
    "12R_天気", "12R_気温", "12R_水温",
]
N_COLS = len(NEW_HEADER)


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


def parse_weather_text(html):
    """Return (weather, air_temp, water_temp). All '' if missing."""
    if not html or "データがありません" in html:
        return ("", "", "")
    try:
        soup = BeautifulSoup(html, "html.parser")
        w = soup.select_one(".weather1")
        if not w:
            return ("", "", "")
        weather = air_temp = water_temp = ""
        node = w.select_one(".is-weather .weather1_bodyUnitLabelTitle")
        if node:
            weather = node.get_text(strip=True)
        for u in w.select(".weather1_bodyUnit"):
            title = u.select_one(".weather1_bodyUnitLabelTitle")
            data = u.select_one(".weather1_bodyUnitLabelData")
            if not title or not data:
                continue
            t = title.get_text(strip=True)
            d = unicodedata.normalize("NFKC", data.get_text(strip=True))
            m = re.search(r"(-?\d+(?:\.\d+)?)", d)
            if not m:
                continue
            if t == "気温":
                air_temp = m.group(1)
            elif t == "水温":
                water_temp = m.group(1)
        return (weather, air_temp, water_temp)
    except Exception as e:
        print(f"  parse error: {e}", file=sys.stderr, flush=True)
        return ("", "", "")


def fetch_meta(date_label):
    ds = date_label.replace("-", "")
    url = (
        f"https://www.boatrace.jp/owpc/pc/race/beforeinfo?"
        f"rno=12&jcd={JYOJO}&hd={ds}"
    )
    return (date_label, parse_weather_text(fetch_html(url)))


def main():
    if not os.path.exists(CSV_PATH):
        print(f"CSV not found: {CSV_PATH}", file=sys.stderr)
        sys.exit(1)

    with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    if not rows:
        print("空のCSV"); return

    body = []
    for r in rows[1:]:
        if not r or not r[0].strip():
            continue
        if len(r) < N_COLS:
            r = list(r) + [""] * (N_COLS - len(r))
        else:
            r = list(r[:N_COLS])
        body.append(r)

    targets = [r[0] for r in body if not r[27] or not r[28] or not r[29]]
    print(f"全{len(body)}日 / バックフィル対象 {len(targets)}日", flush=True)

    if not targets:
        print("対象なし"); return

    results = {}
    start_t = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for date_label, meta in ex.map(fetch_meta, targets):
            results[date_label] = meta
            done += 1
            if done % 60 == 0 or done == len(targets):
                el = time.time() - start_t
                rate = done / el if el > 0 else 0
                eta = (len(targets) - done) / rate if rate > 0 else 0
                print(
                    f"  [{done}/{len(targets)}] elapsed={el:.0f}s "
                    f"rate={rate:.2f}/s eta={eta:.0f}s",
                    flush=True,
                )

    filled = 0
    for r in body:
        if r[0] in results:
            weather, air_temp, water_temp = results[r[0]]
            if not r[27]: r[27] = weather
            if not r[28]: r[28] = air_temp
            if not r[29]: r[29] = water_temp
            if r[27]: filled += 1

    body.sort(key=lambda x: x[0])
    with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(NEW_HEADER)
        for r in body:
            w.writerow(r)

    print(f"完了: 天気入力済 {filled}/{len(body)}日  → {CSV_PATH}", flush=True)


if __name__ == "__main__":
    main()
