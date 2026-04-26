"""
omura_results.csv に「節日数」「開催種別」の2列をバックフィルする一回限りのスクリプト。

- 「節日数」: その節の何日目か（1=初日, 2=2日目, …, 6=最終日相当）。raceindex の
  日付タブで is-active1 が付く要素の位置から算出。
- 「開催種別」: R1発走時刻から デイ/ナイター/ミッドナイト を分類。
  R1 < 12:00 → デイ / 12:00<=R1<17:00 → ナイター / R1>=17:00 → ミッドナイト

既存25列CSVを27列に拡張して書き戻す。並列ワーカー16。
"""
import csv
import os
import re
import sys
import time
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


def parse_raceindex(html):
    """Return (節日数, 開催種別). Empty strings if data missing."""
    if not html or "データがありません" in html:
        return ("", "")
    try:
        soup = BeautifulSoup(html, "html.parser")
        date_tabs = [li for li in soup.find_all("li") if li.find(class_="tab2_inner")]
        day_num = ""
        for i, li in enumerate(date_tabs):
            if "is-active1" in (li.get("class") or []):
                day_num = i + 1
                break
        times = re.findall(r"\b(\d{1,2}):(\d{2})\b", html)
        r1_hour = None
        for h, _m in times:
            hi = int(h)
            if 8 <= hi <= 23:
                r1_hour = hi
                break
        kind = ""
        if r1_hour is not None:
            if r1_hour < 12:
                kind = "デイ"
            elif r1_hour < 17:
                kind = "ナイター"
            else:
                kind = "ミッドナイト"
        return (str(day_num) if day_num else "", kind)
    except Exception as e:
        print(f"  parse error: {e}", file=sys.stderr, flush=True)
        return ("", "")


def fetch_meta(date_label):
    ds = date_label.replace("-", "")
    url = (
        f"https://www.boatrace.jp/owpc/pc/race/raceindex?jcd={JYOJO}&hd={ds}"
    )
    return (date_label, parse_raceindex(fetch_html(url)))


def main():
    if not os.path.exists(CSV_PATH):
        print(f"CSV not found: {CSV_PATH}", file=sys.stderr)
        sys.exit(1)

    with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    if not rows:
        print("空のCSV")
        return

    body = []
    for r in rows[1:]:
        if not r or not r[0].strip():
            continue
        if len(r) < 27:
            r = list(r) + [""] * (27 - len(r))
        else:
            r = list(r[:27])
        body.append(r)

    targets = [r[0] for r in body if not r[25] or not r[26]]
    print(f"全{len(body)}日 / バックフィル対象 {len(targets)}日", flush=True)

    if not targets:
        print("対象なし")
        return

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
            day_num, kind = results[r[0]]
            if not r[25]:
                r[25] = day_num
            if not r[26]:
                r[26] = kind
            if r[25] and r[26]:
                filled += 1

    body.sort(key=lambda x: x[0])
    with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(NEW_HEADER)
        for r in body:
            w.writerow(r)

    print(f"完了: 入力済 {filled}/{len(body)}日  → {CSV_PATH}", flush=True)


if __name__ == "__main__":
    main()
