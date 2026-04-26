"""
指定日付範囲（デフォルト 2023-01-01 〜 今日）の大村ボートレース R10/R11/R12
結果（着順・3連単払戻・人気）と気象情報（風速・風向・波高）を並列HTTPで
取得し、既存 omura_results.csv に25列形式でマージして書き戻す。

- 重複する日付はスキップ
- 「データがありません」や欠損日（未開催）は自動で除外
- raceresult と beforeinfo の2ページを並列取得
- 並列ワーカー 16
"""
import csv
import os
import re
import sys
import time
import unicodedata
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

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


def parse_race_result(html):
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


def parse_weather(html):
    """Return (wind_speed, wind_dir, wave). All '' if missing."""
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
                r"(\d+)",
                unicodedata.normalize("NFKC", node.get_text(strip=True)),
            )
            if m:
                wind_speed = m.group(1)
        node = w.select_one(".is-wave .weather1_bodyUnitLabelData")
        if node:
            m = re.search(
                r"(\d+)",
                unicodedata.normalize("NFKC", node.get_text(strip=True)),
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
        print(f"  weather parse error: {e}", file=sys.stderr, flush=True)
        return ("", "", "")


def parse_raceindex(html):
    """Return (節日数, 開催種別)."""
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
        print(f"  raceindex parse error: {e}", file=sys.stderr, flush=True)
        return ("", "")


def fetch_both(args):
    """Fetch raceresult + beforeinfo for a single (date, rno)."""
    date_str, rno = args
    result_url = (
        f"https://www.boatrace.jp/owpc/pc/race/raceresult?"
        f"rno={rno}&jcd={JYOJO}&hd={date_str}"
    )
    before_url = (
        f"https://www.boatrace.jp/owpc/pc/race/beforeinfo?"
        f"rno={rno}&jcd={JYOJO}&hd={date_str}"
    )
    result = parse_race_result(fetch_html(result_url))
    weather = parse_weather(fetch_html(before_url))
    return (date_str, rno, result, weather)


def fetch_meta(date_str):
    """Fetch raceindex for a single date and return (date_str, (節日数, 開催種別))."""
    url = (
        f"https://www.boatrace.jp/owpc/pc/race/raceindex?"
        f"jcd={JYOJO}&hd={date_str}"
    )
    return (date_str, parse_raceindex(fetch_html(url)))


def read_existing():
    if not os.path.exists(CSV_PATH):
        return {}
    with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    if not rows:
        return {}
    data = {}
    for r in rows[1:]:
        if r and r[0].strip():
            padded = r[: len(HEADER)] + [""] * max(0, len(HEADER) - len(r))
            data[r[0].strip()] = padded
    return data


def write_csv(rows_by_date):
    with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(HEADER)
        for d in sorted(rows_by_date.keys()):
            writer.writerow(rows_by_date[d])


def main():
    print(f"既存CSVを読み込み: {CSV_PATH}", flush=True)
    existing = read_existing()
    print(f"既存日数: {len(existing)}", flush=True)

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

    tasks = [
        (dl.replace("-", ""), rno) for dl in target_dates for rno in (10, 11, 12)
    ]
    print(
        f"フェッチ件数: {len(tasks)} (raceresult+beforeinfo 同時並列 {MAX_WORKERS})",
        flush=True,
    )
    start_t = time.time()

    results = {}
    done_count = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for date_str, rno, race, weather in ex.map(fetch_both, tasks):
            results[(date_str, rno)] = (race, weather)
            done_count += 1
            if done_count % 60 == 0 or done_count == len(tasks):
                elapsed = time.time() - start_t
                rate = done_count / elapsed if elapsed > 0 else 0
                eta = (len(tasks) - done_count) / rate if rate > 0 else 0
                print(
                    f"  [{done_count}/{len(tasks)}] elapsed={elapsed:.0f}s "
                    f"rate={rate:.2f}/s eta={eta:.0f}s",
                    flush=True,
                )

    print(f"全フェッチ完了: {time.time()-start_t:.0f}s", flush=True)

    meta_results = {}
    meta_targets = [dl.replace("-", "") for dl in target_dates]
    print(f"raceindex(節日数+開催種別)取得: {len(meta_targets)}件", flush=True)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for date_str, meta in ex.map(fetch_meta, meta_targets):
            meta_results[date_str] = meta

    added = 0
    skipped_empty = 0
    for dl in target_dates:
        ds = dl.replace("-", "")
        r10, w10 = results.get((ds, 10), (("", "", "", "", ""), ("", "", "")))
        r11, w11 = results.get((ds, 11), (("", "", "", "", ""), ("", "", "")))
        r12, w12 = results.get((ds, 12), (("", "", "", "", ""), ("", "", "")))
        if not (r10[0] or r11[0] or r12[0]):
            skipped_empty += 1
            continue
        day_num, kind = meta_results.get(ds, ("", ""))
        row = [
            dl,
            r10[0], r10[1], r10[2], r10[3],
            r11[0], r11[1], r11[2], r11[3],
            r12[0], r12[1], r12[2], r12[3],
            r10[4], r11[4], r12[4],
            w10[0], w10[1], w10[2],
            w11[0], w11[1], w11[2],
            w12[0], w12[1], w12[2],
            day_num, kind,
        ]
        existing[dl] = row
        added += 1

    print(f"新規追加: {added}日  開催なしスキップ: {skipped_empty}日", flush=True)
    write_csv(existing)
    print(
        f"CSV書き出し完了: 合計{len(existing)}日分 → {CSV_PATH}", flush=True
    )


if __name__ == "__main__":
    main()
