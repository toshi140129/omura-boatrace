"""
既存 omura_results.csv の全行に 12R_安定板使用艇数・12R_潮位 を追記する。
cols[30] と cols[31] が両方空の行のみ取得対象（再実行可能）。
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
MAX_WORKERS = 8

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

HEADER_32 = [
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
    "12R_安定板使用艇数", "12R_潮位",
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


def parse_stabilizer(html):
    if not html or "データがありません" in html:
        return ""
    try:
        soup = BeautifulSoup(html, "html.parser")
        for table in soup.find_all("table"):
            headers = [h.get_text(strip=True) for h in table.select("thead th")]
            if "安定板" not in headers:
                continue
            col_idx = headers.index("安定板")
            count = sum(
                1 for tr in table.select("tbody tr")
                if len(tr.find_all("td")) > col_idx
                and tr.find_all("td")[col_idx].get_text(strip=True) == "○"
            )
            return str(count)
        return ""
    except Exception as e:
        print(f"  stabilizer parse error: {e}", file=sys.stderr, flush=True)
        return ""


def parse_tide(html):
    if not html or "データがありません" in html:
        return ""
    try:
        soup = BeautifulSoup(html, "html.parser")
        # weather1 セクション
        for section_cls in (".weather1", ".weather2"):
            w = soup.select_one(section_cls)
            if not w:
                continue
            unit_cls = section_cls.lstrip(".") + "_bodyUnit"
            title_cls = section_cls.lstrip(".") + "_bodyUnitLabelTitle"
            data_cls = section_cls.lstrip(".") + "_bodyUnitLabelData"
            for u in w.select(f".{unit_cls}"):
                title = u.select_one(f".{title_cls}")
                data = u.select_one(f".{data_cls}")
                if not title or not data:
                    continue
                t = title.get_text(strip=True)
                if t == "潮位":
                    d = unicodedata.normalize("NFKC", data.get_text(strip=True))
                    m = re.search(r"(-?\d+(?:\.\d+)?)", d)
                    if m:
                        return m.group(1)
        return ""
    except Exception as e:
        print(f"  tide parse error: {e}", file=sys.stderr, flush=True)
        return ""


def fetch_r12_before(date_str):
    url = (
        f"https://www.boatrace.jp/owpc/pc/race/beforeinfo?"
        f"rno=12&jcd={JYOJO}&hd={date_str}"
    )
    html = fetch_html(url)
    stab = parse_stabilizer(html)
    tide = parse_tide(html)
    return (stab, tide)


def main():
    if not os.path.exists(CSV_PATH):
        print(f"CSVが見つかりません: {CSV_PATH}", flush=True)
        return

    with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    if not rows:
        print("CSVが空です", flush=True)
        return

    # ヘッダーを32列に拡張（既に32列なら変更なし）
    header = rows[0]
    if len(header) < 32:
        header = HEADER_32

    data_rows = rows[1:]
    # cols[30] と cols[31] が空の行を取得対象にする
    targets = []
    for i, r in enumerate(data_rows):
        padded = r + [""] * max(0, 32 - len(r))
        data_rows[i] = padded
        if not r[0].strip():
            continue
        if not padded[30] and not padded[31]:
            targets.append((i, r[0].strip().replace("-", "")))

    print(f"バックフィル対象: {len(targets)} 行", flush=True)
    if not targets:
        print("対象行なし。終了", flush=True)
        return

    start_t = time.time()
    done = 0

    def task(args):
        idx, ds = args
        stab, tide = fetch_r12_before(ds)
        return (idx, stab, tide)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for idx, stab, tide in ex.map(task, targets):
            data_rows[idx][30] = stab
            data_rows[idx][31] = tide
            done += 1
            if done % 50 == 0 or done == len(targets):
                elapsed = time.time() - start_t
                rate = done / elapsed if elapsed > 0 else 0
                eta = (len(targets) - done) / rate if rate > 0 else 0
                print(
                    f"  [{done}/{len(targets)}] elapsed={elapsed:.0f}s "
                    f"rate={rate:.2f}/s eta={eta:.0f}s",
                    flush=True,
                )

    with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for r in data_rows:
            writer.writerow(r)

    print(f"バックフィル完了: {CSV_PATH}", flush=True)
    print("GitHubへpushする場合: git add omura_results.csv && git commit -m 'backfill tide/stabilizer' && git push", flush=True)


if __name__ == "__main__":
    main()
