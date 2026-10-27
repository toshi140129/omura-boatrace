"""
omura_tide.py — 潮位（気象庁 推算潮位）を日付・12R締切時刻で引いて omura_tide.csv を作る

boatrace.jp の大村ページには潮位が出ないので、気象庁の「潮位表（推算値）」テキストを使う。
大村湾の中に気象庁の潮位観測点は無いため、既定は最寄りの基準港「長崎」(地点記号 NS) の推算値。
大村湾は湾口（針尾瀬戸）が狭く、長崎より潮差が小さく時刻も遅れるので「長崎の潮位」として扱うこと。

  テキストの形式（1行=1日）: 1〜72桁 毎時潮位(0〜23時, 3桁cm) / 73〜78桁 年月日 / 79〜80桁 地点記号 /
  81〜108桁 満潮 時刻(hhmm)+潮位(4桁)×4 / 109〜136桁 干潮 ×4。欠損は 99/9999。
  URL: https://www.data.jma.go.jp/kaiyou/data/db/tide/suisan/txt/{年}/{地点記号}.txt

使い方
  python omura_tide.py                 # omura_results.csv の全日付分を作り直す（年ファイルは tide_cache/ に保存、当年分は毎回取り直す）
  python omura_tide.py --station NS    # 地点記号を変える
  python omura_tide.py --check         # 取得した年ファイルの先頭行と潮差の目安を表示（地点記号の確認用）

出力 omura_tide.csv（1行=1日）
  日付, 締切時刻, 潮位cm, 潮の向き(上げ/下げ), 直前の満干(満潮/干潮), 直前の満干からの分, 次の満干(満潮/干潮), 次の満干までの分,
  当日潮差cm, 月齢, 潮名(大潮/中潮/小潮/長潮/若潮), 満潮1時刻, 満潮1潮位, 満潮2時刻, 満潮2潮位, 干潮1時刻, 干潮1潮位, 干潮2時刻, 干潮2潮位
締切時刻は omura_race_extra.csv の 12R 締切時刻、無ければ開催種別（ナイター 20:40 / ミッドナイト 22:45 / デイ 16:30）で代用。
"""
import argparse
import csv
import os
import sys
import time
import urllib.request
from datetime import date, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS_CSV = os.path.join(HERE, "omura_results.csv")
RACE_EXTRA_CSV = os.path.join(HERE, "omura_race_extra.csv")
TIDE_CSV = os.path.join(HERE, "omura_tide.csv")
CACHE_DIR = os.path.join(HERE, "tide_cache")
STATION = "NS"   # 長崎
URL = "https://www.data.jma.go.jp/kaiyou/data/db/tide/suisan/txt/{year}/{station}.txt"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
HEADER = ["日付", "締切時刻", "潮位cm", "潮の向き", "直前の満干", "直前の満干からの分", "次の満干", "次の満干までの分",
          "当日潮差cm", "月齢", "潮名",
          "満潮1時刻", "満潮1潮位", "満潮2時刻", "満潮2潮位", "干潮1時刻", "干潮1潮位", "干潮2時刻", "干潮2潮位"]
DEFAULT_DEADLINE = {"ナイター": "20:40", "ミッドナイト": "22:45", "デイ": "16:30"}


def fetch_year(year, station, force=False):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, f"{year}_{station}.txt")
    if os.path.exists(path) and not force:
        return path
    url = URL.format(year=year, station=station)
    for i in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as res:
                data = res.read().decode("utf-8", errors="replace")
            if len(data) < 1000:
                raise ValueError(f"応答が短い({len(data)}B)")
            with open(path, "w", encoding="utf-8") as f:
                f.write(data)
            return path
        except Exception as e:
            print(f"  潮位 取得失敗 {url}: {e}", file=sys.stderr, flush=True)
            time.sleep(2 * (i + 1))
    return path if os.path.exists(path) else ""


def parse_year(path):
    """{日付: {"hourly": [24個], "high": [(分, 潮位)], "low": [...]}}"""
    out = {}
    if not path or not os.path.exists(path):
        return out
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if len(line) < 80:
                continue
            try:
                hourly = [int(line[i * 3:(i + 1) * 3]) for i in range(24)]
                yy, mm, dd = int(line[72:74]), int(line[74:76]), int(line[76:78])
            except ValueError:
                continue
            year = 2000 + yy
            d = f"{year:04d}-{mm:02d}-{dd:02d}"

            def pairs(seg):
                res = []
                for k in range(4):
                    t = seg[k * 8:k * 8 + 4].strip()
                    h = seg[k * 8 + 4:k * 8 + 8].strip()
                    if not t or t.startswith("99") or not t.isdigit():
                        continue
                    try:
                        res.append((int(t[:2]) * 60 + int(t[2:]), int(h)))
                    except ValueError:
                        pass
                return res

            out[d] = {"hourly": hourly, "high": pairs(line[80:108]), "low": pairs(line[108:136])}
    return out


def tide_at(hourly, minutes):
    h = minutes // 60
    m = minutes % 60
    a = hourly[min(h, 23)]
    b = hourly[min(h + 1, 23)]
    return a + (b - a) * m / 60.0


def moon_age(d):
    """月齢（2000-01-06 18:14 UTC の新月を基準にした概算）"""
    epoch = datetime(2000, 1, 6, 18, 14)
    dt = datetime(d.year, d.month, d.day, 12, 0) - epoch
    return (dt.total_seconds() / 86400.0) % 29.530589


def tide_name(age):
    """旧暦日（≒月齢+1）から潮名。大潮/中潮/小潮/長潮/若潮"""
    day = int(age) + 1
    if day in (1, 2, 15, 16, 17, 30):
        return "大潮"
    if day in (3, 4, 5, 6, 18, 19, 20, 21):
        return "中潮"
    if day in (7, 8, 9, 22, 23, 24):
        return "小潮"
    if day in (10, 25):
        return "長潮"
    if day in (11, 26):
        return "若潮"
    return "中潮"


def read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def build(station=STATION, refetch_current=True):
    results = read_csv(RESULTS_CSV)
    if not results:
        print("omura_results.csv がありません", flush=True)
        return
    deadlines = {}
    for r in read_csv(RACE_EXTRA_CSV):
        if r.get("R") == "12" and r.get("締切時刻"):
            deadlines[r["日付"]] = r["締切時刻"]
    years = sorted({r["日付"][:4] for r in results})
    this_year = str(date.today().year)
    tide = {}
    for y in years:
        p = fetch_year(y, station, force=(refetch_current and y == this_year))
        tide.update(parse_year(p))
    rows = []
    for r in results:
        d = r["日付"]
        t = tide.get(d)
        dl = deadlines.get(d) or DEFAULT_DEADLINE.get(r.get("開催種別", ""), "20:40")
        if not t:
            rows.append([d, dl] + [""] * (len(HEADER) - 2))
            continue
        hh, mm = dl.split(":")
        minutes = int(hh) * 60 + int(mm)
        level = tide_at(t["hourly"], minutes)
        before = tide_at(t["hourly"], max(minutes - 30, 0))
        after = tide_at(t["hourly"], min(minutes + 30, 23 * 60 + 59))
        direction = "上げ" if after > before else ("下げ" if after < before else "")
        events = sorted([(m, "満潮") for m, _h in t["high"]] + [(m, "干潮") for m, _h in t["low"]])
        prev = [e for e in events if e[0] <= minutes]
        nxt = [e for e in events if e[0] > minutes]
        prev_name, prev_min = (prev[-1][1], minutes - prev[-1][0]) if prev else ("", "")
        next_name, next_min = (nxt[0][1], nxt[0][0] - minutes) if nxt else ("", "")
        dd = date.fromisoformat(d)
        age = moon_age(dd)
        hi = t["high"][:2] + [("", "")] * (2 - len(t["high"][:2]))
        lo = t["low"][:2] + [("", "")] * (2 - len(t["low"][:2]))

        def hm(m):
            return f"{m // 60:02d}:{m % 60:02d}" if m != "" else ""

        rows.append([d, dl, f"{level:.0f}", direction, prev_name, prev_min, next_name, next_min,
                     max(t["hourly"]) - min(t["hourly"]), f"{age:.1f}", tide_name(age),
                     hm(hi[0][0]), hi[0][1], hm(hi[1][0]), hi[1][1], hm(lo[0][0]), lo[0][1], hm(lo[1][0]), lo[1][1]])
    with open(TIDE_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        w.writerows(rows)
    filled = sum(1 for x in rows if x[2])
    print(f"潮位: {len(rows)}日中 {filled}日に潮位あり（地点 {station}）→ {TIDE_CSV}", flush=True)


def check(station):
    for y in sorted({r["日付"][:4] for r in read_csv(RESULTS_CSV)} or {str(date.today().year)}):
        p = fetch_year(y, station)
        data = parse_year(p)
        if not data:
            print(y, "解析できず", p)
            continue
        d0 = sorted(data)[0]
        ranges = [max(v["hourly"]) - min(v["hourly"]) for v in data.values()]
        print(f"{y}: {len(data)}日 先頭 {d0} 毎時={data[d0]['hourly'][:6]}... 満潮={data[d0]['high']} 干潮={data[d0]['low']} 潮差 最小{min(ranges)} 最大{max(ranges)}cm")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--station", default=STATION)
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()
    if args.check:
        check(args.station)
    else:
        build(args.station)


if __name__ == "__main__":
    main()
