"""
omura_extra_fetch.py — 大村ボートレース 追加データ取得（出走表・直前情報・スタート情報・全レース結果・11Rオッズ）

omura_results.csv に入っていない「予測の判定材料」を boatrace.jp から取得し、
日付で結合できる CSV にする。途中で止めても再実行すれば続きから取得する。

生成するファイル（すべてこのスクリプトと同じフォルダ）
  omura_racers.csv       1行=1艇。10R/11R/12R の出走表（級別・勝率・モーター/ボート2連率・F数・平均ST）
                         ＋直前情報（展示タイム・チルト・調整重量・スタート展示の進入/ST）
                         ＋本番のスタート情報（進入/ST）と着順
  omura_races.csv        1行=1レース。1R〜12R の 1〜3着・3連単払戻・決まり手。10〜12R はレース種別（予選/準優勝戦/優勝戦 など）も
  omura_meta.csv         1行=1日。節名・グレード（SG/G1/G2/G3/一般）
  omura_11r_odds.csv     11R の3連単確定オッズ120通り（omura_12r_odds.csv と同じ列構成）
  omura_12r_odds.csv     既存ファイル。未取得の日付があれば追記する
  omura_daily_extra.csv  上記から作る 1行=1日 の要約（omura_results.csv と「日付」で結合して分析に使う）
  omura_racers_extra.csv 1行=1艇。10R/11R/12R の支部・出身地・プロペラ・部品交換（直前情報）と、そのレースの安定板使用（1/0）

使い方
  python omura_extra_fetch.py                  # omura_results.csv の全日付のうち未取得分を取得し、要約CSVを再生成
  python omura_extra_fetch.py --test 2025-10-17  # 1日分だけ取得して解析結果を表示（HTMLは extra_debug/ に保存）
  python omura_extra_fetch.py --build          # 取得はせず要約CSVだけ再生成
  python omura_extra_fetch.py --dates 2026-10-18 2026-10-19   # 指定日のみ取得

サイト負荷対策: 並列 WORKERS、1リクエストごとに SLEEP 秒待つ。
"""
import argparse
import csv
import json
import os
import re
import sys
import threading
import time
import unicodedata
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from itertools import permutations

from bs4 import BeautifulSoup

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS_CSV = os.path.join(HERE, "omura_results.csv")
RACERS_CSV = os.path.join(HERE, "omura_racers.csv")
RACES_CSV = os.path.join(HERE, "omura_races.csv")
META_CSV = os.path.join(HERE, "omura_meta.csv")
ODDS11_CSV = os.path.join(HERE, "omura_11r_odds.csv")
ODDS12_CSV = os.path.join(HERE, "omura_12r_odds.csv")
DAILY_EXTRA_CSV = os.path.join(HERE, "omura_daily_extra.csv")
RACERS_EXTRA_CSV = os.path.join(HERE, "omura_racers_extra.csv")
DEBUG_DIR = os.path.join(HERE, "extra_debug")

JYOJO = "24"
WORKERS = 8          # 同時に取得する日数
INNER_WORKERS = 4    # 1日分の中で同時に取るページ数（最大同時接続 = WORKERS × INNER_WORKERS）
SLEEP = 0.2
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
BASE = "https://www.boatrace.jp/owpc/pc/race/"
MAIN_RACES = (10, 11, 12)

RACERS_HEADER = [
    "日付", "R", "枠", "登録番号", "選手名", "級別", "年齢", "体重",
    "F数", "L数", "平均ST",
    "全国勝率", "全国2連率", "全国3連率", "当地勝率", "当地2連率", "当地3連率",
    "モーター番号", "モーター2連率", "モーター3連率", "ボート番号", "ボート2連率", "ボート3連率",
    "展示タイム", "チルト", "調整重量", "展示進入", "展示ST",
    "本番進入", "本番ST", "着順",
]
RACES_HEADER = ["日付", "R", "レース種別", "1着", "2着", "3着", "3連単払戻", "決まり手"]
RACERS_EXTRA_HEADER = ["日付", "R", "枠", "登録番号", "選手名", "支部", "出身地", "プロペラ", "部品交換", "安定板"]
META_HEADER = ["日付", "節名", "グレード"]
COMBOS = [f"{a}-{b}-{c}" for a, b, c in permutations(range(1, 7), 3)]
ODDS_HEADER = ["日付"] + COMBOS

_write_lock = threading.Lock()


# ---------------------------------------------------------------- HTTP
def fetch_html(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as res:
                html = res.read().decode("utf-8", errors="replace")
            time.sleep(SLEEP)
            return html
        except Exception as e:
            if attempt == retries - 1:
                print(f"  FETCH FAIL {url}: {e}", file=sys.stderr, flush=True)
                return ""
            time.sleep(2 * (attempt + 1))
    return ""


def url_for(page, rno, hd):
    if rno is None:
        return f"{BASE}{page}?jcd={JYOJO}&hd={hd}"
    return f"{BASE}{page}?rno={rno}&jcd={JYOJO}&hd={hd}"


# ---------------------------------------------------------------- helpers
def nfkc(s):
    return unicodedata.normalize("NFKC", s or "").strip()


def lines(node):
    """セル内の <br> 区切りテキストをリストで返す"""
    if node is None:
        return []
    txt = node.get_text("\n", strip=True)
    return [nfkc(t) for t in txt.split("\n") if nfkc(t)]


def first_num(s, pattern=r"-?\d+(?:\.\d+)?"):
    m = re.search(pattern, nfkc(s))
    return m.group(0) if m else ""


def no_data(html):
    return (not html) or ("データがありません" in html)


# ---------------------------------------------------------------- 出走表 racelist
RACE_TYPE_RE = re.compile(
    r"(優勝戦|準優勝戦|準優進出戦|順位決定戦|特別選抜[A-Z]?|予選特選|選抜|特選|予選|一般戦|一般|"
    r"ドリーム戦|ドリーム|トライアル\d*|敗者戦|選抜戦|シード戦)"
)
# 見出し <div class="heading2_title is-SGa "> / is-G1a / is-G1b / is-G3a / is-ippan のように末尾に a/b が付く
GRADE_RE = re.compile(r'heading2_title\s+is-(SG|PG1|G1|G2|G3|ippan|ladies|rookie|venus|takumi|master|shinnyu)[a-z]?\b')
GRADE_JP = {
    "SG": "SG", "PG1": "G1", "G1": "G1", "G2": "G2", "G3": "G3", "ippan": "一般",
    "ladies": "一般", "rookie": "一般", "venus": "一般", "takumi": "一般", "master": "一般", "shinnyu": "一般",
}


def parse_branch(cell):
    """出走表の選手セルから (支部, 出身地) を返す。「広島/広島」のように <br> 区切りの1行で出る"""
    for t in lines(cell):
        if "歳" in t or "kg" in t or re.search(r"\d{4}", t):
            continue
        m = re.fullmatch(r"([^\s/]+)\s*/\s*([^\s/]+)", t)
        if m:
            return (m.group(1), m.group(2))
    # 保険: セル全体のテキストから「支部/出身地 年齢歳」の並びを探す
    ct = nfkc(cell.get_text(" ", strip=True))
    m = re.search(r"([^\s/\d]+)\s*/\s*([^\s/\d]+)\s*\d+歳", ct)
    return (m.group(1), m.group(2)) if m else ("", "")


def parse_stabilizer_flag(soup):
    """ページ見出し付近の「安定板使用」ラベルの有無。戻り値: ("1"/"0", 根拠テキスト)"""
    for node in soup.find_all(string=re.compile("安定板")):
        parent = node.parent
        # 凡例や表の中（部品交換凡例など）は対象外。見出し・ラベル要素だけを見る
        if parent.find_parent("table") is not None:
            continue
        txt = nfkc(str(node))
        if "安定板" in txt:
            cls = " ".join(parent.get("class", []))
            return ("1", f"{parent.name}.{cls}:{txt}"[:60])
    return ("0", "")


def parse_racelist(html):
    """出走表。戻り値: (レース種別, 節名, グレード, {枠: {...}})"""
    if no_data(html):
        return ("", "", "", {})
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    text = nfkc(text)

    race_type = ""
    # <h3 class="title16_titleDetail__add2020">準優勝戦　　　1800m</h3> の形
    h3 = soup.select_one("h3.title16_titleDetail__add2020")
    if h3:
        ht = nfkc(h3.get_text(" ", strip=True))
        ht = re.sub(r"\d{4}m.*$", "", ht).strip()
        t = RACE_TYPE_RE.search(ht)
        race_type = t.group(1) if t else ht
    if not race_type:
        m = re.search(r"(\S{1,12})\s+\d{4}m", text)
        if m:
            t = RACE_TYPE_RE.search(m.group(1))
            race_type = t.group(1) if t else m.group(1)

    title = ""
    h = soup.select_one(".heading2_titleName") or soup.select_one("h2")
    if h:
        title = nfkc(h.get_text(" ", strip=True))

    grade = ""
    g = GRADE_RE.search(html)
    if g:
        grade = GRADE_JP.get(g.group(1), g.group(1))
    if not grade and title:
        for key, val in (("SG", "SG"), ("GⅠ", "G1"), ("G1", "G1"), ("GⅡ", "G2"), ("G2", "G2"), ("GⅢ", "G3"), ("G3", "G3")):
            if key in title:
                grade = val
                break
        if not grade:
            grade = "一般"

    boats = {}
    for tb in soup.select("tbody.is-fs12"):
        trs = tb.find_all("tr")
        if not trs:
            continue
        tds = trs[0].find_all("td")
        if len(tds) < 8:
            continue
        waku = first_num(tds[0].get_text(), r"\d")
        if not waku:
            continue
        # 1行目の td: 0=枠, 1=写真, 2=登録番号/級別/選手名/支部/年齢体重, 3=F数/L数/平均ST,
        #            4=全国勝率, 5=当地勝率, 6=モーター, 7=ボート, 8以降=今節成績
        info = {}
        cell = tds[2]
        ct = nfkc(cell.get_text(" ", strip=True))
        m = re.search(r"(\d{4})\s*/\s*([AB][12])", ct)
        if m:
            info["登録番号"], info["級別"] = m.group(1), m.group(2)
        else:
            info["登録番号"] = first_num(ct, r"\d{4}")
            m2 = re.search(r"\b([AB][12])\b", ct)
            info["級別"] = m2.group(1) if m2 else ""
        a = cell.find("a")
        info["選手名"] = nfkc(a.get_text(" ", strip=True)).replace(" ", "") if a else ""
        m = re.search(r"(\d+)歳", ct)
        info["年齢"] = m.group(1) if m else ""
        m = re.search(r"(\d+(?:\.\d+)?)kg", ct)
        info["体重"] = m.group(1) if m else ""
        info["支部"], info["出身地"] = parse_branch(cell)

        fl = lines(tds[3])
        info["F数"] = info["L数"] = info["平均ST"] = ""
        for t in fl:
            if t.startswith("F"):
                info["F数"] = first_num(t, r"\d+")
            elif t.startswith("L"):
                info["L数"] = first_num(t, r"\d+")
            elif re.fullmatch(r"\d\.\d{2}", t):
                info["平均ST"] = t

        def three(td, keys):
            vals = [first_num(t) for t in lines(td)]
            vals = [v for v in vals if v != ""]
            for i, k in enumerate(keys):
                info[k] = vals[i] if i < len(vals) else ""

        three(tds[4], ("全国勝率", "全国2連率", "全国3連率"))
        three(tds[5], ("当地勝率", "当地2連率", "当地3連率"))
        three(tds[6], ("モーター番号", "モーター2連率", "モーター3連率"))
        three(tds[7], ("ボート番号", "ボート2連率", "ボート3連率"))
        boats[waku] = info
    return (race_type, title, grade, boats)


# ---------------------------------------------------------------- 直前情報 beforeinfo
def parse_start_table(soup, keyword):
    """『スタート展示』『スタート情報』の表から [(コース, 艇番, ST文字列, 補足)] をコース順に返す"""
    out = []
    for table in soup.find_all("table"):
        head = table.find("thead")
        if not head or keyword not in head.get_text():
            continue
        for i, div in enumerate(table.select(".table1_boatImage1"), start=1):
            num = div.select_one(".table1_boatImage1Number")
            boat = ""
            if num is not None:
                for cls in num.get("class", []):
                    m = re.match(r"is-type1_number(\d)", cls)
                    if m:
                        boat = m.group(1)
                if not boat:
                    boat = first_num(num.get_text(), r"\d")
            tnode = div.select_one(".table1_boatImage1Time") or div
            ttxt = nfkc(tnode.get_text(" ", strip=True))
            m = re.search(r"([FL]?\.\d{2}|[FL])", ttxt)
            st = m.group(1) if m else ""
            extra = ttxt.replace(st, "").strip() if st else ttxt
            out.append((str(i), boat, st, extra))
        if out:
            break
    return out


def parse_beforeinfo(html):
    """直前情報。戻り値: ({枠: {体重, 調整重量, 展示タイム, チルト, プロペラ, 部品交換, 安定板}}, [(コース, 艇, 展示ST, 補足)])"""
    if no_data(html):
        return ({}, [])
    soup = BeautifulSoup(html, "html.parser")
    stab, _why = parse_stabilizer_flag(soup)
    boats = {}
    for tb in soup.select("tbody.is-fs12"):
        trs = tb.find_all("tr")
        if not trs:
            continue
        tds = trs[0].find_all("td")
        if len(tds) < 6:
            continue
        waku = first_num(tds[0].get_text(), r"\d")
        if not waku:
            continue
        info = {"体重": "", "調整重量": "", "展示タイム": "", "チルト": "", "プロペラ": "", "部品交換": "", "安定板": stab}
        # 1行目の td: 0=枠, 1=写真, 2=選手名, 3=体重, 4=展示タイム, 5=チルト, 6=プロペラ, 7=部品交換, 8以降=前走成績
        info["体重"] = first_num(tds[3].get_text(), r"\d+(?:\.\d+)?")
        info["展示タイム"] = first_num(tds[4].get_text(), r"\d\.\d{2}")
        info["チルト"] = first_num(tds[5].get_text(), r"-?\d+(?:\.\d+)?")
        if len(tds) > 6:
            info["プロペラ"] = " ".join(lines(tds[6]))
        if len(tds) > 7:
            info["部品交換"] = "・".join(lines(tds[7]))
        # 3行目の先頭 td が調整重量（"0.0" "1.5" など小数）。2行目の「進入」の整数は除外する
        for tr in trs[1:]:
            tds2 = tr.find_all("td")
            if not tds2:
                continue
            t = nfkc(tds2[0].get_text(" ", strip=True))
            if re.fullmatch(r"\d+\.\d+", t):
                info["調整重量"] = t
                break
        boats[waku] = info
    starts = parse_start_table(soup, "スタート展示")
    return (boats, starts)


# ---------------------------------------------------------------- 結果 raceresult
KIMARITE_RE = re.compile(r"(まくり差し|まくり|差し|逃げ|抜き|恵まれ)")


def parse_raceresult(html):
    """結果。戻り値: ({着順: 艇番}, 払戻, 人気, 決まり手, [(コース, 艇, ST, 補足)])"""
    if no_data(html):
        return ({}, "", "", "", [])
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.select("table.is-w495")
    places = {}
    pay = rank = ""
    if tables:
        for tr in tables[0].select("tbody tr"):
            tds = tr.find_all("td")
            if len(tds) >= 2:
                rnk = nfkc(tds[0].get_text(strip=True))
                boat = first_num(tds[1].get_text(), r"\d")
                if rnk in ("1", "2", "3", "4", "5", "6") and boat:
                    places[rnk] = boat
    for table in tables:
        for tr in table.select("tbody tr"):
            tds = tr.find_all("td")
            if len(tds) >= 3 and "3連単" in tds[0].get_text():
                pay = nfkc(tds[2].get_text(strip=True)).replace("¥", "").replace(",", "").replace("円", "")
                if len(tds) >= 4:
                    rank = nfkc(tds[3].get_text(strip=True))
                break
        if pay:
            break
    kimarite = ""
    for table in soup.find_all("table"):
        head = table.find("thead")
        if head and "決まり手" in head.get_text():
            m = KIMARITE_RE.search(nfkc(table.get_text(" ", strip=True)))
            if m:
                kimarite = m.group(1)
                break
    starts = parse_start_table(soup, "スタート情報")
    if not kimarite:
        for _c, _b, _st, extra in starts:
            m = KIMARITE_RE.search(extra)
            if m:
                kimarite = m.group(1)
                break
    return (places, pay, rank, kimarite, starts)


# ---------------------------------------------------------------- オッズ odds3t
def _odds_order():
    res = []
    for row in range(20):
        for first in range(1, 7):
            seconds = [s for s in range(1, 7) if s != first]
            second = seconds[row // 4]
            thirds = [t for t in range(1, 7) if t not in (first, second)]
            res.append(f"{first}-{second}-{thirds[row % 4]}")
    return res


ODDS_ORDER = _odds_order()


def parse_odds3t(html):
    """3連単オッズ。{組番: オッズ文字列} を返す。取れなければ {}"""
    if no_data(html):
        return {}
    vals = re.findall(r'class="oddsPoint[^"]*">([^<]*)<', html)
    if len(vals) != 120:
        return {}
    return dict(zip(ODDS_ORDER, (v.strip() for v in vals)))


# ---------------------------------------------------------------- 1日分の取得
def fetch_day(date_str, save_html=False):
    """1日分をまとめて取得。戻り値: dict（開催なしなら None）"""
    hd = date_str.replace("-", "")
    htmls = {}

    def get(page, rno):
        html = fetch_html(url_for(page, rno, hd))
        if save_html:
            os.makedirs(DEBUG_DIR, exist_ok=True)
            with open(os.path.join(DEBUG_DIR, f"{hd}_{page}_{rno}.html"), "w", encoding="utf-8") as f:
                f.write(html)
        return html

    # まず 12R 結果で開催有無を確認
    r12 = get("raceresult", 12)
    if no_data(r12):
        return None
    htmls[("raceresult", 12)] = r12
    # 残り19ページ。サーバー応答が1件あたり約9秒かかるため、1日分の中でも並列に取る
    keys = [("raceresult", rno) for rno in range(1, 12)]
    for rno in MAIN_RACES:
        keys += [("racelist", rno), ("beforeinfo", rno)]
    keys += [("odds3t", 11), ("odds3t", 12)]
    with ThreadPoolExecutor(max_workers=INNER_WORKERS) as ex:
        for key, html in zip(keys, ex.map(lambda k: get(*k), keys)):
            htmls[key] = html

    day = {"日付": date_str, "races": {}, "racers": {}, "meta": {}, "odds11": {}, "odds12": {}}
    for rno in range(1, 13):
        places, pay, rank, kimarite, starts = parse_raceresult(htmls[("raceresult", rno)])
        day["races"][rno] = {
            "places": places, "pay": pay, "rank": rank, "kimarite": kimarite, "starts": starts,
            "race_type": "",
        }
    for rno in MAIN_RACES:
        race_type, title, grade, boats = parse_racelist(htmls[("racelist", rno)])
        day["races"][rno]["race_type"] = race_type
        if title and not day["meta"].get("節名"):
            day["meta"]["節名"] = title
            day["meta"]["グレード"] = grade
        binfo, ex_starts = parse_beforeinfo(htmls[("beforeinfo", rno)])
        places = day["races"][rno]["places"]
        boat_place = {b: p for p, b in places.items()}
        ex_by_boat = {b: (c, st) for c, b, st, _e in ex_starts}
        real_by_boat = {b: (c, st) for c, b, st, _e in day["races"][rno]["starts"]}
        rows = {}
        for waku in "123456":
            info = dict(boats.get(waku, {}))
            info.update(binfo.get(waku, {}))
            info["展示進入"], info["展示ST"] = ex_by_boat.get(waku, ("", ""))
            info["本番進入"], info["本番ST"] = real_by_boat.get(waku, ("", ""))
            info["着順"] = boat_place.get(waku, "")
            rows[waku] = info
        day["racers"][rno] = rows
    day["odds11"] = parse_odds3t(htmls[("odds3t", 11)])
    day["odds12"] = parse_odds3t(htmls[("odds3t", 12)])
    return day


# ---------------------------------------------------------------- CSV 入出力
def read_dates(path):
    if not os.path.exists(path):
        return set()
    with open(path, encoding="utf-8", newline="") as f:
        return {r[0] for r in csv.reader(f) if r and r[0] != "日付"}


def append_rows(path, header, rows):
    new = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(header)
        for r in rows:
            w.writerow(r)


def write_day(day, odds12_done):
    d = day["日付"]
    racer_rows, extra_rows = [], []
    for rno in MAIN_RACES:
        for waku in "123456":
            info = day["racers"].get(rno, {}).get(waku, {})
            racer_rows.append([d, rno, waku] + [info.get(k, "") for k in RACERS_HEADER[3:]])
            extra_rows.append([d, rno, waku] + [info.get(k, "") for k in RACERS_EXTRA_HEADER[3:]])
    race_rows = []
    for rno in range(1, 13):
        r = day["races"][rno]
        p = r["places"]
        race_rows.append([d, rno, r["race_type"], p.get("1", ""), p.get("2", ""), p.get("3", ""), r["pay"], r["kimarite"]])
    meta_row = [d, day["meta"].get("節名", ""), day["meta"].get("グレード", "")]
    with _write_lock:
        append_rows(RACERS_CSV, RACERS_HEADER, racer_rows)
        append_rows(RACERS_EXTRA_CSV, RACERS_EXTRA_HEADER, extra_rows)
        append_rows(RACES_CSV, RACES_HEADER, race_rows)
        append_rows(META_CSV, META_HEADER, [meta_row])
        if day["odds11"]:
            append_rows(ODDS11_CSV, ODDS_HEADER, [[d] + [day["odds11"].get(c, "") for c in COMBOS]])
        if day["odds12"] and d not in odds12_done:
            append_rows(ODDS12_CSV, ODDS_HEADER, [[d] + [day["odds12"].get(c, "") for c in COMBOS]])
            odds12_done.add(d)


def sort_csv(path, header, key_cols):
    """日付順に並べ直す（重複行は後勝ち）"""
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8", newline="") as f:
        rows = [r for r in csv.reader(f) if r]
    body = {}
    for r in rows[1:]:
        if not r or r[0] == "日付":
            continue
        body[tuple(r[i] for i in key_cols)] = r
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for k in sorted(body.keys(), key=lambda t: tuple((x.zfill(2) if x.isdigit() else x) for x in t)):
            w.writerow(body[k])


def sort_all():
    sort_csv(RACERS_CSV, RACERS_HEADER, (0, 1, 2))
    sort_csv(RACERS_EXTRA_CSV, RACERS_EXTRA_HEADER, (0, 1, 2))
    sort_csv(RACES_CSV, RACES_HEADER, (0, 1))
    sort_csv(META_CSV, META_HEADER, (0,))
    sort_csv(ODDS11_CSV, ODDS_HEADER, (0,))
    sort_csv(ODDS12_CSV, ODDS_HEADER, (0,))


# ---------------------------------------------------------------- 取得の実行
def fetch_dates(dates, label="取得"):
    dates = [d for d in dates if d]
    done = read_dates(RACES_CSV)
    todo = sorted(d for d in dates if d not in done)
    print(f"{label}: 対象{len(dates)}日 取得済み{len(dates) - len(todo)}日 → 今回{len(todo)}日", flush=True)
    if not todo:
        return []
    odds12_done = read_dates(ODDS12_CSV)
    added = []
    start_t = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for i, (d, day) in enumerate(zip(todo, ex.map(fetch_day, todo)), start=1):
            if day is None:
                print(f"  {d}: 開催なし/取得不可", flush=True)
            else:
                write_day(day, odds12_done)
                added.append(d)
            if i % 10 == 0 or i == len(todo):
                el = time.time() - start_t
                print(f"  [{i}/{len(todo)}] {el:.0f}s 経過  残り目安 {el / i * (len(todo) - i):.0f}s", flush=True)
    sort_all()
    print(f"{label}完了: {len(added)}日追加 ({time.time() - start_t:.0f}s)", flush=True)
    return added


def results_dates():
    return sorted(read_dates(RESULTS_CSV))


# ---------------------------------------------------------------- 要約CSV（1行=1日）
def fnum(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def fmt(v, nd=2):
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:.{nd}f}".rstrip("0").rstrip(".") if nd else str(int(round(v)))
    return str(v)


def load_long(path, key_fn):
    out = {}
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            out.setdefault(key_fn(r), []).append(r)
    return out


def odds_summary(row, prefix):
    """オッズ1行から 1番人気・1号艇頭の合成オッズ などを作る"""
    out = {}
    if not row:
        return out
    vals = {c: fnum(row.get(c)) for c in COMBOS}
    vals = {c: v for c, v in vals.items() if v}
    if not vals:
        return out
    ranked = sorted(vals.items(), key=lambda kv: kv[1])
    out[f"{prefix}_1番人気"] = ranked[0][0]
    out[f"{prefix}_1番人気オッズ"] = fmt(ranked[0][1], 1)
    out[f"{prefix}_2番人気"] = ranked[1][0]
    out[f"{prefix}_2番人気オッズ"] = fmt(ranked[1][1], 1)
    head1 = [v for c, v in vals.items() if c.startswith("1-")]
    if head1:
        out[f"{prefix}_1頭最低オッズ"] = fmt(min(head1), 1)
        out[f"{prefix}_1頭合成オッズ"] = fmt(1 / sum(1 / v for v in head1), 2)  # 1-全-全20点の合成（低いほどイン本命）
        out[f"{prefix}_1頭支持率"] = fmt(100 * sum(1 / v for v in head1) / sum(1 / v for v in vals.values()), 1)
    top5 = [c[0] for c, _v in ranked[:5]]
    out[f"{prefix}_上位5点1頭数"] = str(top5.count("1"))
    return out


def build_daily_extra():
    racers = load_long(RACERS_CSV, lambda r: r["日付"])
    races = load_long(RACES_CSV, lambda r: r["日付"])
    meta = {r["日付"]: r for rs in load_long(META_CSV, lambda r: r["日付"]).values() for r in rs}
    odds11 = {r["日付"]: r for rs in load_long(ODDS11_CSV, lambda r: r["日付"]).values() for r in rs}
    odds12 = {r["日付"]: r for rs in load_long(ODDS12_CSV, lambda r: r["日付"]).values() for r in rs}
    dates = sorted(set(races) | set(racers))

    header = ["日付", "節名", "グレード"]
    for rno in MAIN_RACES:
        header += [f"{rno}R_種別", f"{rno}R_決まり手"]
    header += ["1-9R_1号艇1着数", "1-9R_レース数", "1-9R_イン1着率", "1-9R_逃げ数", "1-9R_1着艇", "1-9R_平均払戻"]
    per_race = [
        "1号艇級別", "1号艇全国勝率", "1号艇当地勝率", "1号艇全国2連率", "1号艇モーター2連率", "1号艇ボート2連率",
        "1号艇平均ST", "1号艇F数", "1号艇年齢", "1号艇体重", "1号艇チルト",
        "1号艇展示タイム", "1号艇展示タイム順位", "1号艇展示ST", "1号艇展示進入", "1号艇本番ST", "1号艇本番進入",
        "勝率最高枠", "1号艇勝率差", "A1人数", "A級人数", "展示枠なり", "本番枠なり",
        "展示最速枠", "本番ST最速枠", "2号艇全国勝率", "3号艇全国勝率", "4号艇全国勝率",
    ]
    for rno in MAIN_RACES:
        header += [f"{rno}R_{k}" for k in per_race]
    odds_keys = ["1番人気", "1番人気オッズ", "2番人気", "2番人気オッズ", "1頭最低オッズ", "1頭合成オッズ", "1頭支持率", "上位5点1頭数"]
    header += [f"11R_{k}" for k in odds_keys] + [f"12R_{k}" for k in odds_keys]

    rows = []
    for d in dates:
        row = {"日付": d, "節名": meta.get(d, {}).get("節名", ""), "グレード": meta.get(d, {}).get("グレード", "")}
        rs = {int(r["R"]): r for r in races.get(d, [])}
        for rno in MAIN_RACES:
            row[f"{rno}R_種別"] = rs.get(rno, {}).get("レース種別", "")
            row[f"{rno}R_決まり手"] = rs.get(rno, {}).get("決まり手", "")
        early = [rs[r] for r in range(1, 10) if r in rs and rs[r].get("1着")]
        n = len(early)
        in1 = sum(1 for r in early if r["1着"] == "1")
        row["1-9R_1号艇1着数"] = str(in1) if n else ""
        row["1-9R_レース数"] = str(n) if n else ""
        row["1-9R_イン1着率"] = fmt(100 * in1 / n, 1) if n else ""
        row["1-9R_逃げ数"] = str(sum(1 for r in early if r.get("決まり手") == "逃げ")) if n else ""
        row["1-9R_1着艇"] = "".join(r["1着"] for r in early)
        pays = [fnum(r.get("3連単払戻")) for r in early]
        pays = [p for p in pays if p]
        row["1-9R_平均払戻"] = fmt(sum(pays) / len(pays), 0) if pays else ""

        by_race = {}
        for r in racers.get(d, []):
            by_race.setdefault(int(r["R"]), {})[r["枠"]] = r
        for rno in MAIN_RACES:
            b = by_race.get(rno, {})
            p = f"{rno}R_"
            b1 = b.get("1", {})
            row[p + "1号艇級別"] = b1.get("級別", "")
            for k in ("全国勝率", "当地勝率", "全国2連率", "モーター2連率", "ボート2連率", "平均ST", "F数", "年齢", "体重", "チルト",
                      "展示タイム", "展示ST", "展示進入", "本番ST", "本番進入"):
                row[p + "1号艇" + k] = b1.get(k, "")
            for w in ("2", "3", "4"):
                row[p + f"{w}号艇全国勝率"] = b.get(w, {}).get("全国勝率", "")
            ex = {w: fnum(x.get("展示タイム")) for w, x in b.items()}
            ex = {w: v for w, v in ex.items() if v}
            if ex and "1" in ex:
                row[p + "1号艇展示タイム順位"] = str(1 + sum(1 for w, v in ex.items() if v < ex["1"]))
                row[p + "展示最速枠"] = min(ex, key=ex.get)
            else:
                row[p + "1号艇展示タイム順位"] = row[p + "展示最速枠"] = ""
            wr = {w: fnum(x.get("全国勝率")) for w, x in b.items()}
            wr = {w: v for w, v in wr.items() if v is not None}
            if wr and "1" in wr:
                row[p + "勝率最高枠"] = max(wr, key=wr.get)
                others = [v for w, v in wr.items() if w != "1"]
                row[p + "1号艇勝率差"] = fmt(wr["1"] - max(others), 2) if others else ""
            else:
                row[p + "勝率最高枠"] = row[p + "1号艇勝率差"] = ""
            cls = [x.get("級別", "") for x in b.values()]
            row[p + "A1人数"] = str(cls.count("A1")) if b else ""
            row[p + "A級人数"] = str(cls.count("A1") + cls.count("A2")) if b else ""
            exin = [b.get(w, {}).get("展示進入", "") for w in "123456"]
            row[p + "展示枠なり"] = ("1" if exin == list("123456") else "0") if all(exin) else ""
            rin = [b.get(w, {}).get("本番進入", "") for w in "123456"]
            row[p + "本番枠なり"] = ("1" if rin == list("123456") else "0") if all(rin) else ""

            def st_val(s):
                if not s or s.startswith("F") or s.startswith("L"):
                    return None
                return fnum(s)

            rst = {w: st_val(x.get("本番ST")) for w, x in b.items()}
            rst = {w: v for w, v in rst.items() if v is not None}
            row[p + "本番ST最速枠"] = min(rst, key=rst.get) if rst else ""
        row.update(odds_summary(odds11.get(d), "11R"))
        row.update(odds_summary(odds12.get(d), "12R"))
        rows.append([row.get(h, "") for h in header])

    with open(DAILY_EXTRA_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"要約CSV書き出し: {len(rows)}日 → {DAILY_EXTRA_CSV}", flush=True)


# ---------------------------------------------------------------- main
def run_test(date_str):
    day = fetch_day(date_str, save_html=True)
    if day is None:
        print("開催なし or 取得失敗（extra_debug/ のHTMLを確認）")
        return
    print(json.dumps(day["meta"], ensure_ascii=False))
    for rno in range(1, 13):
        r = day["races"][rno]
        print(f"{rno:>2}R 種別={r['race_type']} 着順={r['places']} 払戻={r['pay']} 人気={r['rank']} 決まり手={r['kimarite']} ST={r['starts']}")
    for rno in MAIN_RACES:
        print(f"--- {rno}R 出走表+直前")
        for waku in "123456":
            print(" ", waku, json.dumps(day["racers"][rno][waku], ensure_ascii=False))
    print("11Rオッズ件数:", len(day["odds11"]), " 12Rオッズ件数:", len(day["odds12"]))
    if day["odds11"]:
        print("  11R 1-2-3:", day["odds11"].get("1-2-3"), " 6-5-4:", day["odds11"].get("6-5-4"))
    # 解析漏れチェック
    missing = []
    for rno in MAIN_RACES:
        for waku in "123456":
            info = day["racers"][rno][waku]
            for k in ("級別", "全国勝率", "モーター2連率", "展示タイム", "展示ST", "本番ST", "着順", "支部"):
                if not info.get(k):
                    missing.append(f"{rno}R-{waku}:{k}")
    print("空欄:", ", ".join(missing[:30]) if missing else "なし")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", metavar="YYYY-MM-DD")
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--dates", nargs="*")
    args = ap.parse_args()
    if args.test:
        run_test(args.test)
        return
    if args.build:
        build_daily_extra()
        return
    dates = args.dates if args.dates else results_dates()
    fetch_dates(dates)
    build_daily_extra()


if __name__ == "__main__":
    main()
