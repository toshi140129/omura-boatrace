"""
omura_line_notify.py — 大村12R「熱い日」LINE通知

流れ
  1. 当日の大村の締切予定時刻（11R・12R）を boatrace.jp から取得
  2. 11Rの結果が確定するまで待つ（20秒おきに確認。待つ間に10R結果・12R出走表は先に取っておく）
  3. 11Rの1着が1号艇なら、A=11Rの2着・B=11Rの3着 を使って
     12Rの買い目 1-A-全 / 1-B-全 / 1-全-A / 1-全-B のうち40倍未満を抽出
  4. 見送り条件（12Rが雨 / 1号艇の全国勝率が他艇最高より1.0以上低い /
     1号艇の展示タイムが4位以下）と②（10Rと11Rの3着順が完全一致）を判定
  5. 1万円を 1/オッズ比で配分した買い目表をLINEにプッシュ送信（1日1通だけ。情報が揃った時点ですぐ送る）
     ※12Rの展示タイムは締切10分前ごろにしか出ないので待たない。出ていれば判定に入れる

GitHub Actions（.github/workflows/line_notify.yml）から15分おきに起動される前提だが、
環境変数を渡せばPCのタスクスケジューラからも同じように動く。

環境変数
  LINE_CHANNEL_ACCESS_TOKEN  LINE Messaging API のチャネルアクセストークン（長期）
  LINE_USER_ID               送信先のユーザーID（Uから始まる33文字）
  NOTIFY_DATE                対象日 YYYYMMDD（省略時は日本時間の今日）
  NOTIFY_FORCE               1 なら時間帯チェックを無視して即実行（テスト用）
  NOTIFY_DRY_RUN             1 ならLINEに送らず本文を表示するだけ
  NOTIFY_QUIET_DAYS          1 なら「11R 1着≠1号艇」の日も一言だけ送る（既定は送らない）
  NOTIFY_BUDGET              配分する総額（既定 10000）
  GITHUB_OUTPUT              Actions が渡す。done=true を書き出して当日の処理完了を記録する

PCで動かす場合は、このファイルと同じフォルダの .env、無ければ隣の kojima-boatrace\\.env
（児島版のLINE設定）を読む。
"""
import json
import os
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from bs4 import BeautifulSoup

HERE = os.path.dirname(os.path.abspath(__file__))
JST = timezone(timedelta(hours=9))
JYOJO = "24"  # 大村
BASE = "https://www.boatrace.jp/owpc/pc/race/"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

ODDS_MAX = 40.0            # これ未満のオッズだけ買う
WAIT_BEFORE_11R_MIN = 40   # 11R締切の何分前から待機を始めるか
POLL_SEC = 20              # 11R結果の確認間隔（秒）
# 12Rの展示タイムは12Rの展示航走後（締切10分前ごろ）に出るため、待つと通知が遅れる。
# 「情報が揃った時点ですぐ1通」送る方針なので、11R確定時点で出ていなければ待たずに送る。
EXHIBITION_WAIT_MIN = 0    # 展示タイムを待つ上限（分）。0なら待たない
STATE_PATH = os.path.join(HERE, "line_notify_state.json")  # PC実行時の送信済み記録


def log(msg):
    print(f"[{datetime.now(JST).strftime('%H:%M:%S')}] {msg}", flush=True)


# ---------------------------------------------------------------- 設定
ENV_CANDIDATES = [
    os.path.join(HERE, ".env"),
    # PCで動かす場合は児島版（kojima-boatrace）の .env をそのまま流用できる
    os.path.join(os.path.dirname(HERE), "kojima-boatrace", ".env"),
]


def load_env():
    env = {}
    for path in ENV_CANDIDATES:
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8-sig") as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith("#") and "=" in s:
                    k, v = s.split("=", 1)
                    env[k.strip()] = v.strip().strip('"').strip("'")
        break
    env.update({k: v for k, v in os.environ.items() if k.startswith(("LINE_", "NOTIFY_"))})
    return env


def ascii_only(s):
    return (s or "").strip().encode("ascii", errors="ignore").decode("ascii")


# ---------------------------------------------------------------- HTTP
def fetch_html(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as res:
                return res.read().decode("utf-8", errors="replace")
        except Exception as e:
            if attempt == retries - 1:
                log(f"FETCH FAIL {url}: {e}")
                return ""
            time.sleep(2 * (attempt + 1))
    return ""


def url_for(page, rno, hd):
    if rno is None:
        return f"{BASE}{page}?jcd={JYOJO}&hd={hd}"
    return f"{BASE}{page}?rno={rno}&jcd={JYOJO}&hd={hd}"


def nfkc(s):
    return unicodedata.normalize("NFKC", s or "").strip()


def first_num(s, pattern=r"-?\d+(?:\.\d+)?"):
    m = re.search(pattern, nfkc(s))
    return m.group(0) if m else ""


def no_data(html):
    return (not html) or ("データがありません" in html)


def lines(node):
    if node is None:
        return []
    txt = node.get_text("\n", strip=True)
    return [nfkc(t) for t in txt.split("\n") if nfkc(t)]


# ---------------------------------------------------------------- レース一覧（締切時刻）
def parse_deadlines(html):
    """raceindex から 1R〜12R の締切予定時刻 [(h, m), ...] を返す。取れなければ []"""
    if no_data(html):
        return []
    found = [(int(h), int(m)) for h, m in re.findall(r"\b(\d{1,2}):(\d{2})\b", html)]
    found = [(h, m) for h, m in found if 8 <= h <= 23 and 0 <= m <= 59]
    # 連続する12個が「増加していて間隔が10〜60分」の並びを探す
    for i in range(0, len(found) - 11):
        seq = found[i:i + 12]
        ok = True
        for a, b in zip(seq, seq[1:]):
            gap = (b[0] * 60 + b[1]) - (a[0] * 60 + a[1])
            if not (10 <= gap <= 60):
                ok = False
                break
        if ok:
            return seq
    return []


def parse_event_type(deadlines):
    if not deadlines:
        return ""
    h = deadlines[0][0]
    if h < 12:
        return "デイ"
    if h < 17:
        return "ナイター"
    return "ミッドナイト"


# ---------------------------------------------------------------- 結果
def parse_raceresult(html):
    """({着順: 艇番}, 3連単払戻)"""
    if no_data(html):
        return ({}, "")
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.select("table.is-w495")
    places = {}
    pay = ""
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
                break
        if pay:
            break
    return (places, pay)


# ---------------------------------------------------------------- 出走表（勝率）
def parse_racelist(html):
    """{枠: {選手名, 級別, 全国勝率}}"""
    if no_data(html):
        return {}
    soup = BeautifulSoup(html, "html.parser")
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
        cell = tds[2]
        ct = nfkc(cell.get_text(" ", strip=True))
        m = re.search(r"\b([AB][12])\b", ct)
        a = cell.find("a")
        name = nfkc(a.get_text(" ", strip=True)).replace(" ", "") if a else ""
        vals = [first_num(t) for t in lines(tds[4])]
        vals = [v for v in vals if v != ""]
        lvals = [first_num(t) for t in lines(tds[5])] if len(tds) > 5 else []
        lvals = [v for v in lvals if v != ""]
        boats[waku] = {
            "選手名": name,
            "級別": m.group(1) if m else "",
            "全国勝率": vals[0] if vals else "",
            "当地勝率": lvals[0] if lvals else "",
        }
    return boats


# ---------------------------------------------------------------- 直前情報（天気・展示タイム）
def parse_weather(soup):
    w = soup.select_one(".weather1")
    out = {"天気": "", "風速": "", "波高": ""}
    if not w:
        return out
    node = w.select_one(".is-weather .weather1_bodyUnitLabelTitle")
    if node:
        out["天気"] = nfkc(node.get_text(strip=True))
    node = w.select_one(".is-wind .weather1_bodyUnitLabelData")
    if node:
        out["風速"] = first_num(node.get_text(), r"\d+")
    node = w.select_one(".is-wave .weather1_bodyUnitLabelData")
    if node:
        out["波高"] = first_num(node.get_text(), r"\d+")
    return out


def parse_beforeinfo(html):
    """({枠: 展示タイム}, 天気dict)"""
    if no_data(html):
        return ({}, {"天気": "", "風速": "", "波高": ""})
    soup = BeautifulSoup(html, "html.parser")
    ex = {}
    for tb in soup.select("tbody.is-fs12"):
        trs = tb.find_all("tr")
        if not trs:
            continue
        tds = trs[0].find_all("td")
        if len(tds) < 6:
            continue
        waku = first_num(tds[0].get_text(), r"\d")
        if waku:
            ex[waku] = first_num(tds[4].get_text(), r"\d\.\d{2}")
    return (ex, parse_weather(soup))


# ---------------------------------------------------------------- オッズ
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
    """{組番: オッズ(float) または None}"""
    if no_data(html):
        return {}
    vals = re.findall(r'class="oddsPoint[^"]*">([^<]*)<', html)
    if len(vals) != 120:
        return {}
    out = {}
    for combo, v in zip(ODDS_ORDER, vals):
        v = nfkc(v)
        try:
            out[combo] = float(v)
        except ValueError:
            out[combo] = None
    return out


# ---------------------------------------------------------------- 買い目と資金配分
def target_combos(a, b):
    """1-A-全 / 1-B-全 / 1-全-A / 1-全-B の組番（重複除去、順序維持）"""
    combos = []
    for x in range(1, 7):
        x = str(x)
        for c in (f"1-{a}-{x}", f"1-{b}-{x}", f"1-{x}-{a}", f"1-{x}-{b}"):
            p = c.split("-")
            if len(set(p)) == 3 and c not in combos:
                combos.append(c)
    return combos


def allocate(buys, budget):
    """1/オッズ比で配分、100円単位。buys=[(組番, オッズ)] → [(組番, オッズ, 金額)], 合成オッズ"""
    if not buys:
        return [], 0.0
    inv = [1.0 / o for _, o in buys]
    synth = 1.0 / sum(inv)
    units = budget // 100
    raw = [units * v / sum(inv) for v in inv]
    alloc = [max(1, int(r)) for r in raw]
    # 端数は端数の大きい順に配る（合計をちょうど units に）
    rest = units - sum(alloc)
    order = sorted(range(len(raw)), key=lambda i: raw[i] - int(raw[i]), reverse=True)
    i = 0
    while rest > 0 and alloc:
        alloc[order[i % len(order)]] += 1
        rest -= 1
        i += 1
    while rest < 0 and alloc:
        j = max(range(len(alloc)), key=lambda k: alloc[k])
        if alloc[j] > 1:
            alloc[j] -= 1
            rest += 1
        else:
            break
    return [(c, o, n * 100) for (c, o), n in zip(buys, alloc)], synth


# ---------------------------------------------------------------- LINE
def line_push(token, user_id, text):
    token, user_id = ascii_only(token), ascii_only(user_id)
    if not token or not user_id:
        log("LINE_CHANNEL_ACCESS_TOKEN / LINE_USER_ID が未設定")
        return False
    body = json.dumps({"to": user_id, "messages": [{"type": "text", "text": text}]},
                      ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        "https://api.line.me/v2/bot/message/push", method="POST", data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as res:
            return res.status == 200
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8", "replace")[:300]
        except Exception:
            pass
        log(f"LINE送信エラー: HTTP {e.code} {detail}")
        return False
    except Exception as e:
        log(f"LINE送信エラー: {e}")
        return False


def fmt_time(hm):
    return f"{hm[0]:02d}:{hm[1]:02d}" if hm else "--:--"


WEEKDAY = "月火水木金土日"


def build_message(ctx):
    d = ctx["date"]
    head = f"{d[4:6]}/{d[6:8]}({WEEKDAY[ctx['weekday']]})"
    if ctx["event_type"]:
        head += f" {ctx['event_type']}"
    r11 = ctx["r11"]
    L = [f"【大村12R 熱い日チェック】{head}"]
    L.append(f"11R結果 {r11['1']}-{r11['2']}-{r11['3']}"
             + (f"（{int(ctx['r11_pay']):,}円）" if ctx["r11_pay"].isdigit() else "")
             + " → 条件①成立")
    L.append(f"A={ctx['a']} B={ctx['b']}  12R締切 {fmt_time(ctx['d12'])}")
    L.append("")
    L.append("■見送りチェック")
    for label, ok, detail in ctx["checks"]:
        L.append(f"{'OK' if ok else 'NG'} {label}: {detail}")
    L.append("")
    if ctx["skip_reasons"]:
        L.append("判定: △ 見送り推奨（" + "・".join(ctx["skip_reasons"]) + "）")
    else:
        L.append("判定: ◎ 買い（見送り条件なし）")
    if ctx["note2"]:
        L.append(ctx["note2"])
    L.append("")
    if ctx["alloc"]:
        L.append(f"■買い目 {len(ctx['alloc'])}点 {ODDS_MAX:.0f}倍未満（{ctx['odds_time']}時点）")
        for c, o, yen in ctx["alloc"]:
            L.append(f"{c}  {o:.1f}倍  {yen:,}円")
        total = sum(y for _, _, y in ctx["alloc"])
        L.append(f"合計 {total:,}円 / 合成オッズ {ctx['synth']:.2f}倍"
                 f" → 的中時 約{int(total * ctx['synth']):,}円")
    elif ctx["odds_missing"]:
        L.append("■買い目: 12Rオッズを取得できませんでした（boatrace.jpで確認してください）")
    else:
        L.append(f"■買い目: {ODDS_MAX:.0f}倍未満の該当なし（全点40倍以上）")
    dropped = ctx["dropped"]
    if dropped:
        L.append("対象外(40倍以上): " + " ".join(dropped))
    return "\n".join(L)


# ---------------------------------------------------------------- 本体
def mark_done():
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as f:
            f.write("done=true\n")


def pc_state():
    if os.environ.get("GITHUB_ACTIONS"):
        return {}
    try:
        return json.load(open(STATE_PATH, encoding="utf-8"))
    except Exception:
        return {}


def pc_mark_done(date):
    if os.environ.get("GITHUB_ACTIONS"):
        return
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump({"done_date": date}, f)


def main():
    env = load_env()
    now = datetime.now(JST)
    date = env.get("NOTIFY_DATE") or now.strftime("%Y%m%d")
    force = env.get("NOTIFY_FORCE", "") in ("1", "true", "True")
    dry = env.get("NOTIFY_DRY_RUN", "") in ("1", "true", "True")
    quiet_days = env.get("NOTIFY_QUIET_DAYS", "") in ("1", "true", "True")
    budget = int(env.get("NOTIFY_BUDGET") or 10000)
    day = datetime.strptime(date, "%Y%m%d").replace(tzinfo=JST)

    if pc_state().get("done_date") == date:
        log("本日は処理済み（PC側の記録）: 終了")
        return

    log(f"対象日 {date}  現在 {now.strftime('%H:%M')}  force={force} dry={dry}")

    # 1. 締切時刻
    deadlines = parse_deadlines(fetch_html(url_for("raceindex", None, date)))
    if not deadlines:
        html_check = fetch_html(url_for("racelist", 12, date))
        if no_data(html_check):
            log("本日は大村の開催なし: 終了")
            mark_done()
            pc_mark_done(date)
            return
        log("締切時刻を読めなかったので時間帯チェックなしで進める")
        d11 = d12 = None
    else:
        d11, d12 = deadlines[10], deadlines[11]
        log(f"開催あり {parse_event_type(deadlines)}  11R締切 {fmt_time(d11)}  12R締切 {fmt_time(d12)}")
    event_type = parse_event_type(deadlines)

    if env.get("NOTIFY_PREVIEW", "") in ("1", "true", "True"):
        # 11Rを待たずに「今取れるもの」を全部表示して終わる（手動確認用）
        def _f(rno, page):
            return fetch_html(url_for(page, rno, date))
        with ThreadPoolExecutor(max_workers=6) as ex_:
            f10 = ex_.submit(_f, 10, "raceresult")
            f11 = ex_.submit(_f, 11, "raceresult")
            f12 = ex_.submit(_f, 12, "raceresult")
            fr = ex_.submit(_f, 12, "racelist")
            fb12 = ex_.submit(_f, 12, "beforeinfo")
            fb11 = ex_.submit(_f, 11, "beforeinfo")
            fo = ex_.submit(_f, 12, "odds3t")
        h10 = f10.result()
        r10p, p10 = parse_raceresult(h10)
        raw_rows = []
        try:
            for i, tb in enumerate(BeautifulSoup(h10, "html.parser").select("table.is-w495")[:2]):
                for tr in tb.select("tbody tr"):
                    raw_rows.append(f"t{i}: " + " | ".join(nfkc(td.get_text(' ', strip=True)) for td in tr.find_all("td")))
        except Exception as e:
            raw_rows.append(f"ERR {e}")
        r11p, p11 = parse_raceresult(f11.result())
        r12p, p12 = parse_raceresult(f12.result())
        print("PREVIEW_JSON " + json.dumps({
            "deadlines": deadlines, "event_type": event_type,
            "r10": r10p, "r10_pay": p10, "r11": r11p, "r11_pay": p11,
            "r12": r12p, "r12_pay": p12, "r10_rows": raw_rows,
            "racers12": parse_racelist(fr.result()),
            "before12": parse_beforeinfo(fb12.result()),
            "before11": parse_beforeinfo(fb11.result()),
            "odds12": parse_odds3t(fo.result()),
        }, ensure_ascii=False), flush=True)
        return

    def at(hm):
        return day.replace(hour=hm[0], minute=hm[1], second=0) if hm else None

    t11, t12 = at(d11), at(d12)
    if not force and t11 and t12:
        if now > t12 + timedelta(minutes=10):
            log("12R締切を過ぎている: 本日は終了")
            mark_done()
            pc_mark_done(date)
            return
        if now < t11 - timedelta(minutes=WAIT_BEFORE_11R_MIN):
            log(f"まだ早い（11R締切 {fmt_time(d11)} の{WAIT_BEFORE_11R_MIN}分前から待機）: 終了")
            return

    # 2. 11R結果を待つ（待っている間に、先に取れるもの＝10R結果・12R出走表を取っておく）
    loop_limit = (t12 + timedelta(minutes=5)) if t12 else (datetime.now(JST) + timedelta(minutes=75))
    r10 = {}
    racers = {}
    r11 = {}
    r11_pay = ""
    while True:
        r11, r11_pay = parse_raceresult(fetch_html(url_for("raceresult", 11, date)))
        if r11.get("1"):
            break
        if not r10:
            r10, _ = parse_raceresult(fetch_html(url_for("raceresult", 10, date)))
        if not racers:
            racers = parse_racelist(fetch_html(url_for("racelist", 12, date)))
        if force and date != now.strftime("%Y%m%d"):
            log("過去日の結果が取れない: 終了")
            return
        if datetime.now(JST) > loop_limit:
            log("12R締切までに11R結果が確定しなかった: 終了")
            mark_done()
            pc_mark_done(date)
            return
        log("11R結果 未確定、待機中")
        time.sleep(POLL_SEC)
    found_at = datetime.now(JST)
    log(f"11R結果 {r11.get('1')}-{r11.get('2')}-{r11.get('3')} 払戻{r11_pay}")

    token = env.get("LINE_CHANNEL_ACCESS_TOKEN", "")
    user_id = env.get("LINE_USER_ID", "")

    def send(text):
        print("----- 送信本文 -----\n" + text + "\n--------------------", flush=True)
        if dry:
            log("DRY RUN: LINEには送らない")
            return True
        ok = line_push(token, user_id, text)
        log("LINE送信 " + ("成功" if ok else "失敗"))
        return ok

    if r11.get("1") != "1":
        log("11Rの1着が1号艇ではない: 本日は見送り")
        if quiet_days:
            send(f"【大村12R】{date[4:6]}/{date[6:8]} 11R結果 {r11['1']}-{r11['2']}-{r11['3']}"
                 " → 1号艇1着ではないので今日は発動なし")
        mark_done()
        pc_mark_done(date)
        return

    a, b = r11.get("2", ""), r11.get("3", "")

    # 3. 判定材料（残りは並列で一気に取る）
    def fetch_rest():
        jobs = {
            "before12": lambda: parse_beforeinfo(fetch_html(url_for("beforeinfo", 12, date))),
            "before11": lambda: parse_beforeinfo(fetch_html(url_for("beforeinfo", 11, date))),
            "odds": lambda: parse_odds3t(fetch_html(url_for("odds3t", 12, date))),
        }
        if not r10:
            jobs["r10"] = lambda: parse_raceresult(fetch_html(url_for("raceresult", 10, date)))
        if not racers:
            jobs["racers"] = lambda: parse_racelist(fetch_html(url_for("racelist", 12, date)))
        with ThreadPoolExecutor(max_workers=len(jobs)) as ex_:
            futs = {k: ex_.submit(f) for k, f in jobs.items()}
            return {k: f.result() for k, f in futs.items()}

    got = fetch_rest()
    if "r10" in got:
        r10 = got["r10"][0]
    if "racers" in got:
        racers = got["racers"]
    same3 = bool(r10) and all(r10.get(k) == r11.get(k) for k in ("1", "2", "3"))
    ex, weather = got["before12"]
    weather_note = ""
    if not weather.get("天気"):
        # 12Rの直前情報がまだ出ていなければ、11Rの気象を使う
        weather = got["before11"][1]
        weather_note = "（11R時点）"
    odds = got["odds"]

    # 展示タイムは任意で少しだけ待てる（既定は待たない）
    ex_deadline = found_at + timedelta(minutes=EXHIBITION_WAIT_MIN)
    while EXHIBITION_WAIT_MIN > 0 and sum(1 for v in ex.values() if v) < 6:
        if datetime.now(JST) >= ex_deadline or (force and date != now.strftime("%Y%m%d")):
            break
        time.sleep(POLL_SEC)
        ex, _w = parse_beforeinfo(fetch_html(url_for("beforeinfo", 12, date)))
    odds_time = datetime.now(JST).strftime("%H:%M")

    # 4. 見送り判定
    checks, skip = [], []
    wtxt = weather.get("天気", "")
    detail = f"{wtxt or '不明'} 風{weather.get('風速') or '?'}m 波{weather.get('波高') or '?'}cm{weather_note}"
    rain = "雨" in wtxt or "雪" in wtxt
    checks.append(("天気", not rain, detail))
    if rain:
        skip.append("雨")

    try:
        wr1 = float(racers.get("1", {}).get("全国勝率", ""))
        others = [float(v["全国勝率"]) for k, v in racers.items() if k != "1" and v.get("全国勝率")]
        diff = wr1 - max(others)
        ok = diff > -1.0
        checks.append(("1号艇勝率", ok, f"{wr1:.2f}（他艇最高 {max(others):.2f}、差 {diff:+.2f}）"))
        if not ok:
            skip.append("勝率差")
    except (ValueError, TypeError):
        checks.append(("1号艇勝率", True, "取得できず"))

    times = {k: float(v) for k, v in ex.items() if v}
    if len(times) >= 6 and "1" in times:
        rank = 1 + sum(1 for k, v in times.items() if k != "1" and v < times["1"])
        ok = rank <= 3
        checks.append(("1号艇展示", ok, f"{times['1']:.2f}（{rank}位）"))
        if not ok:
            skip.append("展示4位以下")
    else:
        checks.append(("1号艇展示", True, "未発表→締切10分前ごろ直前情報で確認。4位以下なら見送り"))

    note2 = ""
    if same3:
        note2 = f"※②注意: 10Rと11Rの3着順が完全一致（{r10['1']}-{r10['2']}-{r10['3']}）"

    # 5. 買い目
    buys, dropped = [], []
    for c in target_combos(a, b):
        o = odds.get(c)
        if o is None:
            continue
        if o < ODDS_MAX:
            buys.append((c, o))
        else:
            dropped.append(f"{c}({o:.0f})")
    buys.sort(key=lambda x: x[1])
    alloc, synth = allocate(buys, budget)

    ctx = {
        "date": date, "weekday": day.weekday(), "event_type": event_type,
        "r11": r11, "r11_pay": r11_pay, "a": a, "b": b, "d12": d12,
        "checks": checks, "skip_reasons": skip, "note2": note2,
        "alloc": alloc, "synth": synth, "odds_time": odds_time,
        "odds_missing": not odds, "dropped": dropped,
    }
    text = build_message(ctx)
    ok = send(text)
    if ok or dry:
        mark_done()
        pc_mark_done(date)
    elif not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
