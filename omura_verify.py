"""
omura_verify.py — 大村システムの答え合わせ（毎日自動・手動どちらでも）

「アプリの数字・見送りサイン・LINE通知の判定は本当に合っているか」を、元データ（omura-boatrace の master
ブランチのCSV）から **別に計算し直した正解** と突き合わせて確かめる。ずれがあれば一覧にして知らせる。

正解の作り方（アプリ・LINE・build_flat_bet_log.py のコードは使わず、ここで一から計算する）
  ①11R 1着=1号艇 ②10Rと11Rの3連単が完全一致しない
  見送り7サイン（12R）: 雨/雪・曇り（結果CSVの12R_天気）/ 1号艇の全国勝率−他艇最高≤−1.0 / 1号艇の展示タイム
    出走艇中4位以下 / 1号艇以外にチルト0.5以上 / 1号艇チルト0.5以上 / 展示進入が枠番の小さい順でない
    （勝率・展示タイム・チルト・進入は選手ごとの元データ omura_racers.csv から。まとめ列 omura_daily_extra.csv は使わない）
  買い目 12R 3連単1番人気1点。的中＝結果ページの公式人気が1（12R_人気=1）、払戻＝実払戻（12R_払戻）
  返還 1番人気に入っている艇の本番STが F / L の日は収支0・回数に数えない（帳簿と同じ決まり）

照合するもの
  A. 元データ: 最終日、欠け、まとめ列（omura_daily_extra.csv）と選手ごとの元データの食い違い
  B. Webアプリ: 見送りサイン（lib/skipRules.ts をそのまま動かす）、定額シミュレーターの日別ログと
     成績（lib/flatBetLog.ts・lib/flatBetStats.ts をそのまま動かす）、サインの説明文の数字
  C. LINE通知: 見送り判定（omura_line_notify.judge をそのまま動かす）、送信記録（notify-state ブランチ）
  D. 本番（--live のときだけ。ネットが要る）: 直近の開催日について、本番アプリ /api/today と
     LINE通知の取得処理が boatrace.jp から読んだ値で判定し、元データの正解と比べる。前日分のデータが溜まっているかも見る

使い方
  python omura_verify.py --data-git origin/master --webapp ../omura-webapp
    （--data-git の代わりに --data <master のCSVがあるフォルダ> でもよい）
  オプション: --live（本番も確認） --line（ずれがあればLINEで知らせる。月曜は「問題なし」も1通）
             --report 結果.md（報告をファイルにも書く） --amount 20000（成績の定額）
  終了コード: ずれ（NG）があれば 1、なければ 0
"""
import argparse
import csv
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
JST = timezone(timedelta(hours=9))
APP_URL = "https://omura-webapp.vercel.app"
NOTIFY_SINCE = "2026-10-30"   # LINE送信記録（notify-state）が残り始めた日
NOTIFY_RECENT_DAYS = 7        # 送信記録の抜けを「NG」にする範囲（それより前は参考表示）

SIGNS = [
    ("rain", "12Rが雨（雪も含む）"),
    ("cloudy", "12Rが曇り"),
    ("winrate_gap", "1号艇の勝率が他艇より1.0以上低い"),
    ("exhibition_rank", "1号艇の展示タイムが4位以下"),
    ("other_tilt", "1号艇以外にチルト0.5以上"),
    ("boat1_tilt", "1号艇のチルトが0.5以上"),
    ("start_exhibition", "展示進入が枠なりでない"),
]
SIGN_LABEL = dict(SIGNS)
# LINE通知の見送り理由の文言 → サインid
LINE_SKIP_ID = {
    "雨": "rain", "曇り": "cloudy", "勝率差": "winrate_gap", "展示4位以下": "exhibition_rank",
    "他艇チルト0.5以上": "other_tilt", "1号艇チルト0.5以上": "boat1_tilt", "展示進入": "start_exhibition",
    "②不成立": "cond2",
}


# ---------------------------------------------------------------- 報告
class Report:
    def __init__(self):
        self.items = []   # (level, title, details)
        self.info = []    # 成績などの表示

    def add(self, level, title, details=None):
        self.items.append((level, title, list(details or [])))

    def ok(self, title, details=None):
        self.add("OK", title, details)

    def warn(self, title, details=None):
        self.add("注意", title, details)

    def ng(self, title, details=None):
        self.add("NG", title, details)

    def count(self, level):
        return sum(1 for lv, _, _ in self.items if lv == level)

    def markdown(self, now):
        ng, warn = self.count("NG"), self.count("注意")
        if ng:
            head = f"❌ ずれが {ng} 件あります（注意 {warn} 件）"
        elif warn:
            head = f"✅ 数字・判定はすべて一致（注意 {warn} 件）"
        else:
            head = "✅ すべて一致"
        L = [f"# 大村システムの答え合わせ {now.strftime('%Y-%m-%d %H:%M')}", "", f"**結論: {head}**", ""]
        if self.info:
            L += ["## 今の買い方の成績（元データから計算し直した値）", ""] + self.info + [""]
        mark = {"OK": "✅", "注意": "⚠️", "NG": "❌"}
        L += ["## 確認した項目", ""]
        for level in ("NG", "注意", "OK"):
            for lv, title, details in self.items:
                if lv != level:
                    continue
                L.append(f"- {mark[lv]} {title}")
                for d in details[:15]:
                    L.append(f"    - {d}")
                if len(details) > 15:
                    L.append(f"    - ほか {len(details) - 15} 件")
        return "\n".join(L) + "\n"

    def line_text(self, now, run_url=""):
        ng = [(t, d) for lv, t, d in self.items if lv == "NG"]
        warn = [(t, d) for lv, t, d in self.items if lv == "注意"]
        L = [f"【大村システム確認】{now.strftime('%m/%d')}"]
        if ng:
            L.append(f"ずれが{len(ng)}件見つかりました")
            for t, d in ng[:5]:
                L.append("× " + t)
                for x in d[:2]:
                    L.append("  " + x)
        else:
            L.append("アプリ・LINE通知・データの答え合わせ: すべて一致")
            if self.info:
                L.append(self.info[0].lstrip("- "))
        if warn:
            L.append(f"注意{len(warn)}件: " + " / ".join(t for t, _ in warn[:3]))
        if run_url:
            L.append("詳しく: " + run_url)
        return "\n".join(L)[:4900]


# ---------------------------------------------------------------- 元データ
def read_csv(path):
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def num(s):
    """'6.83' → 6.83、'F.01' → 0.01（STの F/L は落とす）、空や読めない値は None"""
    if s is None:
        return None
    s = str(s).strip().replace("F", "").replace("L", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


class Data:
    def __init__(self, folder):
        p = lambda n: os.path.join(folder, n)
        self.results = read_csv(p("omura_results.csv"))
        self.res = {r["日付"]: r for r in self.results}
        self.extra = {r["日付"]: r for r in read_csv(p("omura_daily_extra.csv"))} if os.path.exists(p("omura_daily_extra.csv")) else {}
        self.boats = {}
        for r in read_csv(p("omura_racers.csv")):
            if r.get("R") == "12":
                self.boats.setdefault(r["日付"], {})[r["枠"]] = r
        self.branch = {}
        if os.path.exists(p("omura_racers_extra.csv")):
            for r in read_csv(p("omura_racers_extra.csv")):
                if r.get("R") == "12":
                    self.branch.setdefault(r["日付"], {})[r["枠"]] = r.get("支部", "")
        self.odds = {}
        if os.path.exists(p("omura_12r_odds.csv")):
            for r in read_csv(p("omura_12r_odds.csv")):
                d = r.pop("日付")
                o = {c: num(v) for c, v in r.items() if num(v) is not None}
                if o:
                    self.odds[d] = o
        self.last = max(self.res)


def trio(r, rno):
    return (r[f"{rno}R_1着"], r[f"{rno}R_2着"], r[f"{rno}R_3着"])


def cond1(r):
    return r["11R_1着"] == "1"


def cond2(r):
    return trio(r, 10) != trio(r, 11)


def has12(r):
    return bool(r.get("12R_1着") or r.get("12R_払戻"))


def entry_order(boats):
    """展示進入（コース）の小さい順に艇番を並べる。進入が無い艇は除く"""
    en = [(int(b["展示進入"]), w) for w, b in boats.items() if (b.get("展示進入") or "").isdigit()]
    return [w for _, w in sorted(en)]


def correct_signs(r, boats):
    """見送り7サインの正解。True=見送り / False=該当なし / None=データが無く判定できない"""
    s = {}
    w = r.get("12R_天気", "")
    s["rain"] = (("雨" in w) or ("雪" in w)) if w else None
    s["cloudy"] = ("曇" in w) if w else None
    wr = {k: num(b.get("全国勝率")) for k, b in boats.items()}
    others = [v for k, v in wr.items() if k != "1" and v is not None]
    s["winrate_gap"] = (round(wr["1"] - max(others), 2) <= -1.0) if wr.get("1") is not None and others else None
    t = {k: num(b.get("展示タイム")) for k, b in boats.items()}
    t = {k: v for k, v in t.items() if v is not None}
    s["exhibition_rank"] = (1 + sum(1 for k, v in t.items() if k != "1" and v < t["1"]) >= 4) if "1" in t and len(t) >= 5 else None
    tl = {k: num(b.get("チルト")) for k, b in boats.items()}
    tl = {k: v for k, v in tl.items() if v is not None}
    s["other_tilt"] = any(v >= 0.5 for k, v in tl.items() if k != "1") if len(tl) >= 5 else None
    s["boat1_tilt"] = (tl["1"] >= 0.5) if "1" in tl else None
    order = entry_order(boats)
    s["start_exhibition"] = (order != sorted(order, key=int)) if len(order) >= 5 else None
    return s


def favorite(data, d):
    """オッズCSVの1番人気（返還の判定にだけ使う）。同じオッズが並ぶ日は公式人気=1の結果を優先"""
    o = data.odds.get(d)
    if not o:
        return None
    lo = min(o.values())
    ties = [c for c, v in o.items() if v == lo]
    r = data.res.get(d, {})
    res12 = "-".join(trio(r, 12)) if r else ""
    return res12 if (len(ties) > 1 and res12 in ties and r.get("12R_人気") == "1") else ties[0]


def outcome(data, d):
    """('hit', 払戻) / ('miss', 0) / ('refund', 0) / ('unknown', 0)"""
    r = data.res[d]
    boats = data.boats.get(d, {})
    fl = {w for w, b in boats.items() if any(x in (b.get("本番ST") or "") for x in ("F", "L"))}
    if fl:
        fav = favorite(data, d)
        if fav is None:
            return ("unknown", 0)
        if set(fav.split("-")) & fl:
            return ("refund", 0)
    pop = r.get("12R_人気", "")
    if not pop:
        return ("unknown", 0)
    if pop == "1":
        return ("hit", int(r["12R_払戻"] or 0))
    return ("miss", 0)


def build_days(data):
    """①成立日ごとに、正解の判定と結果をまとめる"""
    days = []
    for r in data.results:
        if not cond1(r):
            continue
        d = r["日付"]
        boats = data.boats.get(d, {})
        sg = correct_signs(r, boats)
        oc = outcome(data, d) if has12(r) else ("nodata", 0)
        days.append({
            "d": d, "r": r, "boats": boats, "cond2": cond2(r), "signs": sg,
            "go": cond2(r) and not any(v is True for v in sg.values()),
            "outcome": oc[0], "pay": oc[1],
        })
    return days


def flat_stats(rows, amount):
    """定額 amount 円で毎回1点買いしたときの成績（rows は [(日付, 100円あたり払戻)]）"""
    cum = peak = 0
    dd = 0
    streak = max_streak = 0
    hits = invested = returned = 0
    for d, pay in rows:
        ret = round(pay * amount / 100)
        cum += ret - amount
        invested += amount
        returned += ret
        if pay > 0:
            hits += 1
            streak = 0
        else:
            streak += 1
            max_streak = max(max_streak, streak)
        peak = max(peak, cum)
        dd = min(dd, cum - peak)
    return {"days": len(rows), "hits": hits, "invested": invested, "returned": returned,
            "total": cum, "maxStreak": max_streak, "maxDD": dd}


def man(v):
    s = f"{abs(v) / 10000:.1f}".rstrip("0").rstrip(".")
    return ("−" if v < 0 else "+" if v > 0 else "") + s + "万"


def fmt_stats(label, s):
    if not s["days"]:
        return f"- {label}: 対象日なし"
    roi = s["returned"] / s["invested"] * 100
    return (f"- {label}: {s['days']}日 的中{s['hits']}回（{s['hits'] / s['days'] * 100:.1f}%）"
            f" 回収率{roi:.1f}% 収支{man(s['total'])} 最長{s['maxStreak']}連敗 最大落ち込み{man(s['maxDD'])}")


# ---------------------------------------------------------------- A. 元データ
def check_data(rep, data, days, today):
    gap = (today - datetime.strptime(data.last, "%Y-%m-%d").date()).days
    detail = [f"最終日 {data.last}（{gap}日前）、{len(data.results)}日分"]
    lack = [x["d"] for x in days if x["cond2"] and len(x["boats"]) < 6]
    lack_odds = [x["d"] for x in days if x["cond2"] and x["d"] not in data.odds]
    unknown = [x["d"] for x in days if x["outcome"] == "unknown"]
    if lack:
        detail.append(f"12Rの選手データが6艇そろっていない日 {len(lack)}日（欠場の日を含む）: " + "・".join(lack[-5:]))
    if lack_odds:
        detail.append(f"12Rのオッズが無い日 {len(lack_odds)}日: " + "・".join(lack_odds[-5:]))
    if unknown:
        rep.warn("結果（公式人気）が読めず的中か決められない日がある", [f"{d}" for d in unknown])
    rep.ok("元データの読み込み", detail)

    # まとめ列（omura_daily_extra.csv、build_flat_bet_log.py が使う）と選手ごとの元データの食い違い
    diffs = []
    for x in days:
        e = data.extra.get(x["d"])
        if not e or not x["cond2"]:
            continue
        s = x["signs"]
        g = num(e.get("12R_1号艇勝率差"))
        if g is not None and s["winrate_gap"] is not None and (g <= -1.0) != s["winrate_gap"]:
            diffs.append(f"{x['d']} 勝率差: まとめ列 {g:+.2f} / 元データ {'≤−1.0' if s['winrate_gap'] else '>−1.0'}")
        k = num(e.get("12R_1号艇展示タイム順位"))
        if k is not None and s["exhibition_rank"] is not None and (k >= 4) != s["exhibition_rank"]:
            diffs.append(f"{x['d']} 展示順位: まとめ列 {k:.0f}位 / 元データ {'4位以下' if s['exhibition_rank'] else '3位以内'}")
        wk = e.get("12R_展示枠なり", "")
        if wk in ("0", "1") and s["start_exhibition"] is not None and (wk == "0") != s["start_exhibition"]:
            diffs.append(f"{x['d']} 展示進入: まとめ列 {'枠なりでない' if wk == '0' else '枠なり'} / 元データ {'-'.join(entry_order(x['boats']))}")
    if diffs:
        rep.ng("まとめ列（omura_daily_extra.csv）と選手ごとの元データが食い違う日がある", diffs)
    else:
        rep.ok("まとめ列（omura_daily_extra.csv）と選手ごとの元データ: 勝率差・展示順位・展示進入すべて一致")


# ---------------------------------------------------------------- B. Webアプリ
def race_info(data, x):
    """元データから、アプリの見送りサインに渡す RaceInfo（lib/boatrace.ts の形）を作る"""
    r, boats = x["r"], x["boats"]
    br = data.branch.get(x["d"], {})
    bl = []
    for w in sorted(boats):
        b = boats[w]
        bl.append({
            "waku": w, "name": b.get("選手名", ""), "klass": b.get("級別", ""), "branch": br.get(w, ""),
            "winRate": num(b.get("全国勝率")), "localWinRate": num(b.get("当地勝率")), "avgST": num(b.get("平均ST")),
            "exhibitionTime": num(b.get("展示タイム")), "tilt": num(b.get("チルト")),
        })
    w = r.get("12R_天気", "")
    return {
        "date": x["d"], "rno": 12, "raceListAvailable": len(bl) >= 6, "beforeInfoAvailable": True,
        "raceType": "", "title": "", "boats": bl,
        "weather": {"weather": w, "wind": r.get("12R_風速", ""), "wave": r.get("12R_波高", "")} if w else None,
        "startExhibition": entry_order(boats),
    }


def run_webapp(webapp, cases, amount):
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as f:
        json.dump({"amount": amount, "cases": cases}, f, ensure_ascii=False)
        path = f.name
    try:
        out = subprocess.run(["node", "--no-warnings", os.path.join(webapp, "scripts", "verify_export.mjs"), path],
                             capture_output=True, text=True, encoding="utf-8", check=False)
    finally:
        os.unlink(path)
    if out.returncode != 0:
        raise RuntimeError(out.stderr.strip()[-800:])
    return json.loads(out.stdout)


def status_of(v):
    return {True: "skip", False: "ok", None: "pending"}[v]


JA_STATUS = {"skip": "見送り", "ok": "該当なし", "pending": "判定できず"}


def evidence_numbers(L):
    """[(日付, 払戻)] → (日数, 的中率%, 回収率%)"""
    n = len(L)
    if not n:
        return (0, 0.0, 0.0)
    h = sum(1 for _, p in L if p > 0)
    return (n, h / n * 100, sum(p for _, p in L) / n)


def evidence_text(fire, rest):
    a, b = evidence_numbers(fire), evidence_numbers(rest)
    return f"該当{a[0]}日: 1番人気的中{a[1]:.1f}%・回収率{a[2]:.0f}%（それ以外 {b[1]:.1f}%・{b[2]:.0f}%"


def check_webapp(rep, data, days, webapp, amount):
    targets = [x for x in days if x["cond2"] and len(x["boats"]) >= 5]
    try:
        out = run_webapp(webapp, [{"date": x["d"], "info": race_info(data, x)} for x in targets], amount)
    except Exception as e:
        rep.ng("Webアプリの計算を動かせなかった", [str(e)])
        return None

    # B1 見送りサイン
    diffs = []
    for x in targets:
        app = out["signs"][x["d"]]["signs"]
        for sid, _ in SIGNS:
            want = status_of(x["signs"][sid])
            got = app.get(sid)
            if got != want:
                diffs.append(f"{x['d']} {SIGN_LABEL[sid]}: アプリ {JA_STATUS.get(got, got)} / 正解 {JA_STATUS[want]}")
    if diffs:
        rep.ng(f"Webアプリの見送りサインが正解と食い違う（①②成立 {len(targets)}日を確認）", diffs)
    else:
        rep.ok(f"Webアプリの見送りサイン: ①②成立 {len(targets)}日 × 7サインすべて正解と一致")

    # B2 定額シミュレーターの日別ログ
    rng_to = out["range"]["to"]
    log = {r["d"]: r for r in out["log"]}
    want_go = {x["d"]: x for x in days if x["go"] and x["outcome"] in ("hit", "miss") and x["d"] <= rng_to}
    want_ref = sorted(x["d"] for x in days if x["go"] and x["outcome"] == "refund" and x["d"] <= rng_to)
    diffs = []
    for d in sorted(set(log) | set(want_go)):
        a, w = log.get(d), want_go.get(d)
        if a and not w:
            why = next((x for x in days if x["d"] == d), None)
            reason = "①②不成立" if not why or not why["cond2"] else \
                ("見送りサイン " + "・".join(SIGN_LABEL[k] for k, v in why["signs"].items() if v)) if not why["go"] else \
                {"refund": "返還（買い目の艇がF/L）", "unknown": "結果が読めない", "nodata": "12Rの結果なし"}.get(why["outcome"], why["outcome"])
            diffs.append(f"{d}: アプリは買った日に入れているが、正解では対象外（{reason}）")
        elif w and not a:
            diffs.append(f"{d}: 正解では買う日だが、アプリのログに無い")
        elif (a["pay"] > 0) != (w["pay"] > 0) or a["pay"] != w["pay"]:
            diffs.append(f"{d}: 払戻 アプリ {a['pay']}円 / 正解 {w['pay']}円（公式人気 {w['r'].get('12R_人気')}・結果 {'-'.join(trio(w['r'], 12))}）")
    if sorted(out.get("refunds", [])) != want_ref:
        diffs.append(f"返還で外した日: アプリ {out.get('refunds')} / 正解 {want_ref}")
    if diffs:
        rep.ng("定額シミュレーターの日別ログが正解と食い違う", diffs)
    else:
        rep.ok(f"定額シミュレーターの日別ログ: {len(log)}日すべて買う日・的中・払戻が正解と一致（返還{len(want_ref)}日も一致）")

    # B3 成績（画面に出る数字）
    rows = sorted((d, x["pay"]) for d, x in want_go.items())
    want_all = flat_stats(rows, amount)
    diffs = []
    keys = [("days", "日数"), ("hits", "的中回数"), ("invested", "投入額"), ("returned", "払戻額"),
            ("total", "収支"), ("maxStreak", "最長連敗"), ("maxDD", "最大落ち込み")]

    def cmp(label, app, want):
        for k, ja in keys:
            if app.get(k) != want[k]:
                f = (lambda v: f"{v:,}" if isinstance(v, int) else str(v))
                diffs.append(f"{label} {ja}: アプリ {f(app.get(k))} / 正解 {f(want[k])}")
    cmp("全期間", out["stats"]["all"], want_all)
    years = sorted({d[:4] for d, _ in rows})
    for y in years:
        cmp(f"{y}年", out["stats"]["years"].get(y, {}), flat_stats([r for r in rows if r[0].startswith(y)], amount))
    if diffs:
        rep.ng(f"定額シミュレーターの成績（{amount:,}円定額）が正解と食い違う", diffs)
    else:
        rep.ok(f"定額シミュレーターの成績（{amount:,}円定額）: 全期間と{len(years)}年分の日数・的中・収支・連敗・落ち込みが一致")

    # B4 アプリの収支表がデータに追いついているか
    behind = [x["d"] for x in days if x["go"] and x["d"] > rng_to]
    if data.last > rng_to:
        msg = f"アプリの収支表は {rng_to} まで、元データは {data.last} まで"
        if behind:
            rep.warn(msg + f"（買う日 {len(behind)}日分が未反映: {'・'.join(behind[-5:])}）",
                     ["omura-webapp で scripts/build_flat_bet_log.py を流し直すと反映される"])
        else:
            rep.ok(msg + "（その間に買う日は無いので数字は同じ）")

    # B5 サインの説明文の数字（①②成立日・返還除く、EVIDENCE_UNTIL まで）
    until = out.get("evidenceUntil") or ""
    base = [x for x in days if x["cond2"] and x["outcome"] in ("hit", "miss") and x["d"] <= until]
    diffs = []
    for rule in out["rules"]:
        if not rule["enabled"] or rule["id"] not in SIGN_LABEL:
            continue
        fire = [(x["d"], x["pay"]) for x in base if x["signs"][rule["id"]] is True]
        rest = [(x["d"], x["pay"]) for x in base if x["signs"][rule["id"]] is not True]
        want = evidence_text(fire, rest)
        if not rule["evidence"].startswith(want):
            diffs.append(f"{rule['label']}: アプリ「{rule['evidence']}」 / 正解「{want}…」")
    boost_fn = {
        "local_branch": lambda x: data.branch.get(x["d"], {}).get("1") == "長崎",
        "b_class": lambda x: (x["boats"].get("1", {}).get("級別") or "").startswith("B"),
    }
    # 追い風サインは「買う日（①②成立・7サインなし）」の中での数字
    buy = [x for x in base if x["go"]]
    for rule in out.get("boostRules", []):
        fn = boost_fn.get(rule["id"])
        if not fn:
            continue
        fire = [(x["d"], x["pay"]) for x in buy if fn(x)]
        rest = [(x["d"], x["pay"]) for x in buy if not fn(x)]
        want = f"買う日{len(buy)}日のうち" + evidence_text(fire, rest)
        if not rule["evidence"].startswith(want):
            diffs.append(f"{rule['label']}: アプリ「{rule['evidence']}」 / 正解「{want}…」")
        # LINE通知の本文に書いてある数字も同じか
        n, hr, roi = evidence_numbers(fire)
        key = {"local_branch": "長崎支部", "b_class": "B級1号艇"}[rule["id"]]
        src = open(os.path.join(HERE, "omura_line_notify.py"), encoding="utf-8").read()
        line_txt = next((ln_ for ln_ in src.splitlines() if key in ln_ and "boosts.append" in ln_), "")
        if f"該当{n}日" not in line_txt or f"回収率{roi:.0f}%" not in line_txt:
            diffs.append(f"LINE通知の追い風の文言（{key}）: 「{line_txt.strip()[:120]}」 / 正解 該当{n}日・回収率{roi:.0f}%")
    if diffs:
        rep.ng(f"サインの説明文の数字が数え直しと合わない（①②成立 {len(base)}日、{until} まで）", diffs)
    else:
        rep.ok(f"サインの説明文の数字: ①②成立 {len(base)}日（{until} まで）で数え直して全部一致")
    return out


# ---------------------------------------------------------------- C. LINE通知
def line_inputs(x, data):
    r, boats = x["r"], x["boats"]
    br = data.branch.get(x["d"], {})
    racers = {w: {"選手名": b.get("選手名", ""), "級別": b.get("級別", ""), "支部": br.get(w, ""),
                  "全国勝率": b.get("全国勝率", ""), "当地勝率": b.get("当地勝率", "")} for w, b in boats.items()}
    ex = {w: b.get("展示タイム", "") for w, b in boats.items()}
    weather = {"天気": r.get("12R_天気", ""), "風速": r.get("12R_風速", ""), "波高": r.get("12R_波高", "")}
    extra12 = {"tilt": {w: b.get("チルト", "") for w, b in boats.items() if b.get("チルト", "") != ""},
               "start": entry_order(boats)}
    r10 = dict(zip(("1", "2", "3"), trio(r, 10)))
    r11 = dict(zip(("1", "2", "3"), trio(r, 11)))
    return racers, ex, weather, extra12, r10, r11


def line_skip_ids(skip):
    return {LINE_SKIP_ID.get(s, s) for s in skip}


def check_line_logic(rep, data, days, ln):
    targets = [x for x in days if len(x["boats"]) >= 5]
    diffs = []
    for x in targets:
        racers, ex, weather, extra12, r10, r11 = line_inputs(x, data)
        _, skip, _ = ln.judge(racers, ex, weather, "", extra12, r10, r11, "favorite")
        got = line_skip_ids(skip)
        want = {k for k, v in x["signs"].items() if v is True} | (set() if x["cond2"] else {"cond2"})
        if got != want:
            only_l = [SIGN_LABEL.get(k, "②不成立") for k in got - want]
            only_w = [SIGN_LABEL.get(k, "②不成立") for k in want - got]
            diffs.append(f"{x['d']}: " + " / ".join(filter(None, [
                ("LINEだけ見送り: " + "・".join(only_l)) if only_l else "",
                ("正解では見送りなのにLINEは見送らない: " + "・".join(only_w)) if only_w else ""])))
    if diffs:
        rep.ng(f"LINE通知の見送り判定が正解と食い違う（①成立 {len(targets)}日を確認）", diffs)
    else:
        rep.ok(f"LINE通知の見送り判定: ①成立 {len(targets)}日すべて正解と一致（②と7サイン）")


def git(*args):
    return subprocess.run(["git", "-C", HERE, *args], capture_output=True, text=True, encoding="utf-8", check=False)


def check_line_sent(rep, data, days, today):
    if git("fetch", "--quiet", "origin", "notify-state").returncode != 0:
        rep.warn("LINEの送信記録（notify-state ブランチ）を取得できなかった")
        return
    subjects = git("log", "FETCH_HEAD", "--format=%s").stdout.splitlines()
    sent = {}
    for s in subjects:
        m = re.match(r"sent (\d{8}) by (\w+)", s)
        if m:
            d = f"{m[1][:4]}-{m[1][4:6]}-{m[1][6:]}"
            sent.setdefault(d, []).append(m[2])
    expect = [x["d"] for x in days if x["d"] >= NOTIFY_SINCE]
    recent_from = (today - timedelta(days=NOTIFY_RECENT_DAYS)).isoformat()
    miss_recent = [d for d in expect if d not in sent and d >= recent_from]
    miss_old = [d for d in expect if d not in sent and d < recent_from]
    dup = [f"{d}（{'・'.join(w)}）" for d, w in sorted(sent.items()) if len(w) > 1]
    extra = [d for d in sorted(sent) if d in data.res and d not in {x["d"] for x in days}]
    bad = [p for p in git("ls-tree", "-r", "--name-only", "FETCH_HEAD").stdout.splitlines()
           if not re.fullmatch(r"sent/\d{8}\.txt", p)]
    if miss_recent:
        rep.ng("LINE通知が送られていない日がある（11R 1着=1号艇の日）",
               [f"{d}: 送信記録なし（PCが止まっていた／GitHubの定時起動が遅れた可能性）" for d in miss_recent])
    if dup:
        rep.ng("同じ日にLINE通知が2回以上送られている", dup)
    if extra:
        rep.ng("11R 1着が1号艇でない日にLINE通知が送られている", extra)
    if bad:
        rep.ng("送信記録のファイル名が壊れている（GitHub側から送信済みが見えず、二重送信になりうる）",
               [repr(p) for p in bad] + [
                   "PCの PowerShell で1回: git -C \"$env:USERPROFILE\\omura-notify\" pull --ff-only origin main",
                   "（omura_line_notify.py は修正済み。PCが新しい版で次に送った時点で記録が直り、この項目は消える）"])
    if not (miss_recent or dup or extra or bad):
        rec = [d for d in expect if d >= recent_from]
        rep.ok(f"LINEの送信記録: {NOTIFY_SINCE} 以降の対象 {len(expect)}日すべて1通ずつ送信済み"
               + (f"（直近{NOTIFY_RECENT_DAYS}日 {len(rec)}日）" if rec else ""))
    if miss_old:
        rep.warn(f"{NOTIFY_RECENT_DAYS}日より前に送信記録が無い日（参考）", miss_old)


# ---------------------------------------------------------------- D. 本番（ネット）
def fetch_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "omura-verify"})
    with urllib.request.urlopen(req, timeout=60) as res:
        return json.loads(res.read().decode("utf-8"))


def live_line(ln, d):
    """LINE通知と同じ取得処理で、過去日の判定材料を boatrace.jp から読む"""
    hd = d.replace("-", "")
    r10, _ = ln.parse_raceresult(ln.fetch_html(ln.url_for("raceresult", 10, hd)))
    r11, _ = ln.parse_raceresult(ln.fetch_html(ln.url_for("raceresult", 11, hd)))
    r12, p12 = ln.parse_raceresult(ln.fetch_html(ln.url_for("raceresult", 12, hd)))
    racers = ln.parse_racelist(ln.fetch_html(ln.url_for("racelist", 12, hd)))
    ln.BEFORE_EXTRA.clear()
    ex, weather = ln.parse_beforeinfo(ln.fetch_html(ln.url_for("beforeinfo", 12, hd)))
    extra12 = ln.BEFORE_EXTRA.get(max(ln.BEFORE_EXTRA)) if ln.BEFORE_EXTRA else {}
    return r10, r11, r12, p12, racers, ex, weather, extra12 or {}


def check_live(rep, data, days, ln, today, n_days):
    dates = sorted(data.res)[-n_days:]
    app_diff, line_diff, data_diff, weather_note, errors = [], [], [], [], []
    for d in dates:
        r = data.res[d]
        boats = data.boats.get(d, {})
        want = correct_signs(r, boats)
        # 本番アプリ
        try:
            js = fetch_json(f"{APP_URL}/api/today?date={d}")
            got = {s["id"]: s["status"] for s in js.get("signs", [])}
            if "pending" in got.values():
                # アプリは boatrace.jp の読み取りを5分キャッシュし、期限切れ後の最初の1回は古い控えを返す
                # （例: 当日の展示前に読んだもの）。少し待ってもう一度聞き直す
                time.sleep(20)
                js = fetch_json(f"{APP_URL}/api/today?date={d}")
                got = {s["id"]: s["status"] for s in js.get("signs", [])}
            for sid, _ in SIGNS:
                w = status_of(want[sid])
                if got.get(sid) != w:
                    line = f"{d} {SIGN_LABEL[sid]}: 本番アプリ {JA_STATUS.get(got.get(sid), got.get(sid))} / 元データ {JA_STATUS[w]}"
                    (weather_note if sid in ("rain", "cloudy") else app_diff).append(
                        line + (f"（アプリは展示時点の天気「{(js.get('weather') or {}).get('weather', '')}」、元データは結果の天気「{r.get('12R_天気')}」）" if sid in ("rain", "cloudy") else ""))
        except Exception as e:
            errors.append(f"{d} 本番アプリ: {e}")
        # LINE通知の取得処理
        try:
            r10, r11, r12, p12, racers, ex, weather, extra12 = live_line(ln, d)
            if r12.get("1") and (r12.get("1"), r12.get("2"), r12.get("3")) != trio(r, 12):
                data_diff.append(f"{d} 12R結果: boatrace.jp {r12.get('1')}-{r12.get('2')}-{r12.get('3')} / 元データ {'-'.join(trio(r, 12))}")
            if r11.get("1") and (r11.get("1"), r11.get("2"), r11.get("3")) != trio(r, 11):
                data_diff.append(f"{d} 11R結果: boatrace.jp {r11.get('1')}-{r11.get('2')}-{r11.get('3')} / 元データ {'-'.join(trio(r, 11))}")
            if r11.get("1") == "1":
                _, skip, _ = ln.judge(racers, ex, weather, "", extra12, r10, r11, "favorite")
                got = line_skip_ids(skip) - {"cond2"}
                w = {k for k, v in want.items() if v is True}
                for sid in sorted(got ^ w):
                    line = f"{d} {SIGN_LABEL[sid]}: LINEの取得処理 {'見送り' if sid in got else '該当なし'} / 元データ {'見送り' if sid in w else '該当なし'}"
                    (weather_note if sid in ("rain", "cloudy") else line_diff).append(line)
        except Exception as e:
            errors.append(f"{d} LINEの取得処理: {e}")
    span = f"{dates[0]}〜{dates[-1]} の {len(dates)}日"
    (rep.ng if app_diff else rep.ok)(
        "本番アプリ（boatrace.jp から読んだ値）の見送りサインが元データと食い違う" if app_diff
        else f"本番アプリの見送りサイン: {span}、天気以外のサインが元データと一致", app_diff)
    (rep.ng if line_diff else rep.ok)(
        "LINE通知の取得処理（boatrace.jp から読んだ値）の判定が元データと食い違う" if line_diff
        else f"LINE通知の取得処理: {span}（①成立日）、天気以外のサインが元データと一致", line_diff)
    if data_diff:
        rep.ng("元データの着順が boatrace.jp と食い違う", data_diff)
    if weather_note:
        rep.warn("天気の見方が違う日（アプリ・LINEは展示時点、元データはレース結果の天気）", weather_note)
    if errors:
        rep.warn("本番の確認ができなかったもの（通信エラーなど）", errors)

    # 前日のデータが溜まっているか（PCの毎朝の取得）
    y = (today - timedelta(days=1)).isoformat()
    if y not in data.res:
        try:
            r12, _ = ln.parse_raceresult(ln.fetch_html(ln.url_for("raceresult", 12, y.replace("-", ""))))
            if r12.get("1"):
                rep.ng(f"前日 {y} は開催があったのに元データに入っていない（PCの毎朝08:05の取得が動いていない可能性）")
            else:
                rep.ok(f"前日 {y} は大村の開催なし（データが無いのは正常）")
        except Exception as e:
            rep.warn(f"前日 {y} の開催有無を確認できなかった: {e}")
    else:
        rep.ok(f"前日 {y} のデータは取得済み")


# ---------------------------------------------------------------- 本体
DATA_FILES = ["omura_results.csv", "omura_daily_extra.csv", "omura_racers.csv", "omura_racers_extra.csv", "omura_12r_odds.csv"]


def data_from_git(ref):
    """master ブランチのCSVを、チェックアウトせずに一時フォルダへ書き出す"""
    if "/" in ref:
        remote, branch = ref.split("/", 1)
        git("fetch", "--quiet", remote, branch)
    out = tempfile.mkdtemp(prefix="omura-data-")
    for name in DATA_FILES:
        r = subprocess.run(["git", "-C", HERE, "show", f"{ref}:{name}"], capture_output=True, check=False)
        if r.returncode == 0:
            with open(os.path.join(out, name), "wb") as f:
                f.write(r.stdout)
    return out


def main():
    ap = argparse.ArgumentParser(description="大村システムの答え合わせ")
    ap.add_argument("--data", help="omura-boatrace master のCSVがあるフォルダ")
    ap.add_argument("--data-git", metavar="REF", help="フォルダの代わりに、このリポジトリの git から直接読む（例 origin/master）")
    ap.add_argument("--webapp", help="omura-webapp のフォルダ（無ければアプリの照合は飛ばす）")
    ap.add_argument("--amount", type=int, default=20000)
    ap.add_argument("--live", action="store_true", help="本番アプリと boatrace.jp も確認する")
    ap.add_argument("--live-days", type=int, default=3)
    ap.add_argument("--no-sent-log", action="store_true", help="LINE送信記録の確認を飛ばす")
    ap.add_argument("--line", action="store_true", help="ずれがあればLINEで知らせる（月曜は問題なしも1通）")
    ap.add_argument("--line-always", action="store_true", help="結果に関係なくLINEで送る（テスト用）")
    ap.add_argument("--report", help="報告を書き出すファイル")
    a = ap.parse_args()

    now = datetime.now(JST)
    today = now.date()
    rep = Report()
    if a.data_git:
        a.data = data_from_git(a.data_git)
    if not a.data:
        ap.error("--data か --data-git を指定してください")
    data = Data(a.data)
    days = build_days(data)

    sys.path.insert(0, HERE)
    import omura_line_notify as ln  # LINE通知の判定をそのまま使う

    go_rows = sorted((x["d"], x["pay"]) for x in days if x["go"] and x["outcome"] in ("hit", "miss"))
    refunds = [x["d"] for x in days if x["go"] and x["outcome"] == "refund"]
    rep.info.append(fmt_stats(f"全期間 {go_rows[0][0]}〜{data.last}（{a.amount // 10000}万定額）", flat_stats(go_rows, a.amount)))
    for y in sorted({d[:4] for d, _ in go_rows}):
        rep.info.append(fmt_stats(f"{y}年", flat_stats([r for r in go_rows if r[0].startswith(y)], a.amount)))
    if refunds:
        rep.info.append(f"- 返還で数えない日: {'・'.join(refunds)}")

    check_data(rep, data, days, today)
    if a.webapp:
        check_webapp(rep, data, days, a.webapp, a.amount)
    check_line_logic(rep, data, days, ln)
    if not a.no_sent_log:
        check_line_sent(rep, data, days, today)
    if a.live:
        check_live(rep, data, days, ln, today, a.live_days)

    md = rep.markdown(now)
    print(md)
    if a.report:
        with open(a.report, "w", encoding="utf-8") as f:
            f.write(md)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(md)

    ng = rep.count("NG")
    if a.line or a.line_always:
        weekly = today.weekday() == 0
        if ng or weekly or a.line_always:
            env = ln.load_env()
            run_url = ""
            if os.environ.get("GITHUB_RUN_ID"):
                run_url = f"{os.environ.get('GITHUB_SERVER_URL')}/{os.environ.get('GITHUB_REPOSITORY')}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
            text = rep.line_text(now, run_url)
            print("----- LINE本文 -----\n" + text)
            if env.get("LINE_CHANNEL_ACCESS_TOKEN") and env.get("LINE_USER_ID"):
                ok = ln.line_push(env["LINE_CHANNEL_ACCESS_TOKEN"], env["LINE_USER_ID"], text)
                print("LINE送信 " + ("成功" if ok else "失敗"))
            else:
                print("LINEの設定が無いので送らない")
    sys.exit(1 if ng else 0)


if __name__ == "__main__":
    main()
