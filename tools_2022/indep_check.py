#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""大村12R「1番人気1点・定額」の独立検算スクリプト（標準ライブラリのみ）。

使い方:  python indep.py <CSVフォルダ> [1回の金額(円)。省略時 50000]

既存コードを見ずに、ルール文だけから元CSVを計算し直す。
"""
import csv
import os
import sys
from collections import defaultdict
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

SIGNS = ["雨", "曇り", "勝率差", "展示4位以下", "他艇チルト", "1号艇チルト", "展示進入が枠なりでない"]


def load(folder, name):
    with open(os.path.join(folder, name), encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def dec(s):
    """数値文字列→Decimal。空・数値でないものは None。"""
    s = (s or "").strip()
    if not s:
        return None
    try:
        d = Decimal(s)
    except InvalidOperation:
        return None
    return d if d.is_finite() else None


def to_int(s):
    d = dec(s)
    return int(d) if d is not None and d == d.to_integral_value() else None


def judge_signs(res, boats):
    """7サインの判定。boats = {枠: 行}（12Rのみ）。判定できない項目は False。"""
    s = dict.fromkeys(SIGNS, False)
    w = res.get("12R_天気") or ""
    s["雨"] = ("雨" in w) or ("雪" in w)
    s["曇り"] = "曇" in w

    b1 = boats.get(1)
    others = [b for k, b in boats.items() if k != 1]

    # 勝率差
    w1 = dec(b1["全国勝率"]) if b1 else None
    wo = [x for x in (dec(b["全国勝率"]) for b in others) if x is not None]
    if w1 is not None and wo:
        diff = (w1 - max(wo)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        s["勝率差"] = diff <= Decimal("-1.0")

    # 展示タイム順位
    t = {k: dec(b["展示タイム"]) for k, b in boats.items()}
    t = {k: v for k, v in t.items() if v is not None}
    if len(t) >= 5 and 1 in t:
        rank = 1 + sum(1 for k, v in t.items() if k != 1 and v < t[1])
        s["展示4位以下"] = rank >= 4

    # チルト
    c = {k: dec(b["チルト"]) for k, b in boats.items()}
    c = {k: v for k, v in c.items() if v is not None}
    half = Decimal("0.5")
    if len(c) >= 5:
        s["他艇チルト"] = any(v >= half for k, v in c.items() if k != 1)
    if 1 in c:
        s["1号艇チルト"] = c[1] >= half

    # 展示進入
    e = {k: to_int(b["展示進入"]) for k, b in boats.items()}
    e = {k: v for k, v in e.items() if v is not None}
    if len(e) >= 5:
        order = [k for k, _ in sorted(e.items(), key=lambda kv: (kv[1], kv[0]))]
        s["展示進入が枠なりでない"] = order != sorted(order)
    return s


def is_refund(res, boats, odds_row, odds_cols):
    """返還（数えない日）かどうか。"""
    if not (res.get("12R_人気") or "").strip():
        return True
    fl = {k for k, b in boats.items() if any(ch in (b.get("本番ST") or "").upper() for ch in "FL")}
    if not fl or odds_row is None:
        return False
    nums = [(c, dec(odds_row.get(c))) for c in odds_cols]
    nums = [(c, v) for c, v in nums if v is not None and v > 0]
    if not nums:
        return False
    lo = min(v for _, v in nums)
    ties = [c for c, v in nums if v == lo]
    result = "-".join((res.get("12R_%d着" % i) or "").strip() for i in (1, 2, 3))
    if result in ties and (res.get("12R_人気") or "").strip() == "1":
        pick = result
    else:
        pick = ties[0]
    return any(int(x) in fl for x in pick.split("-"))


def stats(days, stake):
    """days = [(日付, 的中bool, 100円あたり払戻)]（日付順）。"""
    n = len(days)
    hits = sum(1 for _, h, _ in days if h)
    ret = sum(Decimal(p) * stake / 100 for _, h, p in days if h)
    bet = Decimal(stake) * n
    cum = peak = dd = Decimal(0)   # ピークは開始時点の0円も含める
    run = longest = 0
    for _, h, p in days:
        cum += (Decimal(p) * stake / 100 if h else 0) - stake
        peak = max(peak, cum)
        dd = max(dd, peak - cum)
        run = 0 if h else run + 1
        longest = max(longest, run)
    return dict(n=n, hits=hits, bet=bet, ret=ret, pl=ret - bet, streak=longest, dd=dd,
                hit_rate=(100.0 * hits / n) if n else None,
                roi=(float(ret) * 100 / float(bet)) if n else None)


def pct(x):
    return "   --" if x is None else "%5.1f%%" % x


def yen(x):
    return "{:+,d}円".format(int(x))


def line(label, st):
    return "%s  %4d日  的中%3d回  的中率%s  回収率%s  収支%s  最長%2d連敗  最大落ち込み-%s円" % (
        label, st["n"], st["hits"], pct(st["hit_rate"]), pct(st["roi"]), yen(st["pl"]),
        st["streak"], "{:,d}".format(int(st["dd"])))


def short(label, st):
    return "%s  %4d日  的中%3d回  的中率%s  回収率%s  収支%s" % (
        label, st["n"], st["hits"], pct(st["hit_rate"]), pct(st["roi"]), yen(st["pl"]))


def main():
    if len(sys.argv) < 2:
        sys.exit("使い方: python indep.py <CSVフォルダ> [金額]")
    folder = sys.argv[1]
    stake = int(sys.argv[2]) if len(sys.argv) > 2 else 50000

    results = sorted(load(folder, "omura_results.csv"), key=lambda r: r["日付"])
    boats_by_day = defaultdict(dict)
    for r in load(folder, "omura_racers.csv"):
        if (r.get("R") or "").strip() == "12" and to_int(r.get("枠")) is not None:
            boats_by_day[r["日付"]][to_int(r["枠"])] = r
    shibu = {}
    for r in load(folder, "omura_racers_extra.csv"):
        if (r.get("R") or "").strip() == "12" and (r.get("枠") or "").strip() == "1":
            shibu[r["日付"]] = (r.get("支部") or "").strip()
    odds_rows = load(folder, "omura_12r_odds.csv")
    odds_cols = [c for c in (odds_rows[0].keys() if odds_rows else []) if c != "日付"]
    odds = {r["日付"]: r for r in odds_rows}

    cand = []      # ①②成立・返還でない日
    n_12 = n_ref = 0
    refunds = []
    for res in results:
        d = res["日付"]
        g = lambda k: (res.get(k) or "").strip()
        if g("11R_1着") != "1":
            continue
        r10 = [g("10R_%d着" % i) for i in (1, 2, 3)]
        r11 = [g("11R_%d着" % i) for i in (1, 2, 3)]
        if r10 == r11:
            continue
        n_12 += 1
        boats = boats_by_day.get(d, {})
        if is_refund(res, boats, odds.get(d), odds_cols):
            n_ref += 1
            refunds.append(d)
            continue
        hit = g("12R_人気") == "1"
        b1 = boats.get(1)
        cand.append(dict(date=d, hit=hit, pay=g("12R_払戻") if hit else "0",
                         signs=judge_signs(res, boats), shibu=shibu.get(d, ""),
                         kyu=(b1["級別"].strip() if b1 else "")))

    T = lambda rows: stats([(c["date"], c["hit"], c["pay"]) for c in rows], stake)
    buy = [c for c in cand if not any(c["signs"].values())]

    print("=== 大村12R 1番人気1点 定額%s円 独立検算 ===" % "{:,d}".format(stake))
    print("データ期間: %s 〜 %s（%d日）" % (results[0]["日付"], results[-1]["日付"], len(results)))
    print("①②成立: %d日 / うち返還・人気不明で数えない: %d日 %s" % (n_12, n_ref, refunds))
    print()
    print(line("①②だけ毎回", T(cand)))
    print(line("確定方針    ", T(buy)))
    print()
    print("--- 年別（確定方針） ---")
    for y in sorted({c["date"][:4] for c in cand}):
        print(line(y + "年      ", T([c for c in buy if c["date"][:4] == y])))
    print()
    print("--- 見送りサイン別（①②成立・返還除く %d日を、そのサインの有無だけで2分） ---" % len(cand))
    for s in SIGNS:
        print(short("%s\n   出た日に買っていたら" % s, T([c for c in cand if c["signs"][s]])))
        print(short("   出なかった日        ", T([c for c in cand if not c["signs"][s]])))
    print()
    print("--- 追い風（確定方針で買う日のうち） ---")
    print(short("1号艇が長崎支部    ", T([c for c in buy if c["shibu"] == "長崎"])))
    print(short("  それ以外         ", T([c for c in buy if c["shibu"] != "長崎"])))
    print(short("1号艇がB級         ", T([c for c in buy if c["kyu"].startswith("B")])))
    print(short("  それ以外         ", T([c for c in buy if not c["kyu"].startswith("B")])))


if __name__ == "__main__":
    main()
