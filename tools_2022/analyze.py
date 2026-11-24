# 使い方: omura_verify.py（main ブランチ）と同じフォルダに置いて python analyze.py <CSVフォルダ> <見出し>
import sys, os
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import omura_verify as V

NAMES = {"rain":"雨","cloudy":"曇り","winrate_gap":"勝率差≤−1.0","exhibition_rank":"展示4位以下",
         "other_tilt":"他艇チルト0.5以上","boat1_tilt":"1号艇チルト0.5以上","start_exhibition":"展示進入が枠なりでない"}

def rows_of(days, pred):
    out = []
    for x in days:
        if not pred(x): continue
        if x["outcome"] == "hit": out.append((x["d"], x["pay"]))
        elif x["outcome"] == "miss": out.append((x["d"], 0))
    return out

def stat(rows, amount=50000):
    s = V.flat_stats(rows, amount); return s

def line(label, rows, amount=50000):
    return V.fmt_stats(label, stat(rows, amount))

def report(folder, title, years=None):
    data = V.Data(folder)
    days = build = V.build_days(data)
    if years:
        days = [x for x in days if x["d"][:4] in years]
    print(f"\n## {title}  （期間 {days[0]['d']}〜{days[-1]['d']}）")
    c1 = [x for x in days]
    c12 = [x for x in days if x["cond2"]]
    oc = defaultdict(int)
    for x in c12: oc[x["outcome"]] += 1
    print(f"①成立 {len(c1)}日 / ①②成立 {len(c12)}日  結果内訳 {dict(oc)}")
    unk = [x["d"] for x in c12 if any(v is None for v in x["signs"].values())]
    print(f"①②成立のうちサイン判定できない項目がある日: {len(unk)}日 {unk[:8]}")
    print(line("①②だけで毎回買う（5万定額）", rows_of(days, lambda x: x["cond2"])))
    print(line("確定方針（①②＋7サインなし）5万定額", rows_of(days, lambda x: x["go"])))
    print(line("確定方針 2万定額", rows_of(days, lambda x: x["go"]), 20000))
    for y in sorted({x["d"][:4] for x in days}):
        print(line(f"  {y}年 確定方針 5万", rows_of(days, lambda x, y=y: x["go"] and x["d"][:4]==y)))
    print("サイン別（①②成立日のうち、そのサインが出た日に買っていたら / 出なかった日）")
    for k, n in NAMES.items():
        on = rows_of(c12, lambda x, k=k: x["signs"].get(k) is True)
        off = rows_of(c12, lambda x, k=k: x["signs"].get(k) is False)
        def f(r):
            s = stat(r)
            return f"{s['days']}日 的中{s['hits']/s['days']*100:.0f}% 回収{s['returned']/s['invested']*100:.0f}%" if s["days"] else "0日"
        print(f"- {n}: 出た日 {f(on)} ／ 出なかった日 {f(off)}")
    # 追い風
    go = [x for x in days if x["go"]]
    def oikaze(x, kind):
        b1 = x["boats"].get("1", {})
        if kind == "nagasaki": return data.branch.get(x["d"], {}).get("1", "") == "長崎"
        return (b1.get("級別") or "").startswith("B")
    for kind, n in (("nagasaki","長崎支部1号艇"),("b","B級1号艇")):
        print(line(f"追い風 {n}（買う日のうち）", rows_of(go, lambda x, kind=kind: oikaze(x, kind))))
    return data, days

if __name__ == "__main__":
    report(sys.argv[1], sys.argv[2], sys.argv[3].split(",") if len(sys.argv) > 3 else None)
