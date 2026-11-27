"""年ごと一覧: 確定方針（5万定額）と12R 1号艇1着率など。python yearly.py <CSVフォルダ>"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import omura_verify as V
data = V.Data(sys.argv[1])
days = V.build_days(data)
def rows(pred):
    return [(x["d"], x["pay"] if x["outcome"] == "hit" else 0) for x in days if pred(x) and x["outcome"] in ("hit", "miss")]
def s(pred, amt=50000):
    st = V.flat_stats(rows(pred), amt)
    if not st["days"]: return "0日"
    return f"{st['days']}日 的中{st['hits']}（{st['hits']/st['days']*100:.1f}%） 回収{st['returned']/st['invested']*100:.1f}% 収支{V.man(st['total'])} 最長{st['maxStreak']}連敗 最大落込{V.man(st['maxDD'])}"
years = sorted({r["日付"][:4] for r in data.results})
print("| 年 | 開催日 | 12R 1号艇1着率 | 12R 1番人気的中率 | ①②成立 | 確定方針（5万定額） | ①②だけ毎回 回収率 |")
print("|---|---|---|---|---|---|---|")
for y in years:
    R = [r for r in data.results if r["日付"][:4] == y and r.get("12R_1着")]
    w1 = sum(r["12R_1着"] == "1" for r in R) / len(R) * 100
    p1 = sum(r["12R_人気"] == "1" for r in R) / len(R) * 100
    c12 = sum(1 for x in days if x["cond2"] and x["d"][:4] == y)
    st = V.flat_stats(rows(lambda x, y=y: x["cond2"] and x["d"][:4] == y), 50000)
    roi12 = st["returned"] / st["invested"] * 100 if st["days"] else 0
    print(f"| {y} | {len(R)} | {w1:.0f}% | {p1:.1f}% | {c12} | {s(lambda x, y=y: x['go'] and x['d'][:4] == y)} | {roi12:.0f}% |")
for lab, pred in (("ナイター前（〜2018-09-22）", lambda x: x["d"] < "2018-09-23"), ("ナイター後（2018-09-23〜）", lambda x: x["d"] >= "2018-09-23"),
                  ("リニューアル前（〜2015-03-27）", lambda x: x["d"] < "2015-03-28"), ("全期間", lambda x: True)):
    print(f"- {lab}: {s(lambda x, p=pred: x['go'] and p(x))}")
unk = sum(1 for x in days if x["cond2"] and any(v is None for v in x["signs"].values()))
print(f"- ①②成立のうちサインが判定できない項目がある日: {unk}日")
