"""各期間の取得結果（parts/<期間>/*.csv）を1つにまとめて data_2022/ に書き出す。"""
import csv, glob, os, sys

src, dst = sys.argv[1], sys.argv[2]
os.makedirs(dst, exist_ok=True)
names = sorted({os.path.basename(p) for p in glob.glob(os.path.join(src, "*", "*.csv"))})
for name in names:
    header, rows = None, []
    for p in sorted(glob.glob(os.path.join(src, "*", name))):
        with open(p, encoding="utf-8", newline="") as f:
            r = list(csv.reader(f))
        if not r:
            continue
        header = header or r[0]
        rows += [x for x in r[1:] if x]
    seen, out = set(), []
    for x in rows:
        k = tuple(x)
        if k not in seen:
            seen.add(k); out.append(x)
    out.sort(key=lambda x: x[:3])
    with open(os.path.join(dst, name), "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f); w.writerow(header); w.writerows(out)
    print(name, len(out))
