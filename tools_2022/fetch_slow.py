"""古い年を「ゆっくり」取得する版（2026-11-24 に同時接続が多すぎて boatrace.jp から 403 拒否されたため）。
使い方: python fetch_slow.py 2016-10-01 2017-06-30
- 同時接続は1ジョブ最大6本。1ページごとに少し待つ。
- 403/429 が返ったら 2分→4分→8分 と待ってやり直す。連続して拒否され続けたら途中で打ち切る（取れた分は残す）。
- 取れなかったページの数を最後に出し、取りこぼしが「開催なし」に紛れないよう fetch_failures.txt に残す。
"""
import os, sys, threading, time, urllib.error, urllib.request
from datetime import date

start, end = (date.fromisoformat(s) for s in sys.argv[1:3])
import omura_history_fetch as hf
import omura_extra_fetch as ex

UA = hf.UA
lock = threading.Lock()
state = {"blocked_streak": 0, "abort": False, "fail": []}
MAX_BLOCK_STREAK = 25


def polite_fetch(url, retries=4):
    if state["abort"]:
        return ""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=40) as res:
                html = res.read().decode("utf-8", errors="replace")
            with lock:
                state["blocked_streak"] = 0
            time.sleep(0.5)
            return html
        except urllib.error.HTTPError as e:
            if e.code in (403, 429, 503):
                with lock:
                    state["blocked_streak"] += 1
                    streak = state["blocked_streak"]
                    if streak >= MAX_BLOCK_STREAK:
                        state["abort"] = True
                print(f"  拒否 {e.code}（連続{streak}）{url} → {120 * 2 ** attempt}s 待つ", file=sys.stderr, flush=True)
                if state["abort"]:
                    break
                time.sleep(120 * 2 ** attempt)
            else:
                time.sleep(3 * (attempt + 1))
        except Exception:
            time.sleep(3 * (attempt + 1))
    with lock:
        state["fail"].append(url)
    return ""


hf.fetch_html = polite_fetch
ex.fetch_html = polite_fetch
hf.START, hf.END = start, end
hf.MAX_WORKERS = 3
ex.WORKERS = 2
ex.INNER_WORKERS = 3
ex.SLEEP = 0.5

hf.main()
if not state["abort"]:
    ex.fetch_dates(ex.results_dates(), label=f"追加データ {start}〜{end}")
ex.build_daily_extra()
with open("fetch_failures.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(state["fail"]))
print(f"取れなかったページ: {len(state['fail'])}件  拒否で打ち切り: {state['abort']}", flush=True)
