"""
omura_realtime.py — 大村12R リアルタイム期待値計算 + LINE通知

仕様
- 5分おきに起動されることを想定（タスクスケジューラ）
- 当日11Rの結果が確定したら、12Rの直前情報(風速・波高・風向)を取得
- 過去 omura_results.csv の類似条件と照合し、12R 3連単パターン別の的中率・
  平均払戻・期待値(EV per 100yen)を集計
- EV >= 100% の買い目があれば1日1回LINE通知（state file で重複抑止）
- 結果は realtime_ev.json に書き出し、変更があれば GitHub にpush
  → webapp(Next.js)が raw.githubusercontent から取得して表示

階層フィルタ（greedy）
- Tier1: 11R_1着 / 開催種別
- Tier2: 風速バケット / 波高バケット
- Tier3: 節日数 / 風向

各 tier はサンプル >= MIN_SAMPLES のとき採用、足りないとスキップ
"""
import csv
import json
import os
import re
import subprocess
import sys
import time
import unicodedata
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime

from bs4 import BeautifulSoup

HERE = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(HERE, "omura_results.csv")
EV_JSON = os.path.join(HERE, "realtime_ev.json")
STATE_PATH = os.path.join(HERE, "realtime_state.json")
ENV_PATH = os.path.join(HERE, ".env")
LOG_PATH = os.path.join(HERE, "realtime.log")

JYOJO = "24"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
MIN_SAMPLES = 30
MIN_PATTERN_HITS = 2
EV_THRESHOLD = 100  # 期待値プラスのライン (回収率%)


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def load_env():
    env = {}
    if os.path.exists(ENV_PATH):
        with open(ENV_PATH, encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith("#") and "=" in s:
                    k, v = s.split("=", 1)
                    env[k.strip()] = v.strip()
    return env


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
            time.sleep(1)
    return ""


def parse_race_result(html):
    if not html or "データがありません" in html:
        return ("", "", "", "", "")
    try:
        soup = BeautifulSoup(html, "html.parser")
        tables = soup.select("table.is-w495")
        p1 = p2 = p3 = pay = rank = ""
        if tables:
            for tr in tables[0].select("tbody tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    rnk = unicodedata.normalize("NFKC", tds[0].get_text(strip=True))
                    boat = tds[1].get_text(strip=True)
                    if rnk == "1":
                        p1 = boat
                    elif rnk == "2":
                        p2 = boat
                    elif rnk == "3":
                        p3 = boat
        if len(tables) >= 3:
            for tr in tables[2].select("tbody tr"):
                tds = tr.find_all("td")
                if len(tds) >= 3 and "3連単" in tds[0].get_text():
                    pay = (
                        tds[2].get_text(strip=True)
                        .replace("¥", "").replace(",", "").replace("円", "").strip()
                    )
                    if len(tds) >= 4:
                        rank = unicodedata.normalize("NFKC", tds[3].get_text(strip=True))
                    break
        return (p1, p2, p3, pay, rank)
    except Exception as e:
        log(f"parse race result error: {e}")
        return ("", "", "", "", "")


def parse_weather(html):
    if not html or "データがありません" in html:
        return ("", "", "")
    try:
        soup = BeautifulSoup(html, "html.parser")
        w = soup.select_one(".weather1")
        if not w:
            return ("", "", "")
        wind_speed = wind_dir = wave = ""
        node = w.select_one(".is-wind .weather1_bodyUnitLabelData")
        if node:
            m = re.search(r"(\d+)", unicodedata.normalize("NFKC", node.get_text(strip=True)))
            if m:
                wind_speed = m.group(1)
        node = w.select_one(".is-wave .weather1_bodyUnitLabelData")
        if node:
            m = re.search(r"(\d+)", unicodedata.normalize("NFKC", node.get_text(strip=True)))
            if m:
                wave = m.group(1)
        node = w.select_one(".is-windDirection .weather1_bodyUnitImage")
        if node:
            for cls in node.get("class", []):
                m = re.match(r"is-wind(\d+)", cls)
                if m:
                    wind_dir = m.group(1)
                    break
        return (wind_speed, wind_dir, wave)
    except Exception as e:
        log(f"weather parse error: {e}")
        return ("", "", "")


def parse_raceindex(html):
    if not html or "データがありません" in html:
        return ("", "")
    try:
        soup = BeautifulSoup(html, "html.parser")
        date_tabs = [li for li in soup.find_all("li") if li.find(class_="tab2_inner")]
        day_num = ""
        for i, li in enumerate(date_tabs):
            if "is-active1" in (li.get("class") or []):
                day_num = i + 1
                break
        times = re.findall(r"\b(\d{1,2}):(\d{2})\b", html)
        r1_hour = None
        for h, _ in times:
            hi = int(h)
            if 8 <= hi <= 23:
                r1_hour = hi
                break
        kind = ""
        if r1_hour is not None:
            if r1_hour < 12:
                kind = "デイ"
            elif r1_hour < 17:
                kind = "ナイター"
            else:
                kind = "ミッドナイト"
        return (str(day_num) if day_num else "", kind)
    except Exception as e:
        log(f"raceindex parse error: {e}")
        return ("", "")


def bucket_wind(s):
    if not s:
        return None
    try:
        n = int(s)
    except ValueError:
        return None
    if n <= 2:
        return "弱"
    if n <= 5:
        return "中"
    return "強"


def bucket_wave(s):
    if not s:
        return None
    try:
        n = int(s)
    except ValueError:
        return None
    if n <= 2:
        return "凪"
    if n <= 9:
        return "中"
    return "高"


def load_history():
    if not os.path.exists(CSV_PATH):
        return []
    rows = []
    with open(CSV_PATH, encoding="utf-8") as f:
        r = csv.reader(f)
        next(r, None)  # header
        for cols in r:
            if not cols or not cols[0].strip():
                continue
            if len(cols) < 27:
                cols = list(cols) + [""] * (27 - len(cols))
            try:
                pay = int(cols[12]) if cols[12].isdigit() else 0
            except ValueError:
                pay = 0
            rows.append({
                "date": cols[0],
                "r11_p1": cols[5], "r11_p2": cols[6], "r11_p3": cols[7],
                "r12_p1": cols[9], "r12_p2": cols[10], "r12_p3": cols[11],
                "r12_pay": pay,
                "r12_wind": cols[22], "r12_wdir": cols[23], "r12_wave": cols[24],
                "wind_b": bucket_wind(cols[22]),
                "wave_b": bucket_wave(cols[24]),
                "series_day": cols[25],
                "event_type": cols[26],
            })
    return rows


def filter_rows(rows, key, val):
    if val in (None, ""):
        return None
    return [r for r in rows if r.get(key) == val]


def match_history(rows, conds):
    cur = list(rows)
    applied = []
    tiers = [
        ["r11_p1", "event_type"],   # Tier 1
        ["wind_b", "wave_b"],         # Tier 2
        ["series_day", "r12_wdir"],   # Tier 3
    ]
    for tier in tiers:
        for f in tier:
            v = conds.get(f)
            filt = filter_rows(cur, f, v)
            if filt is not None and len(filt) >= MIN_SAMPLES:
                cur = filt
                applied.append((f, v))
    return cur, applied


def compute_ev(matched):
    pat_count = Counter()
    pat_pay_sum = defaultdict(int)
    valid = 0
    for r in matched:
        p1, p2, p3 = r["r12_p1"], r["r12_p2"], r["r12_p3"]
        if not (p1 and p2 and p3):
            continue
        valid += 1
        pat = f"{p1}-{p2}-{p3}"
        pat_count[pat] += 1
        pat_pay_sum[pat] += r["r12_pay"]
    out = []
    for pat, cnt in pat_count.items():
        if cnt < MIN_PATTERN_HITS:
            continue
        avg_pay = pat_pay_sum[pat] / cnt
        prob = cnt / valid if valid else 0
        ev = prob * avg_pay
        out.append({
            "pattern": pat,
            "hit": cnt,
            "total": valid,
            "prob": round(prob, 4),
            "avg_pay": round(avg_pay),
            "ev": round(ev, 1),
        })
    out.sort(key=lambda x: -x["ev"])
    return out, valid


def line_push(token, user_id, text):
    body = json.dumps(
        {"to": user_id, "messages": [{"type": "text", "text": text}]},
        ensure_ascii=False,
    ).encode("utf-8")
    req = urllib.request.Request(
        "https://api.line.me/v2/bot/message/push",
        method="POST", data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as res:
            return res.status == 200
    except Exception as e:
        log(f"LINE error: {e}")
        return False


def build_line_message(payload, ev_plus):
    lines = [
        "【大村12R リアルタイム期待値】",
        f"{payload['date']} {payload['event_type']} {payload['series_day']}日目",
        "",
        f"■11R結果: {payload['r11']['p1']}-{payload['r11']['p2']}-{payload['r11']['p3']}"
        f" ({payload['r11']['pay']}円)",
        f"■12R気象: 風{payload['r12_weather']['wind']}m / 波{payload['r12_weather']['wave']}cm"
        f" / 風向{payload['r12_weather']['wdir']}",
        f"■類似サンプル: {payload['matched_samples']}日",
    ]
    if payload["applied_filters"]:
        cond_str = " / ".join([f"{k}={v}" for k, v in payload["applied_filters"]])
        lines.append(f"■適用条件: {cond_str}")
    lines += ["", f"▼ 期待値プラス買い目 TOP{len(ev_plus)}"]
    for p in ev_plus:
        lines.append(
            f"  {p['pattern']}: 回収率{int(p['ev'])}% "
            f"(的中{p['prob']*100:.1f}% × 平均{int(p['avg_pay'])}円)"
        )
    lines += ["", "※1点100円換算 EV>=100が候補"]
    return "\n".join(lines)


def git_push_if_changed():
    try:
        subprocess.run(
            ["git", "-C", HERE, "add", "realtime_ev.json"],
            check=True, capture_output=True,
        )
        diff = subprocess.run(
            ["git", "-C", HERE, "diff", "--cached", "--quiet"],
            capture_output=True,
        )
        if diff.returncode != 0:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M")
            subprocess.run(
                ["git", "-C", HERE, "commit", "-m", f"chore(realtime): EV update {ts}"],
                check=True, capture_output=True,
            )
            subprocess.run(
                ["git", "-C", HERE, "push", "origin", "master"],
                check=True, capture_output=True,
            )
            log("git push complete")
        else:
            log("JSON変更なし: pushスキップ")
    except subprocess.CalledProcessError as e:
        err = e.stderr.decode("utf-8", "ignore") if e.stderr else str(e)
        log(f"git error: {err}")


def main():
    today = datetime.now().strftime("%Y%m%d")
    today_label = datetime.now().strftime("%Y-%m-%d")

    r11_url = (
        f"https://www.boatrace.jp/owpc/pc/race/raceresult?"
        f"rno=11&jcd={JYOJO}&hd={today}"
    )
    r11 = parse_race_result(fetch_html(r11_url))
    if not r11[0]:
        log(f"11R結果未確定 {today_label}: 終了")
        return
    log(f"11R結果検出: {r11[0]}-{r11[1]}-{r11[2]} 払戻={r11[3]}")

    r12_url = (
        f"https://www.boatrace.jp/owpc/pc/race/beforeinfo?"
        f"rno=12&jcd={JYOJO}&hd={today}"
    )
    weather = parse_weather(fetch_html(r12_url))
    log(f"12R気象: 風{weather[0]}m 風向{weather[1]} 波{weather[2]}cm")

    idx_url = (
        f"https://www.boatrace.jp/owpc/pc/race/raceindex?"
        f"jcd={JYOJO}&hd={today}"
    )
    series_day, event_type = parse_raceindex(fetch_html(idx_url))
    log(f"節日数={series_day} 開催種別={event_type}")

    history = load_history()
    log(f"ヒストリ {len(history)}日読み込み")

    conds = {
        "r11_p1": r11[0],
        "wind_b": bucket_wind(weather[0]),
        "wave_b": bucket_wave(weather[2]),
        "r12_wdir": weather[1] or None,
        "series_day": series_day or None,
        "event_type": event_type or None,
    }
    matched, applied = match_history(history, conds)
    log(f"類似サンプル {len(matched)}日 適用条件: {applied}")

    ev_list, valid = compute_ev(matched)

    payload = {
        "date": today_label,
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
        "r11": {"p1": r11[0], "p2": r11[1], "p3": r11[2], "pay": r11[3]},
        "r12_weather": {"wind": weather[0], "wdir": weather[1], "wave": weather[2]},
        "series_day": series_day,
        "event_type": event_type,
        "matched_samples": valid,
        "applied_filters": [list(t) for t in applied],
        "patterns": ev_list[:15],
    }

    new_text = json.dumps(payload, ensure_ascii=False, indent=2)
    old_text = ""
    if os.path.exists(EV_JSON):
        try:
            old_text = open(EV_JSON, encoding="utf-8").read()
        except Exception:
            pass

    if old_text != new_text:
        with open(EV_JSON, "w", encoding="utf-8") as f:
            f.write(new_text)
        log(f"realtime_ev.json更新 (パターン{len(ev_list)})")

    state = {}
    if os.path.exists(STATE_PATH):
        try:
            state = json.load(open(STATE_PATH, encoding="utf-8"))
        except Exception:
            state = {}

    ev_plus = [p for p in ev_list if p["ev"] >= EV_THRESHOLD]
    if ev_plus and state.get("last_notified_date") != today_label:
        env = load_env()
        token = env.get("LINE_CHANNEL_ACCESS_TOKEN")
        user_id = env.get("LINE_USER_ID")
        if token and user_id:
            text = build_line_message(payload, ev_plus[:5])
            if line_push(token, user_id, text):
                state["last_notified_date"] = today_label
                with open(STATE_PATH, "w", encoding="utf-8") as f:
                    json.dump(state, f, ensure_ascii=False)
                log(f"LINE通知送信完了 EV+件数={len(ev_plus)}")
        else:
            log("LINE_CHANNEL_ACCESS_TOKEN または LINE_USER_ID が .env に未設定")
    elif state.get("last_notified_date") == today_label:
        log("本日通知済み: スキップ")
    else:
        ev_max = ev_list[0]["ev"] if ev_list else 0
        log(f"EVプラス買い目なし (ev_max={ev_max:.0f})")

    git_push_if_changed()


if __name__ == "__main__":
    main()
