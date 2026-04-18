import requests
from bs4 import BeautifulSoup
import csv
import os
import subprocess
from datetime import datetime, timedelta

DESKTOP = os.path.join(os.path.expanduser("~"), "Desktop")
CSV_PATH = os.path.join(DESKTOP, "omura_results.csv")
JYOJO = "24"  # 大村競艇場コード

def get_yesterday():
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y-%m-%d")

def fetch_race_result(date_str, race_no):
    url = f"https://www.boatrace.jp/owpc/pc/race/raceresult?rno={race_no}&jcd={JYOJO}&hd={date_str}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.encoding = "utf-8"
        soup = BeautifulSoup(res.text, "html.parser")

        # 着順取得
        rows = soup.select("table.is-w495 tbody tr")
        ranking = {}
        for row in rows:
            cols = row.select("td")
            if len(cols) >= 2:
                rank = cols[0].get_text(strip=True)
                boat = cols[1].get_text(strip=True)
                if rank in ["1", "2", "3"]:
                    ranking[rank] = boat

        # 3連単払戻取得
        harai = ""
        harai_rows = soup.select("table.is-w495 ~ table tbody tr")
        for row in harai_rows:
            cols = row.select("td")
            if len(cols) >= 2 and "3連単" in cols[0].get_text():
                harai = cols[1].get_text(strip=True).replace(",", "").replace("円", "")
                break

        r1 = ranking.get("1", "")
        r2 = ranking.get("2", "")
        r3 = ranking.get("3", "")
        return r1, r2, r3, harai

    except Exception as e:
        print(f"R{race_no} 取得エラー: {e}")
        return "", "", "", ""

def already_exists(date_label):
    if not os.path.exists(CSV_PATH):
        return False
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        return date_label in f.read()

def append_csv(row):
    with open(CSV_PATH, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)

def git_push():
    try:
        subprocess.run(["git", "-C", DESKTOP, "add", "omura_results.csv"], check=True)
        subprocess.run(["git", "-C", DESKTOP, "commit", "-m", f"自動更新 {datetime.now().strftime('%Y-%m-%d')}"], check=True)
        subprocess.run(["git", "-C", DESKTOP, "push", "origin", "main"], check=True)
        print("GitHubへのpush完了")
    except subprocess.CalledProcessError as e:
        print(f"Git pushエラー: {e}")

def main():
    date_str, date_label = get_yesterday()
    print(f"取得日: {date_label}")

    if already_exists(date_label):
        print("すでに記録済みです")
        return

    r10 = fetch_race_result(date_str, 10)
    r11 = fetch_race_result(date_str, 11)
    r12 = fetch_race_result(date_str, 12)

    row = [date_label,
           r10[0], r10[1], r10[2], r10[3],
           r11[0], r11[1], r11[2], r11[3],
           r12[0], r12[1], r12[2], r12[3]]

    append_csv(row)
    print(f"CSV追記完了: {row}")
    git_push()

if __name__ == "__main__":
    main()
