import csv
import os
import re
import subprocess
import time
import unicodedata
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

DESKTOP = os.path.join(os.path.expanduser("~"), "Desktop")
CSV_PATH = os.path.join(DESKTOP, "omura_results.csv")
JYOJO = "24"  # 大村競艇場コード


def get_yesterday():
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y-%m-%d")


def create_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def fetch_race_result(driver, date_str, race_no):
    """Return (r1, r2, r3, pay, ninki)."""
    url = f"https://www.boatrace.jp/owpc/pc/race/raceresult?rno={race_no}&jcd={JYOJO}&hd={date_str}"
    try:
        driver.get(url)
        time.sleep(3)

        if "データがありません" in driver.page_source:
            return "", "", "", "", ""

        r1, r2, r3, harai, ninki = "", "", "", "", ""

        tables = driver.find_elements(By.CSS_SELECTOR, "table.is-w495")
        if tables:
            rows = tables[0].find_elements(By.CSS_SELECTOR, "tbody tr")
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 2:
                    rank = unicodedata.normalize("NFKC", cols[0].text.strip())
                    boat = cols[1].text.strip()
                    if rank == "1": r1 = boat
                    elif rank == "2": r2 = boat
                    elif rank == "3": r3 = boat

        if len(tables) >= 3:
            pay_rows = tables[2].find_elements(By.CSS_SELECTOR, "tbody tr")
            for row in pay_rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 3 and "3連単" in cols[0].text:
                    harai = cols[2].text.strip().replace("¥", "").replace(",", "").replace("円", "").strip()
                    if len(cols) >= 4:
                        ninki = unicodedata.normalize("NFKC", cols[3].text.strip())
                    break

        print(f"  R{race_no} result: {r1}-{r2}-{r3} 払戻={harai} 人気={ninki}")
        return r1, r2, r3, harai, ninki

    except Exception as e:
        print(f"  R{race_no} 取得エラー: {e}")
        return "", "", "", "", ""


def fetch_weather(driver, date_str, race_no):
    """Return (wind_speed, wind_dir, wave)."""
    url = f"https://www.boatrace.jp/owpc/pc/race/beforeinfo?rno={race_no}&jcd={JYOJO}&hd={date_str}"
    try:
        driver.get(url)
        time.sleep(2)

        if "データがありません" in driver.page_source:
            return "", "", ""

        wind_speed = wind_dir = wave = ""

        try:
            node = driver.find_element(By.CSS_SELECTOR, ".weather1 .is-wind .weather1_bodyUnitLabelData")
            m = re.search(r"(\d+)", unicodedata.normalize("NFKC", node.text.strip()))
            if m: wind_speed = m.group(1)
        except Exception:
            pass

        try:
            node = driver.find_element(By.CSS_SELECTOR, ".weather1 .is-wave .weather1_bodyUnitLabelData")
            m = re.search(r"(\d+)", unicodedata.normalize("NFKC", node.text.strip()))
            if m: wave = m.group(1)
        except Exception:
            pass

        try:
            node = driver.find_element(By.CSS_SELECTOR, ".weather1 .is-windDirection .weather1_bodyUnitImage")
            cls_attr = node.get_attribute("class") or ""
            m = re.search(r"is-wind(\d+)", cls_attr)
            if m: wind_dir = m.group(1)
        except Exception:
            pass

        print(f"  R{race_no} weather: wind={wind_speed}m dir={wind_dir} wave={wave}cm")
        return wind_speed, wind_dir, wave

    except Exception as e:
        print(f"  R{race_no} 気象取得エラー: {e}")
        return "", "", ""


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
        subprocess.run(["git", "-C", DESKTOP, "commit", "-m", f"auto update {datetime.now().strftime('%Y-%m-%d')}"], check=True)
        subprocess.run(["git", "-C", DESKTOP, "push", "origin", "master"], check=True)
        print("GitHubへのpush完了")
    except subprocess.CalledProcessError as e:
        print(f"Git pushエラー: {e}")


def main():
    date_str, date_label = get_yesterday()
    print(f"取得日: {date_label}")

    if already_exists(date_label):
        print("すでに記録済みです")
        return

    print("ブラウザ起動中...")
    driver = create_driver()

    try:
        r10 = fetch_race_result(driver, date_str, 10)
        r11 = fetch_race_result(driver, date_str, 11)
        r12 = fetch_race_result(driver, date_str, 12)
        w10 = fetch_weather(driver, date_str, 10)
        w11 = fetch_weather(driver, date_str, 11)
        w12 = fetch_weather(driver, date_str, 12)
    finally:
        driver.quit()

    if not any([r10[0], r11[0], r12[0]]):
        print("レース開催なし（スキップ）")
        return

    row = [date_label,
           r10[0], r10[1], r10[2], r10[3],
           r11[0], r11[1], r11[2], r11[3],
           r12[0], r12[1], r12[2], r12[3],
           r10[4], r11[4], r12[4],
           w10[0], w10[1], w10[2],
           w11[0], w11[1], w11[2],
           w12[0], w12[1], w12[2]]

    append_csv(row)
    print(f"CSV追記完了: {row}")
    git_push()


if __name__ == "__main__":
    main()
