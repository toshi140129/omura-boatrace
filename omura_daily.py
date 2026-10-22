"""
大村ボートレース 日次自動取得
CSV の最終日付の翌日〜昨日までを boatrace.jp から取得して omura_results.csv（31列）に追記し、GitHub(master) に自動 push する。
取得処理は omura_history_fetch.py をそのまま使う。
追加データ（出走表・直前情報・スタート情報・全レース結果・11R/12Rオッズ）は omura_extra_fetch.py で
同じ日付分を取得し、omura_racers.csv などに追記する。
"""
import os
import subprocess
from datetime import date, datetime, timedelta

import omura_history_fetch as hf
import omura_extra_fetch as extra

HERE = os.path.dirname(os.path.abspath(__file__))


def last_date():
    existing = hf.read_existing()
    if not existing:
        return None
    return datetime.strptime(max(existing.keys()), "%Y-%m-%d").date()


EXTRA_FILES = [
    "omura_racers.csv", "omura_races.csv", "omura_meta.csv",
    "omura_11r_odds.csv", "omura_12r_odds.csv", "omura_daily_extra.csv",
]


def git_push():
    try:
        subprocess.run(["git", "-C", HERE, "add", "omura_results.csv"] + EXTRA_FILES, check=True)
        diff = subprocess.run(["git", "-C", HERE, "diff", "--cached", "--quiet"])
        if diff.returncode == 0:
            print("変更なし（push不要）")
            return
        subprocess.run(["git", "-C", HERE, "commit", "-m",
                        f"auto update {datetime.now().strftime('%Y-%m-%d')}"], check=True)
        subprocess.run(["git", "-C", HERE, "pull", "--rebase", "--autostash", "origin", "master"], check=True)
        subprocess.run(["git", "-C", HERE, "push", "origin", "master"], check=True)
        print("GitHub push 完了")
    except subprocess.CalledProcessError as e:
        print(f"git push 失敗: {e}")


def main():
    yesterday = date.today() - timedelta(days=1)
    last = last_date()
    start = (last + timedelta(days=1)) if last else hf.START
    if start > yesterday:
        print(f"最新まで取得済み（最終日 {last}）")
        return
    print(f"取得範囲: {start} 〜 {yesterday}")
    hf.START = start
    hf.END = yesterday
    hf.main()
    # 追加データ: 結果CSVにある日付のうち未取得分（取得失敗しても結果CSVのpushは行う）
    try:
        extra.fetch_dates(extra.results_dates(), label="追加データ取得")
        extra.build_daily_extra()
    except Exception as e:
        print(f"追加データ取得で失敗（結果CSVのみ更新）: {e}")
    git_push()


if __name__ == "__main__":
    main()
