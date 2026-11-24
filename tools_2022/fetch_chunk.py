"""2022年分（参考データ）を期間指定で取得する。既存の取得スクリプトをそのまま使い、作業フォルダに新規CSVを作る。
使い方: python fetch_chunk.py 2022-01-01 2022-02-28
（このファイルと omura_*.py を同じ空フォルダにコピーして実行する。既存の本番CSVには触れない）
"""
import sys
from datetime import date

start, end = (date.fromisoformat(s) for s in sys.argv[1:3])
import omura_history_fetch as hf
hf.START, hf.END = start, end
hf.MAX_WORKERS = 6          # サイトに負荷をかけすぎないよう本番より少なめ
hf.main()

import omura_extra_fetch as ex
ex.WORKERS = 3
ex.INNER_WORKERS = 3
ex.fetch_dates(ex.results_dates(), label=f"追加データ {start}〜{end}")
ex.build_daily_extra()
