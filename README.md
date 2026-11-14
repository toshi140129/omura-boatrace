# omura-boatrace

大村ボートレースのデータ蓄積と自動通知。

- データ本体（CSV・取得スクリプト）は `master` ブランチにある。
- このブランチ（`main`）には、GitHub Actions の定期実行に使うものだけを置いている。
  GitHub Actions のスケジュール実行はデフォルトブランチ（main）の workflow だけが動くため。

## 12R LINE通知（omura_line_notify.py）

毎日 19:00〜23:45（日本時間）に15分おきに起動し、11Rの結果が確定したら
「11Rの1着＝1号艇」の日だけ 12R の買い目・見送り判定をLINEにプッシュする。
設定手順と仕組みは `.github/workflows/line_notify.yml` と `omura_line_notify.py` 冒頭のコメントを参照。

必要な GitHub Secrets（Settings → Secrets and variables → Actions）
- `LINE_CHANNEL_ACCESS_TOKEN` … LINE Messaging API のチャネルアクセストークン（長期）
- `LINE_USER_ID` … 送信先の LINE ユーザーID

手動テスト: Actions タブ →「12R LINE通知」→ Run workflow（date に過去の開催日、dry_run にチェックでLINEに送らずログ確認）

## システムの答え合わせ（omura_verify.py）

アプリ・LINE通知・データが正しいかを、元データ（master のCSV）から別に計算し直した正解と突き合わせる。
毎朝 9:23 ごろ（日本時間）に GitHub Actions「システムの答え合わせ」（`.github/workflows/system_check.yml`）が自動で動き、
ずれ（NG）があれば失敗にして LINE で知らせる。月曜は「問題なし」も1通送る。omura-webapp と このリポジトリの PR でも同じ照合が自動で走る。

照合するもの: 見送りサイン（アプリ・LINE とも実物のコードを過去の全①成立日で動かす）、定額シミュレーターの日別ログと成績、
サインの説明文の数字、LINEの送信記録（送り忘れ・二重送信・記録の壊れ）、本番アプリと boatrace.jp からの読み取り、前日分のデータ取得。

手で動かすとき（Node 22.18 以降・`pip install beautifulsoup4`）:
```bash
python omura_verify.py --data-git origin/master --webapp ../omura-webapp          # 手元の計算だけ
python omura_verify.py --data-git origin/master --webapp ../omura-webapp --live   # 本番アプリ・boatrace.jp も
```
GitHub 上では Actions →「システムの答え合わせ」→ Run workflow（send_line にチェックで結果を LINE にも送る）。
