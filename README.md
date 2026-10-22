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
