---
name: system-check
description: 大村システム（Webアプリの見送りサイン・定額シミュレーター・LINE通知・元データ）が正しいかを、元データから計算し直した正解と突き合わせて日本語で報告する。「システム確認して」「答え合わせして」「数字合ってる？」と言われたとき、またアプリ・LINE通知・データ取得を変更したあとに使う。
---

# 大村システムの答え合わせ

`omura_verify.py`（このリポジトリの main ブランチ）を動かして、結果を日本語で報告する。
正解はアプリ・LINEのコードを使わず元データ（master ブランチのCSV）から一から計算し、
アプリ（omura-webapp の lib/skipRules.ts・lib/flatBetLog.ts・lib/flatBetStats.ts）と
LINE通知（omura_line_notify.judge）は実物をそのまま動かして突き合わせる。

## 手順

1. 2つのリポジトリを最新にする（クラウドのスレッドなら /home/user に両方ある）。
   ```bash
   git -C /home/user/omura-boatrace fetch origin main master notify-state
   git -C /home/user/omura-webapp fetch origin master
   ```
   omura-boatrace に omura_verify.py が無いブランチにいるなら `git -C /home/user/omura-boatrace switch main && git -C /home/user/omura-boatrace pull`。
   アプリやLINE通知を作業ブランチで変えたなら、そのブランチのまま照合してよい（変更が正しいかの確認になる）。
2. 依存を入れる（初回だけ）: `pip install --quiet beautifulsoup4`。Node 22.18 以降が要る（`node --version`）。
3. 実行する。
   ```bash
   cd /home/user/omura-boatrace
   python omura_verify.py --data-git origin/master --webapp /home/user/omura-webapp --report /tmp/omura_verify.md
   ```
   - 本番アプリと boatrace.jp も見るときは `--live` を足す（ネットが要る。クラウドのスレッドでは boatrace.jp・vercel.app に出られないことがあるので、そのときは下の GitHub Actions を使う）。
   - 終了コード 1 = ずれ（NG）あり。
4. GitHub 上で本番も含めて走らせたいとき: Actions の「システムの答え合わせ」を Run workflow
   （MCP なら `actions_run_trigger`、workflow `system_check.yml`、ref `main`）。結果は実行ページの Summary に出る。

## 報告のしかた（長岡さん向け）

- 結論から: 「全部一致」か「ずれ N 件」。NG があれば、どの画面・どの日・何が違うかを1件ずつ短く。
- 成績（日数・的中率・回収率・最長連敗・最大落ち込み）は報告の「今の買い方の成績」の値をそのまま使う。
- 専門用語（ハーネス・スキル・TS など）は使わない。直す必要があるなら、何を直すかを先に言う。
- 「注意」はずれではない（例: アプリの収支表がデータに追いついていない、天気の見方の違い）。必要なら一言だけ添える。

## 正解の決まり（変えるときは omura_verify.py も直す）

- ①11R 1着=1号艇 ②10Rと11Rの3連単が完全一致しない
- 見送り7サイン: 雨（雪も含む）・曇り・1号艇の勝率差≤−1.0・展示タイム出走艇中4位以下・他艇チルト0.5以上・1号艇チルト0.5以上・展示進入が枠番順でない
- 12R 3連単1番人気1点。的中＝公式人気1、払戻＝実払戻。買い目の艇がF/Lなら返還（収支0・回数に数えない）
