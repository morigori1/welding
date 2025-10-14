Web App
=======

Run
---

```
python -m welding_registry app --duckdb out/registry.duckdb --review-db warehouse/review.sqlite
```

Routes
------

- `/` 人別一覧（`roster` の `name` を一覧表示、検索/在籍者のみフィルタ）
- `/report` 期限レポート（列: 氏名・生年・資格・登録番号・有効期限・残日数・通知）
- `/report/print` 印刷ビュー（A4、固定幅、`?rows=35&ori=portrait` などで調整）
- `/person?name=...` 個別確認画面（当人の資格履歴を表示）
- `/input` 手入力画面（`roster_manual` に追記、`person` 画面で統合表示）

Data Sources
------------

- 優先: DuckDBの `due` テーブルがあればそれを使用（列不足時は `compute_due` で補完）。
- 代替: `roster` から90日窓で期限を即時計算。
- `roster_enriched` があれば `birth_year_west` を自動でマージし、レポート/印刷に生年を表示。

Tips
----

- `workers` テーブルがあれば `/report` を「在籍者のみ」に絞り込めます。
- 画面上で「最新/要更新＋メモ」を記録すると、`review.sqlite` の `decisions` に保存されます。

Legacy Local App
----------------

旧来の Tk ベース「ローカル版」は 2025-10 時点で提供を終了しました。ローカル閲覧が必要な場合も `python -m welding_registry web` で Web 版を起動し、ブラウザ経由で利用してください。
