# ローカルアプリ版（Tkinter）

サーバを立てずに、PC上で完結するローカルGUIです。既存のETL/計算ロジックをそのまま再利用しています。

## 起動

- 仮想環境でパッケージをインストール済みであること（`pip install -e .[dev]`）。
- コマンド:

```
python -m welding_registry gui --duckdb warehouse/local.duckdb
```

`--duckdb` を省略した場合、環境変数 `DUCKDB_DB_PATH` または `warehouse/local.duckdb` を使用します。

> 注意: 一部のLinuxでは Tkinter が同梱されていない場合があります。その場合は OS パッケージの `tk` を追加インストールしてください。

## 主な機能

- データベース: DuckDBファイルの選択/作成
- 取込:
  - 通常の横持ちExcelの取込（シートとヘッダー行の指定可→正規化してDuckDBに保存）
  - 縦レイアウト（A列=氏名）対応: 氏名列/番号列、JIS/ボイラのブロック範囲（例: `C:H`, `I:K`）を指定して取込
  - 取込結果は `out/roster.csv` / `out/roster.xlsx` にも保存
- 期限レポート:
  - `roster` テーブルから 90日ウィンドウで `due` を作成し DuckDB に保存
  - `roster_enriched` がある場合は `birth_year_west` を自動で左結合
  - 画面内プレビュー、CSV/ICS出力、印刷プレビュー（HTML生成）

## ワークフロー例

1. ホームタブで Excel を指定し「取込→DuckDB」
2. 同タブで「期限を計算」→ レポートタブに反映
3. レポートタブで「印刷プレビュー」を押すと `out/print_report.html` が開き、ブラウザから印刷可能

## 既存CLIとの関係

- Web版 (`python -m welding_registry app`) はそのまま利用可能です。
- ローカル版は Flask サーバを起動せず、Jinja2 テンプレートを直接レンダリングして印刷HTMLを生成します。
- ワーカー情報の付与（enrich）は従来通り CLI (`python -m welding_registry enrich --workers-csv ... --duckdb ...`) を先に実行してください。

