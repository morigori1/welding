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

## ポータブルZIPパッケージ (Windows)

PyInstaller を使って Python 本体ごと持ち運べる ZIP を作成できます。

1. PowerShell (x64) でリポジトリ直下に移動します (`Set-Location C:\welding`)。
2. 仮想環境を作成・有効化します (`py -3.11 -m venv .venv; .\.venv\Scripts\Activate.ps1`)。
3. 依存をインストールします (`pip install -e .[dev]`)。
4. `powershell -ExecutionPolicy Bypass -File .\scripts\build_portable_zip.ps1` を実行します (既定で .venv の Python を使用)。
5. `dist\welding-portable.zip` が生成されます。展開すると `welding-gui\welding-gui.exe` (GUI) と `welding-cli\welding-cli.exe` (CLI) が利用できます。
6. `warehouse/local.duckdb` が同梱されるため、既存の DuckDB を差し替える場合はこのファイルを入れ替えてください。

※ Tesseract OCR 本体は同梱されません。必要に応じて別途インストールし、`TESSERACT_CMD` を設定してください。
