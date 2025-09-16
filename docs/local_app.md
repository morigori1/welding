# ローカルアプリ (Tkinter)

PC 上で名簿の取り込み・フィルタ調整・発行履歴管理を完結できるデスクトップ GUI です。Excel → DuckDB の ETL や期限算出は CLI/Web 版と共通のロジックを利用します。

## 起動

```powershell
python -m welding_registry gui --duckdb warehouse/local.duckdb
```

`--duckdb` を省略した場合は環境変数 `DUCKDB_DB_PATH` または `warehouse/local.duckdb` を使用します。Tk ランタイムが無い Linux では `python3-tk` 等を追加インストールしてください。

## 画面構成

GUI は 3 つのタブに分かれています。

- **登録**: DuckDB ファイルの選択・Excel 取り込み・期限計算を実行します。従来のホーム画面をそのまま移設しており、垂直レイアウト取込や CSV/Excel 出力もここで行います。
- **全体リスト**: DuckDB 内の `roster` と `roster_manual` を統合した一覧を人単位で表示します。左ペインで掲載／非掲載をまとめて切り替え、右ペインで資格ごとの掲載フラグや手修正履歴を確認・追加できます。フィルタは `issue_person_filter` / `issue_license_filter` に保存され、発行タブや CLI 側にも反映されます。
- **発行**: フィルタ適用後の `due` テーブルを確認し、CSV/ICS/帳票出力を行います。`発行` ボタンで現在のリストをスナップショットとして `issue_runs` / `issue_run_items` に保存し、右側の履歴リストから過去回の確認・再出力が可能です。

## ワークフロー

1. **登録タブ**で Excel を選択し、`名簿をDuckDB` → `期限を計算` を実行します。
2. **全体リストタブ**で人／資格ごとの掲載可否を調整し、必要に応じて「手修正」から `roster_manual` に追記します。
3. **発行タブ**で `再読込` → `発行` を押して履歴に記録し、CSV/ICS/帳票を出力します。履歴を選択すると過去のスナップショットに切り替わり、現在のフィルタに依存せず再出力できます。

## CLI との連携

- Web 版 (`python -m welding_registry app`) と同じ DuckDB を共有できます。
- CLI の `ingest` / `due` コマンドで更新した場合も、GUI の `再読込` ボタンで最新状態に同期されます。

## ポータブル ZIP (Windows)

PyInstaller のビルドスクリプトで GUI/CLI を同梱した ZIP を作成できます。

1. PowerShell (x64) でリポジトリへ移動: `Set-Location C:\welding`
2. 仮想環境を作成して有効化: `py -3.11 -m venv .venv; .\.venv\Scripts\Activate.ps1`
3. 依存関係をインストール: `pip install -e .[dev]`
4. `powershell -ExecutionPolicy Bypass -File .\scripts\build_portable_zip.ps1`
5. `dist\welding-portable.zip` に `welding-gui.exe` / `welding-cli.exe` と `warehouse\local.duckdb` が含まれます。DuckDB を差し替えたい場合は `warehouse/local.duckdb` を更新してから再実行してください。

Tesseract OCR を利用する場合は適宜パスを環境変数 `TESSERACT_CMD` に設定してください。
