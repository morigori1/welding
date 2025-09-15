DocuWorks 自動変換（XDW/XBD → PDF）
===================================

前提: DocuWorks Viewer/Desk と仮想プリンタ「DocuWorks PDF」がインストール済み。
Viewer Light だけでは無人での自動保存が難しいため、DocuWorks PDF の自動保存を推奨。

推奨構成
- DocuWorks Viewer/Desk 9 以降（または 10）
- DocuWorks PDF（仮想プリンタ）
- プリンタ設定で「自動保存（ダイアログ無）」と保存先・ファイル名規則を設定
  - 保存先例: C:\welding\out\pdf
  - ファイル名: 元文書名 + .pdf

一括変換の手順（PowerShell）
1) スクリプトを実行: scripts\\convert_xdw_to_pdf.ps1 -InputDir data -Recurse
2) 事前確認のみ: scripts\\convert_xdw_to_pdf.ps1 -InputDir data -Recurse -DryRun
3) 変換は DocuWorks Viewer のコマンドライン /pt を利用し、指定プリンタに印刷します。

注意事項
- Microsoft Print to PDF は保存ダイアログが出るため無人運用に不向き。
- DocuWorks PDF の自動保存設定を有効にしてください。

変換結果の取り込み
- 例: python -m welding_registry due --sheet P1 --header-row 7 --licenses-scan out/pdf --window 120 --out out/due.csv --ics out/expiry.ics -- "data\\…\\.xls"

AutoHotkey 運用（Viewer Light）
- `scripts/xdw_to_pdf.ahk` … 単一ファイルを開いて印刷→保存を自動操作（既定プリンタに出力）
- `scripts/xdw_light_convert.ps1` … フォルダ一括変換。実行時に既定プリンタを `Microsoft Print to PDF` に切替→変換→元に戻す。
  - 例: `powershell -ExecutionPolicy Bypass -File scripts\xdw_light_convert.ps1 -InputDir data -Recurse`
  - 保存ダイアログには自動でフルパスを入力します。上書き確認が出た場合は自動で承諾します。

代替案（DocuWorks 非導入）
- サードパーティ変換ツールの利用（品質・ライセンス要確認）
