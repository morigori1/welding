Windows Setup (venv)
====================

Quick start
-----------

```
cd C:\welding
py -3.13 -m venv .venv   # 3.13が無ければ 3.12
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
 .\.venv\Scripts\Activate.ps1
python -m pip install -U pip setuptools wheel
python -m pip install -e .
```

Troubleshooting
---------------

- ModuleNotFoundError: `flask` — 仮想環境が有効になっていません。` .\.venv\Scripts\Activate.ps1` を実行。
- WSLのvenvが混ざった — `.venv` を削除してWindows側で作り直し、`-e .` で再インストール。
- 実行ポリシーで止まる — 上記の `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` を先に実行。

Optional scripts
----------------

- `scripts\setup_windows_venv.ps1` — venv作成～依存導入～インポート検証までを自動化（起動手前まで）。
- `scripts\run_app.ps1` — venvを有効化してアプリを起動。

