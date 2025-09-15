from __future__ import annotations

import argparse
from pathlib import Path
from collections import Counter

from welding_registry.io_excel import list_sheets, read_sheet


def main() -> int:
    ap = argparse.ArgumentParser(description="Scan Excel files for header tokens")
    ap.add_argument("root", help="Directory or file to scan")
    ap.add_argument("--limit", type=int, default=20, help="Max rows to probe per sheet")
    ap.add_argument("--out", help="Optional TSV to write (token\tcount)")
    args = ap.parse_args()

    root = Path(args.root)
    files = []
    if root.is_file():
        files = [root]
    else:
        files = list(root.rglob("*.xls")) + list(root.rglob("*.xlsx"))

    tokens = Counter()
    for f in files:
        try:
            for s in list_sheets(f):
                df, _ = read_sheet(f, s, header_row_override=None)
                probe = df.head(args.limit)
                for c in probe.columns:
                    tokens[str(c).strip()] += 1
        except Exception:
            continue

    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", encoding="utf-8") as w:
            for k, v in tokens.most_common():
                w.write(f"{k}\t{v}\n")
        print(f"Wrote {out} ({len(tokens)} unique tokens)")
    else:
        for k, v in tokens.most_common(50):
            print(f"{k}\t{v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
