from __future__ import annotations

import argparse
from pathlib import Path

from welding_registry.io_excel import read_sheet, to_canonical
from welding_registry.normalize import normalize


def main() -> int:
    ap = argparse.ArgumentParser(description="Convert one sheet of an XLS to Parquet")
    ap.add_argument("xls", help="Path to XLS file")
    ap.add_argument("--sheet", help="Sheet name or index (default: 0)")
    ap.add_argument("--header-row", type=int, help="Override header row index (0-based)")
    ap.add_argument("--out", required=True, help="Output .parquet path")
    args = ap.parse_args()

    xls = Path(args.xls)
    if not xls.exists():
        ap.error(f"not found: {xls}")

    sheet = args.sheet if args.sheet is not None else 0
    df_raw, _ = read_sheet(xls, sheet, header_row_override=args.header_row)
    df = normalize(to_canonical(df_raw))

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(out)
    except Exception as e:
        ap.error(f"to_parquet failed ({e}); install pyarrow or fastparquet")
    print(f"Wrote {out} ({len(df)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
