from __future__ import annotations
import subprocess
from pathlib import Path
from typing import Optional


DEFAULT_PRINTER = "DocuWorks PDF"


def find_dwviewer() -> Optional[Path]:
    candidates = [
        Path(r"C:\\Program Files\\FUJIFILM Business Innovation\\DocuWorks\\dwviewer.exe"),
        Path(r"C:\\Program Files (x86)\\FUJIFILM Business Innovation\\DocuWorks\\dwviewer.exe"),
        Path(r"C:\\Program Files\\Fuji Xerox\\DocuWorks\\dwviewer.exe"),
        Path(r"C:\\Program Files (x86)\\Fuji Xerox\\DocuWorks\\dwviewer.exe"),
        # Some environments might only have DocuWorks Desk
        Path(r"C:\\Program Files\\FUJIFILM Business Innovation\\DocuWorks\\dwdesk.exe"),
        Path(r"C:\\Program Files (x86)\\FUJIFILM Business Innovation\\DocuWorks\\dwdesk.exe"),
        Path(r"C:\\Program Files\\Fuji Xerox\\DocuWorks\\dwdesk.exe"),
        Path(r"C:\\Program Files (x86)\\Fuji Xerox\\DocuWorks\\dwdesk.exe"),
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


def print_to_printer(
    xdw: Path, printer: str = DEFAULT_PRINTER, viewer: Optional[Path] = None
) -> int:
    """Invoke DocuWorks Viewer to print to the specified printer.
    Requires the printer to be configured for auto-save to avoid UI prompts.
    Returns process return code.
    """
    viewer = Path(viewer) if viewer else find_dwviewer()
    if not viewer:
        raise FileNotFoundError("DocuWorks Viewer not found (dwviewer.exe)")
    cmd = [str(viewer), "/pt", printer, str(xdw)]
    proc = subprocess.run(cmd, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    return proc.returncode


def batch_convert(
    root: Path, recurse: bool = True, printer: str = DEFAULT_PRINTER, viewer: Optional[str] = None
) -> list[tuple[Path, int]]:
    pattern = "**/*.xdw" if recurse else "*.xdw"
    files = sorted(root.glob(pattern))
    files += sorted(root.glob("**/*.xbd" if recurse else "*.xbd"))
    results: list[tuple[Path, int]] = []
    for f in files:
        rc = print_to_printer(f, printer=printer, viewer=Path(viewer) if viewer else None)
        results.append((f, rc))
    return results
