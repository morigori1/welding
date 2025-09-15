from pathlib import Path
import types
import pandas as pd

import welding_registry.licenses as lic


class DummyPage:
    def extract_tables(self):
        return []

    def extract_text(self):
        return ""

    def to_image(self, resolution=200):
        class _Img:
            original = None

        return _Img()


class DummyPDF:
    pages = [DummyPage()]


class DummyCtx:
    def __init__(self, *args, **kwargs):
        self.obj = DummyPDF()

    def __enter__(self):
        return self.obj

    def __exit__(self, exc_type, exc, tb):
        return False


def test_scan_pdf_uses_ocr_fallback(monkeypatch):
    # Monkeypatch pdfplumber.open to our dummy context
    monkeypatch.setattr(lic, "pdfplumber", types.SimpleNamespace(open=lambda p: DummyCtx()))
    # Force OCR helper to return a predictable text
    monkeypatch.setattr(lic, "_ocr_pdf", lambda pdf: "登録番号: ZX-999\n有効期限: 2027/01/31\n")
    df = lic.scan_pdf(Path("dummy.pdf"))
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "license_no" in df.columns
    assert df.loc[0, "license_no"] == "ZX-999"
