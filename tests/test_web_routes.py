from welding_registry.app import create_app
import io
import pandas as pd


def test_routes_exist():
    app = create_app()
    app.testing = True
    c = app.test_client()
    assert c.get("/").status_code == 200
    assert c.get("/ver").status_code == 200
    assert c.get("/ver/xlsx").status_code == 200
    assert c.get("/ver/editor").status_code == 200
    # tolerant endpoints
    assert c.get("/person/TEST").status_code in (200, 302)
    assert c.get("/ver/asof/2025-09-12").status_code == 200


def test_csv_preview_accepts_excel(tmp_path):
    app = create_app()
    app.testing = True
    c = app.test_client()
    # create a tiny excel with Japanese headers
    df = pd.DataFrame(
        [{"氏名": "山田太郎", "登録番号": "AB-1", "資格": "SC-3F", "有効期限": "2028-09-01"}]
    )
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="P1")
    bio.seek(0)
    data = {
        "file": (bio, "dummy.xlsx"),
        "date": "2025-09-12",
    }
    rv = c.post("/ver/csv/preview", data=data, content_type="multipart/form-data")
    assert rv.status_code == 200
