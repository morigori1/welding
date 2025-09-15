from __future__ import annotations

from pathlib import Path
from typing import Optional, Mapping, Any

import pandas as pd
from sqlalchemy import create_engine, text


def to_sqlite(df: pd.DataFrame, db_path: Path, table: str = "roster") -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        df.to_sql(table, conn, if_exists="replace", index=False)
        try:
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table}_name ON {table}(name)"))
        except Exception:
            pass


def to_duckdb(df: pd.DataFrame, db_path: Path, table: str = "roster") -> None:
    import duckdb  # type: ignore

    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.register("df", df)
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df WHERE 0=1")
        con.execute(f"DELETE FROM {table}")
        con.execute(f"INSERT INTO {table} SELECT * FROM df")
        con.unregister("df")
    finally:
        con.close()


def _build_mssql_odbc_connect(
    *,
    host: str,
    database: str,
    user: str,
    password: str,
    driver: str = "ODBC Driver 17 for SQL Server",
    encrypt: str | None = None,
    trust_server_certificate: str | None = None,
) -> str:
    # Build a standard ODBC connection string with proper semicolons.
    # Example: DRIVER={ODBC Driver 17 for SQL Server};SERVER=host\instance;DATABASE=db;UID=user;PWD=pass;Encrypt=yes;TrustServerCertificate=yes
    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={host}",
        f"DATABASE={database}",
        f"UID={user}",
        f"PWD={password}",
    ]
    if encrypt:
        parts.append(f"Encrypt={encrypt}")
    if trust_server_certificate:
        parts.append(f"TrustServerCertificate={trust_server_certificate}")
    return ";".join(parts)


def read_sqlserver_table(
    *,
    host: str,
    database: str,
    schema: str,
    table: str,
    user: str,
    password: str,
    driver: str = "ODBC Driver 17 for SQL Server",
    encrypt: str | None = None,
    trust_server_certificate: str | None = None,
    limit: Optional[int] = None,
    where: Optional[str] = None,
) -> pd.DataFrame:
    try:
        import urllib.parse  # stdlib
        import pyodbc  # type: ignore  # noqa: F401  (import to ensure driver availability)
    except Exception as e:  # pragma: no cover - optional dependency at runtime
        raise RuntimeError(
            "pyodbc is required for SQL Server access. Install 'pyodbc' and an appropriate ODBC driver."
        ) from e

    odbc_connect = _build_mssql_odbc_connect(
        host=host,
        database=database,
        user=user,
        password=password,
        driver=driver,
        encrypt=encrypt,
        trust_server_certificate=trust_server_certificate,
    )
    params = urllib.parse.quote_plus(odbc_connect)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    q = f"SELECT * FROM [{schema}].[{table}]"
    if where:
        q += f" WHERE {where}"
    if limit is not None:
        q = f"SELECT TOP({int(limit)}) * FROM ({q}) AS _t"
    with engine.begin() as conn:
        df = pd.read_sql(text(q), conn)
    return df


def read_sqlserver_query(
    *,
    host: str,
    database: str,
    user: str,
    password: str,
    driver: str = "ODBC Driver 17 for SQL Server",
    encrypt: str | None = None,
    trust_server_certificate: str | None = None,
    query: str,
    params: Optional[Mapping[str, Any]] = None,
) -> pd.DataFrame:
    try:
        import urllib.parse  # stdlib
        import pyodbc  # type: ignore  # noqa: F401
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "pyodbc is required for SQL Server access. Install 'pyodbc' and an appropriate ODBC driver."
        ) from e

    odbc_connect = _build_mssql_odbc_connect(
        host=host,
        database=database,
        user=user,
        password=password,
        driver=driver,
        encrypt=encrypt,
        trust_server_certificate=trust_server_certificate,
    )
    import urllib.parse

    params_enc = urllib.parse.quote_plus(odbc_connect)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params_enc}")
    with engine.begin() as conn:
        df = pd.read_sql(text(query), conn, params=params)
    return df
