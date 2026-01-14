import polars as pl
import pyodbc
from roskarl import env_var_dsn, DSN
from typing import Generator, Any


def read_pyodbc_mssql(
    query: str,
    env_var_name: str,
    driver: str = "{ODBC Driver 18 for SQL Server}",
    trust_server_certificate: str = "yes",
) -> Generator[pl.DataFrame, Any, Any]:
    dsn: DSN = env_var_dsn(name=env_var_name)

    conn = pyodbc.connect(
        f"DRIVER={driver};"
        f"SERVER={dsn.hostname},{dsn.port};"
        f"DATABASE={dsn.database};"
        f"UID={dsn.username};"
        f"PWD={dsn.password};"
        f"TrustServerCertificate={trust_server_certificate}"
    )

    try:
        if conn:
            df = pl.read_database(query, conn)
            if not df.is_empty():
                yield df
            else:
                yield from ()
    finally:
        conn.close()
