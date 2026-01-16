import json

import polars as pl


def escape_sql_string(s: str) -> str:
    return s.replace("'", "''").replace("\\", "\\\\")


def df_to_json_payload(df: pl.DataFrame) -> pl.DataFrame:
    payloads: list[str] = [
        escape_sql_string(s=json.dumps(obj=row, ensure_ascii=False, default=str))
        for row in df.to_dicts()
    ]
    
    return pl.DataFrame({"payload": payloads})