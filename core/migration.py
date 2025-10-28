from typing import Dict
import pandas as pd
from .providers.base import DataProvider


def migrate_table(source: DataProvider, dest: DataProvider, table_name: str, chunk_size: int = 5000) -> int:
    df = source.read_table(table_name)
    if df.empty:
        # ensure table exists on destination
        dest.write_table(table_name, df, if_exists="append")
        return 0
    total = 0
    if len(df) <= chunk_size:
        total += dest.write_table(table_name, df, if_exists="append")
    else:
        for i in range(0, len(df), chunk_size):
            total += dest.write_table(table_name, df.iloc[i : i + chunk_size], if_exists="append")
    return total


def check_table_quality(provider: DataProvider, table_name: str) -> Dict:
    df = provider.read_table(table_name)
    report = {
        "rows": len(df),
        "columns": list(df.columns),
        "duplicates": 0,
        "nulls_by_column": {},
        "truncated_like": 0,
    }
    if df.empty:
        return report
    dups = df.duplicated(keep="first").sum()
    report["duplicates"] = int(dups)
    nulls = {c: int(df[c].isna().sum()) for c in df.columns}
    report["nulls_by_column"] = nulls
    # naive heuristic: strings ending with '...' considered truncated-like
    truncated_like = 0
    for c in df.select_dtypes(include="object").columns:
        truncated_like += int(df[c].astype(str).str.endswith("...").sum())
    report["truncated_like"] = truncated_like
    return report


def apply_corrections(provider: DataProvider, table_name: str) -> int:
    df = provider.read_table(table_name)
    if df.empty:
        return 0
    before = len(df)
    df2 = df.drop_duplicates(keep="first").reset_index(drop=True)
    provider.delete_table(table_name)
    provider.write_table(table_name, df2, if_exists="replace")
    return before - len(df2)


def add_data_from_dataframe(provider: DataProvider, table_name: str, df: pd.DataFrame) -> int:
    return provider.write_table(table_name, df, if_exists="append")