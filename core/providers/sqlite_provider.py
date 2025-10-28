from typing import List, Dict, Optional
from pathlib import Path
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from .base import DataProvider


class SQLiteProvider(DataProvider):
    def __init__(self, credentials: Dict):
        super().__init__(credentials)
        self.db_path = credentials.get("db_path")
        self.conn: Optional[sqlite3.Connection] = None
        self.engine = None

    def connect(self) -> None:
        if not self.db_path:
            raise ValueError("db_path é obrigatório para SQLite")
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.engine = create_engine(f"sqlite:///{self.db_path}")
        self._connected = True

    def test_connection(self) -> bool:
        try:
            self.connect()
            cur = self.conn.cursor()
            cur.execute("SELECT 1")
            _ = cur.fetchone()
            return True
        except Exception:
            return False

    def has_permissions(self, actions: List[str]) -> Dict[str, bool]:
        # For local SQLite we assume read/write permissions if the file is accessible
        perms = {}
        for act in actions:
            if act in ("read", "write"):
                try:
                    Path(self.db_path).touch(exist_ok=True)
                    perms[act] = True
                except Exception:
                    perms[act] = False
            else:
                perms[act] = True
        return perms

    def list_datasets(self) -> List[str]:
        # Dataset concept is not applicable; return ["main"]
        return ["main"]

    def list_tables(self, dataset: Optional[str] = None) -> List[str]:
        cur = self.conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        return [row[0] for row in cur.fetchall()]

    def read_table(self, table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        q = f"SELECT * FROM {table_name}"
        if limit and limit > 0:
            q += f" LIMIT {int(limit)}"
        return pd.read_sql_query(q, self.conn)

    def write_table(self, table_name: str, df: pd.DataFrame, if_exists: str = "append") -> int:
        df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
        return len(df)

    def delete_rows(self, table_name: str, where_clause: Optional[str] = None) -> int:
        if where_clause and where_clause.strip():
            q = text(f"DELETE FROM {table_name} WHERE {where_clause}")
        else:
            q = text(f"DELETE FROM {table_name}")
        with self.engine.begin() as conn:
            res = conn.execute(q)
            return res.rowcount if res.rowcount is not None else 0

    def delete_table(self, table_name: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

    def close(self) -> None:
        try:
            if self.conn:
                self.conn.close()
        finally:
            self._connected = False