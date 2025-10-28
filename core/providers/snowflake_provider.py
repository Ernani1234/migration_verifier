from typing import List, Dict, Optional
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from .base import DataProvider


class SnowflakeProvider(DataProvider):
    def __init__(self, credentials: Dict):
        super().__init__(credentials)
        self.conn: Optional[snowflake.connector.SnowflakeConnection] = None

    def connect(self) -> None:
        self.conn = snowflake.connector.connect(
            account=self.credentials.get("account"),
            user=self.credentials.get("user"),
            password=self.credentials.get("password"),
            warehouse=self.credentials.get("warehouse"),
            database=self.credentials.get("database"),
            schema=self.credentials.get("schema"),
        )
        cur = self.conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        self._connected = True

    def test_connection(self) -> bool:
        try:
            self.connect()
            return True
        except Exception:
            return False

    def has_permissions(self, actions: List[str]) -> Dict[str, bool]:
        res = {}
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            res["read"] = True
        except Exception:
            res["read"] = False
        try:
            cur = self.conn.cursor()
            cur.execute("CREATE TEMP TABLE PERM_TEST (c STRING)")
            cur.execute("DROP TABLE IF EXISTS PERM_TEST")
            cur.close()
            res["write"] = True
        except Exception:
            res["write"] = False
        return {a: res.get(a, True) for a in actions}

    def list_datasets(self) -> List[str]:
        # In Snowflake, use databases
        cur = self.conn.cursor()
        cur.execute("SHOW TABLES")
        cur.close()
        return [self.credentials.get("database", "")]  # placeholder

    def list_tables(self, dataset: Optional[str] = None) -> List[str]:
        cur = self.conn.cursor()
        cur.execute("SHOW TABLES")
        tables = [row[1] for row in cur.fetchall()]  # name is column 2
        cur.close()
        return tables

    def read_table(self, table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        cur = self.conn.cursor()
        q = f'SELECT * FROM "{table_name}"'
        if limit and limit > 0:
            q += f" LIMIT {int(limit)}"
        cur.execute(q)
        df = cur.fetch_pandas_all()
        cur.close()
        return df

    def write_table(self, table_name: str, df: pd.DataFrame, if_exists: str = "append") -> int:
        cur = self.conn.cursor()
        if if_exists == "replace":
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        # Ensure table exists; create based on dataframe columns as TEXT
        cols = ", ".join([f'"{c}" STRING' for c in df.columns])
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({cols})')
        cur.close()
        success, nchunks, nrows, _ = write_pandas(self.conn, df, table_name, quote_identifiers=True)
        return int(nrows)

    def delete_rows(self, table_name: str, where_clause: Optional[str] = None) -> int:
        cur = self.conn.cursor()
        if where_clause and where_clause.strip():
            q = f'DELETE FROM "{table_name}" WHERE {where_clause}'
        else:
            q = f'DELETE FROM "{table_name}"'
        res = cur.execute(q)
        cur.close()
        try:
            return int(res.rowcount)
        except Exception:
            return 0

    def delete_table(self, table_name: str) -> None:
        cur = self.conn.cursor()
        cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        cur.close()

    def close(self) -> None:
        try:
            if self.conn:
                self.conn.close()
        finally:
            self._connected = False