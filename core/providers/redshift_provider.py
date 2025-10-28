from typing import List, Dict, Optional
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from .base import DataProvider


class RedshiftProvider(DataProvider):
    def __init__(self, credentials: Dict):
        super().__init__(credentials)
        self.engine = None

    def connect(self) -> None:
        user = self.credentials.get("user")
        password = self.credentials.get("password")
        host = self.credentials.get("host")
        port = self.credentials.get("port", 5439)
        database = self.credentials.get("database")
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(url)
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        self._connected = True

    def test_connection(self) -> bool:
        try:
            self.connect()
            return True
        except Exception:
            return False

    def has_permissions(self, actions: List[str]) -> Dict[str, bool]:
        results = {}
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            results["read"] = True
        except Exception:
            results["read"] = False
        try:
            with self.engine.begin() as conn:
                conn.execute(text("SELECT 1"))
            results["write"] = True
        except Exception:
            results["write"] = False
        return {a: results.get(a, True) for a in actions}

    def list_datasets(self) -> List[str]:
        return [self.credentials.get("database", "dev")] 

    def list_tables(self, dataset: Optional[str] = None) -> List[str]:
        insp = inspect(self.engine)
        schema = self.credentials.get("schema", "public")
        return insp.get_table_names(schema=schema)

    def read_table(self, table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        schema = self.credentials.get("schema", "public")
        q = f'SELECT * FROM "{schema}"."{table_name}"'
        if limit and limit > 0:
            q += f" LIMIT {int(limit)}"
        with self.engine.connect() as conn:
            return pd.read_sql_query(q, conn)

    def write_table(self, table_name: str, df: pd.DataFrame, if_exists: str = "append") -> int:
        schema = self.credentials.get("schema", "public")
        df.to_sql(table_name, self.engine, if_exists=if_exists, index=False, schema=schema)
        return len(df)

    def delete_rows(self, table_name: str, where_clause: Optional[str] = None) -> int:
        schema = self.credentials.get("schema", "public")
        if where_clause and where_clause.strip():
            q = text(f'DELETE FROM "{schema}"."{table_name}" WHERE {where_clause}')
        else:
            q = text(f'DELETE FROM "{schema}"."{table_name}"')
        with self.engine.begin() as conn:
            res = conn.execute(q)
            return res.rowcount if res.rowcount is not None else 0

    def delete_table(self, table_name: str) -> None:
        schema = self.credentials.get("schema", "public")
        with self.engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"'))

    def close(self) -> None:
        if self.engine:
            self.engine.dispose()
        self._connected = False