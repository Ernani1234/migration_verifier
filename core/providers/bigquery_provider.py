from typing import List, Dict, Optional
import pandas as pd
from google.cloud import bigquery
from .base import DataProvider


class BigQueryProvider(DataProvider):
    def __init__(self, credentials: Dict):
        super().__init__(credentials)
        self.client: Optional[bigquery.Client] = None

    def connect(self) -> None:
        project_id = self.credentials.get("project_id")
        keyfile_path = self.credentials.get("keyfile_path")
        if keyfile_path:
            if project_id:
                self.client = bigquery.Client.from_service_account_json(keyfile_path, project=project_id)
            else:
                self.client = bigquery.Client.from_service_account_json(keyfile_path)
        else:
            self.client = bigquery.Client(project=project_id)
        # smoke test
        _ = list(self.client.list_datasets())
        self._connected = True

    def test_connection(self) -> bool:
        try:
            self.connect()
            self.client.query("SELECT 1").result()
            return True
        except Exception:
            return False

    def has_permissions(self, actions: List[str]) -> Dict[str, bool]:
        res = {}
        # read
        try:
            self.client.query("SELECT 1").result()
            res["read"] = True
        except Exception:
            res["read"] = False
        # write (try creating a temp table if dataset is provided)
        dataset = self.credentials.get("dataset")
        try:
            if dataset:
                table_id = f"{self.client.project}.{dataset}.__perm_test"
                schema = [bigquery.SchemaField("c", "STRING")]
                table = bigquery.Table(table_id, schema=schema)
                table = self.client.create_table(table, exists_ok=True)
                self.client.delete_table(table_id, not_found_ok=True)
                res["write"] = True
            else:
                res["write"] = True
        except Exception:
            res["write"] = False
        return {a: res.get(a, True) for a in actions}

    def list_datasets(self) -> List[str]:
        return [d.dataset_id for d in self.client.list_datasets()]

    def list_tables(self, dataset: Optional[str] = None) -> List[str]:
        ds = dataset or self.credentials.get("dataset")
        if not ds:
            return []
        return [t.table_id for t in self.client.list_tables(ds)]

    def _full_table_ref(self, table_name: str) -> str:
        # Accept 'dataset.table' or use provided dataset
        if "." in table_name:
            return f"{self.client.project}.{table_name}"
        dataset = self.credentials.get("dataset")
        return f"{self.client.project}.{dataset}.{table_name}"

    def read_table(self, table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        full = self._full_table_ref(table_name)
        q = f"SELECT * FROM `{full}`"
        if limit and limit > 0:
            q += f" LIMIT {int(limit)}"
        job = self.client.query(q)
        df = job.result().to_dataframe()
        return df

    def write_table(self, table_name: str, df: pd.DataFrame, if_exists: str = "append") -> int:
        full = self._full_table_ref(table_name)
        table = self.client.get_table(full) if if_exists != "replace" else None
        if if_exists == "replace":
            try:
                self.client.delete_table(full)
            except Exception:
                pass
        job = self.client.load_table_from_dataframe(df, full)
        job.result()
        return len(df)

    def delete_rows(self, table_name: str, where_clause: Optional[str] = None) -> int:
        full = self._full_table_ref(table_name)
        if where_clause and where_clause.strip():
            q = f"DELETE FROM `{full}` WHERE {where_clause}"
        else:
            q = f"DELETE FROM `{full}` WHERE TRUE"
        job = self.client.query(q)
        res = job.result()
        return getattr(res, "num_dml_affected_rows", 0) or 0

    def delete_table(self, table_name: str) -> None:
        full = self._full_table_ref(table_name)
        self.client.delete_table(full, not_found_ok=True)

    def close(self) -> None:
        self._connected = False