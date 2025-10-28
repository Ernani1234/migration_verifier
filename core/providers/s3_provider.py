from typing import List, Dict, Optional
import pandas as pd
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from io import BytesIO
from .base import DataProvider


class S3Provider(DataProvider):
    def __init__(self, credentials: Dict):
        super().__init__(credentials)
        self.client = None

    def connect(self) -> None:
        access_key = self.credentials.get("aws_access_key_id")
        secret_key = self.credentials.get("aws_secret_access_key")
        region = self.credentials.get("region")
        session_token = self.credentials.get("aws_session_token")
        self.client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name=region,
            config=Config(signature_version="s3v4"),
        )
        # smoke test
        self.client.list_buckets()
        self._connected = True

    def test_connection(self) -> bool:
        try:
            self.connect()
            return True
        except Exception:
            return False

    def has_permissions(self, actions: List[str]) -> Dict[str, bool]:
        results = {}
        bucket = self.credentials.get("bucket")
        # read: list objects in bucket
        try:
            if bucket:
                self.client.list_objects_v2(Bucket=bucket, MaxKeys=1)
            else:
                self.client.list_buckets()
            results["read"] = True
        except Exception:
            results["read"] = False
        # write: put and delete a temp object if bucket provided
        try:
            if bucket:
                key = "__perm_test__.txt"
                self.client.put_object(Bucket=bucket, Key=key, Body=b"test")
                self.client.delete_object(Bucket=bucket, Key=key)
                results["write"] = True
            else:
                results["write"] = False
        except Exception:
            results["write"] = False
        return {a: results.get(a, True) for a in actions}

    def list_datasets(self) -> List[str]:
        resp = self.client.list_buckets()
        return [b["Name"] for b in resp.get("Buckets", [])]

    def list_tables(self, dataset: Optional[str] = None) -> List[str]:
        bucket = dataset or self.credentials.get("bucket")
        prefix = self.credentials.get("prefix", "")
        if not bucket:
            return []
        keys = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
            if len(keys) > 200:
                break
        return keys

    def _read_object_df(self, bucket: str, key: str) -> pd.DataFrame:
        obj = self.client.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        bio = BytesIO(body)
        if key.lower().endswith(".parquet"):
            return pd.read_parquet(bio)
        else:
            return pd.read_csv(bio)

    def read_table(self, table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        bucket = self.credentials.get("bucket")
        key = table_name or self.credentials.get("key")
        if not bucket or not key:
            raise ValueError("bucket e key são obrigatórios para leitura S3")
        df = self._read_object_df(bucket, key)
        if limit and limit > 0:
            return df.iloc[: int(limit)]
        return df

    def write_table(self, table_name: str, df: pd.DataFrame, if_exists: str = "append") -> int:
        bucket = self.credentials.get("bucket")
        key = table_name or self.credentials.get("key")
        if not bucket or not key:
            raise ValueError("bucket e key são obrigatórios para escrita S3")
        # For S3, we write full object; append will overwrite by concatenating
        existing = pd.DataFrame()
        if if_exists == "append":
            try:
                existing = self._read_object_df(bucket, key)
            except ClientError:
                existing = pd.DataFrame()
        out_df = pd.concat([existing, df], ignore_index=True) if not existing.empty else df
        bio = BytesIO()
        if key.lower().endswith(".parquet"):
            out_df.to_parquet(bio, index=False)
        else:
            out_df.to_csv(bio, index=False)
        bio.seek(0)
        self.client.put_object(Bucket=bucket, Key=key, Body=bio.read())
        return len(df)

    def delete_rows(self, table_name: str, where_clause: Optional[str] = None) -> int:
        # naive: load object, filter with pandas.query, rewrite
        bucket = self.credentials.get("bucket")
        key = table_name or self.credentials.get("key")
        if not bucket or not key:
            return 0
        df = self._read_object_df(bucket, key)
        if df.empty:
            return 0
        before = len(df)
        if where_clause and where_clause.strip():
            try:
                mask = df.query(where_clause).index
                df2 = df.drop(index=mask)
            except Exception:
                # if query fails, do not modify
                df2 = df
        else:
            df2 = pd.DataFrame(columns=df.columns)
        bio = BytesIO()
        if key.lower().endswith(".parquet"):
            df2.to_parquet(bio, index=False)
        else:
            df2.to_csv(bio, index=False)
        bio.seek(0)
        self.client.put_object(Bucket=bucket, Key=key, Body=bio.read())
        return before - len(df2)

    def delete_table(self, table_name: str) -> None:
        bucket = self.credentials.get("bucket")
        key = table_name or self.credentials.get("key")
        if not bucket or not key:
            return
        self.client.delete_object(Bucket=bucket, Key=key)

    def close(self) -> None:
        self._connected = False