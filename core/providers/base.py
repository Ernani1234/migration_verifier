from abc import ABC, abstractmethod
from typing import List, Dict, Optional
import pandas as pd


class DataProvider(ABC):
    def __init__(self, credentials: Dict):
        self.credentials = credentials
        self._connected = False

    @abstractmethod
    def connect(self) -> None:
        ...

    @abstractmethod
    def test_connection(self) -> bool:
        ...

    def has_permissions(self, actions: List[str]) -> Dict[str, bool]:
        # Base implementation assumes permissive; providers should override
        return {act: True for act in actions}

    @abstractmethod
    def list_datasets(self) -> List[str]:
        ...

    @abstractmethod
    def list_tables(self, dataset: Optional[str] = None) -> List[str]:
        ...

    @abstractmethod
    def read_table(self, table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        ...

    @abstractmethod
    def write_table(self, table_name: str, df: pd.DataFrame, if_exists: str = "append") -> int:
        ...

    @abstractmethod
    def delete_rows(self, table_name: str, where_clause: Optional[str] = None) -> int:
        ...

    @abstractmethod
    def delete_table(self, table_name: str) -> None:
        ...

    @abstractmethod
    def close(self) -> None:
        ...