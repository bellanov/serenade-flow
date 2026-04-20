from __future__ import annotations

from pathlib import Path
from typing import Union

import pandas as pd


class FileReader:
    def __init__(self, file_path: Union[str, Path]) -> None:
        self.file_path = Path(file_path)

    def read(self) -> pd.DataFrame:
        suffix = self.file_path.suffix.lower()
        if suffix == ".csv":
            return self._read_csv()
        elif suffix == ".json":
            return self._read_json()
        elif suffix == ".parquet":
            return self._read_parquet()
        else:
            raise ValueError(f"Unsupported format: {suffix}")

    def _read_csv(self) -> pd.DataFrame:
        return pd.read_csv(self.file_path)

    def _read_json(self) -> pd.DataFrame:
        return pd.read_json(self.file_path)

    def _read_parquet(self) -> pd.DataFrame:
        return pd.read_parquet(self.file_path)


def read_csv(file_path: Union[str, Path]) -> pd.DataFrame:
    return pd.read_csv(file_path)


def read_json(file_path: Union[str, Path]) -> pd.DataFrame:
    return pd.read_json(file_path)


def read_parquet(file_path: Union[str, Path]) -> pd.DataFrame:
    return pd.read_parquet(file_path)


def read_data(file_path: Union[str, Path]) -> pd.DataFrame:
    return FileReader(file_path).read()