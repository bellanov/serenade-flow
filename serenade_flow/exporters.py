from __future__ import annotations

from pathlib import Path
from typing import Union

import pandas as pd


class FileExporter:
    def __init__(self, data: pd.DataFrame, file_path: Union[str, Path]) -> None:
        self.data = data
        self.file_path = Path(file_path)

    def export(self) -> None:
        suffix = self.file_path.suffix.lower()
        if suffix == ".csv":
            self._export_csv()
        elif suffix == ".json":
            self._export_json()
        elif suffix == ".parquet":
            self._export_parquet()
        else:
            raise ValueError(f"Unsupported format: {suffix}")

    def _export_csv(self) -> None:
        self.data.to_csv(self.file_path, index=False)

    def _export_json(self) -> None:
        self.data.to_json(self.file_path, orient="records", indent=2)

    def _export_parquet(self) -> None:
        self.data.to_parquet(self.file_path, index=False)


class FileExporterByFormat:
    def __init__(self, data: pd.DataFrame, format: str) -> None:
        self.data = data
        self.format = format.lower()

    def export(self, file_path: Union[str, Path]) -> None:
        if self.format == "parquet":
            self.data.to_parquet(file_path, index=False)
        elif self.format == "csv":
            self.data.to_csv(file_path, index=False)
        elif self.format == "json":
            self.data.to_json(file_path, orient="records", indent=2)
        else:
            raise ValueError(f"No exporter found for format: {self.format}")


def export_csv(data: pd.DataFrame, file_path: Union[str, Path]) -> None:
    data.to_csv(file_path, index=False)


def export_json(data: pd.DataFrame, file_path: Union[str, Path]) -> None:
    data.to_json(file_path, orient="records", indent=2)


def export_parquet(data: pd.DataFrame, file_path: Union[str, Path]) -> None:
    data.to_parquet(file_path, index=False)


def export_data(data: pd.DataFrame, file_path: Union[str, Path]) -> None:
    exporter = FileExporter(data, file_path)
    exporter.export()


def export_to_format(data: pd.DataFrame, format: str, file_path: Union[str, Path]) -> None:
    exporter = FileExporterByFormat(data, format)
    exporter.export(file_path)