#!/usr/bin/env python
"""Example demonstrating reading and exporting data to CSV, JSON, and PARQUET formats."""

import sys
from pathlib import Path

from serenade_flow import readers, exporters


def main():
    data_dir = Path(__file__).parent / "data"
    csv_file = next(data_dir.glob("*.csv"))

    print(f"Reading from: {csv_file}")
    df = readers.read_csv(csv_file)
    print(f"Loaded {len(df)} rows\n")

    base_name = csv_file.stem

    export_dir = Path("output")
    export_dir.mkdir(exist_ok=True)

    print("Exporting to CSV, JSON, and PARQUET...")
    exporters.export_csv(df, export_dir / f"{base_name}_copy.csv")
    exporters.export_json(df, export_dir / f"{base_name}.json")
    exporters.export_parquet(df, export_dir / f"{base_name}.parquet")

    print(f"Exported to: {export_dir}\n")

    print("Verifying exports (auto-detect format)...")
    df_json = readers.read_data(export_dir / f"{base_name}.json")
    df_parquet = readers.read_data(export_dir / f"{base_name}.parquet")
    df_csv = readers.read_data(export_dir / f"{base_name}_copy.csv")

    print(f"  JSON: {len(df_json)} rows")
    print(f"  PARQUET: {len(df_parquet)} rows")
    print(f"  CSV: {len(df_csv)} rows")


if __name__ == "__main__":
    main()