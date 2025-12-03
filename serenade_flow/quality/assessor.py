"""Data Quality Assessor.

This file defines the DataQualityAssesor class. It defines a series of functions that are responsible
for assessing data quality for pipeline runs. It also provides scoring, missing value detection,
schema validation, and duplicate detection.
"""

import pandas as pd


class DataQualityAssessor:
    """
    Assess data quality for pipeline runs. Provides scoring, missing value detection,
    schema validation, and duplicate detection.
    """

    def __init__(self):
        """Initialize Class Variables."""
        pass

    def assess(self, data, schema=None):
        """

        Run all quality checks and return a report dict.

        Accepts either a DataFrame or a dict of DataFrames.

        """

        if isinstance(data, pd.DataFrame):

            data = {"data": data}

        missing = self.missing_values(data)

        schema_valid = self.schema_validation(data, schema)

        duplicates = self.duplicate_detection(data)

        score = self.score(data, schema, missing, schema_valid, duplicates)

        return {
            "score": score,
            "missing_values": missing,
            "schema_validation": schema_valid,
            "duplicates": duplicates,
        }

    def score(self, data, schema, missing, schema_valid, duplicates):

        score = 100

        total_cells = 0

        total_missing = 0

        for fname, miss in missing.items():

            total_missing += miss["total_missing"]

            total_cells += miss["total_cells"]

        if total_cells > 0:

            missing_rate = total_missing / total_cells

            score -= int(missing_rate * 40)  # up to -40 for missing values

        if not all(schema_valid.values()):

            score -= 30  # -30 if any schema invalid

        total_duplicates = sum(len(dups) for dups in duplicates.values())

        total_rows = sum(len(df) for df in data.values())

        if total_rows > 0 and total_duplicates > 0:

            dup_rate = total_duplicates / total_rows

            score -= int(dup_rate * 30)  # up to -30 for duplicates

        return max(0, score)

    def missing_values(self, data):

        result = {}

        for fname, df in data.items():

            if isinstance(df, pd.DataFrame):

                total_cells = df.size

                total_missing = df.isnull().sum().sum()

                missing_per_col = df.isnull().sum().to_dict()

                result[fname] = {
                    "total_missing": int(total_missing),
                    "total_cells": int(total_cells),
                    "missing_per_column": missing_per_col,
                }

        return result

    def schema_validation(self, data, schema):

        result = {}

        if not schema:

            for fname in data:

                result[fname] = True

            return result

        for fname, df in data.items():

            if not isinstance(df, pd.DataFrame):

                result[fname] = False

                continue

            valid = True

            for col, dtype in schema.items():

                if col not in df.columns:

                    valid = False

                    break

                if dtype and not pd.api.types.is_dtype_equal(df[col].dtype, dtype):

                    valid = False

                    break

            result[fname] = valid

        return result

    def duplicate_detection(self, data):

        result = {}

        for fname, df in data.items():

            if isinstance(df, pd.DataFrame):

                dups = df.duplicated()

                result[fname] = df.index[dups].tolist()

        return result
