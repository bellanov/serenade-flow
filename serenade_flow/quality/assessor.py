"""Data Quality Assessor.

This file defines the DataQualityAssesor class. It defines a series of functions that are responsible
for assessing data quality for pipeline runs. It also provides scoring, missing value detection,
schema validation, and duplicate detection.

TODO: Add typical usage documentation
Typical usage example:

  foo = ClassFoo()
  bar = foo.function_bar()
"""

from typing import Any

import pandas as pd


class DataQualityAssessor:
    """
    Assess data quality for pipeline runs. Provides scoring, missing value detection,
    schema validation, and duplicate detection.
    """

    def __init__(self):
        """Initialize Class Variables."""
        pass

    def assess(
        self, data: dict[str, pd.DataFrame] | pd.DataFrame, schema: dict[str, Any] = {}
    ) -> dict[str, Any]:
        """
        Run all quality checks and return a report dict.

        Args:
            data: A DataFrame or a dictionary of DataFrames.
            schema: TODO: Add description of argument.

        Returns:
            TODO: Describe the value being returned.
        """

        # Process individual DataFrame as input
        if isinstance(data, pd.DataFrame):
            data = {"data": data}

        # TODO: Briefly describe what's happening here
        missing = self.missing_values(data)

        # TODO: Briefly describe what's happening here
        schema_valid = self.schema_validation(data, schema)

        # TODO: Briefly describe what's happening here
        duplicates = self.duplicate_detection(data)

        # TODO: Briefly describe what's happening here
        score = self.score(data, schema, missing, schema_valid, duplicates)

        return {
            "score": score,
            "missing_values": missing,
            "schema_validation": schema_valid,
            "duplicates": duplicates,
        }

    def score(self, data, schema, missing: dict[str, Any], schema_valid, duplicates):
        """
        TODO: Add function description.

        Args:
            data: A DataFrame or a dictionary of DataFrames.
            schema: TODO: Add description of argument.
            missing: TODO: Add description of argument.
            schema_valid: TODO: Add description of argument.
            duplicates: TODO: Add description of argument.

        Returns:
            TODO: Describe the value being returned.
        """

        score = 100
        total_cells = 0
        total_missing = 0

        # TODO: Briefly describe this block
        for miss in missing.values():

            # TODO: Briefly describe what's happening here
            total_missing += miss["total_missing"]

            # TODO: Briefly describe what's happening here
            total_cells += miss["total_cells"]

        # TODO: Briefly describe this block
        if total_cells > 0:

            # TODO: Briefly describe what's happening here
            missing_rate = total_missing / total_cells

            # Score up to -40 for missing values
            score -= int(missing_rate * 40)

        # TODO: Briefly describe what's happening here
        if not all(schema_valid.values()):
            # Score -30 if any schema invalid
            score -= 30

        # TODO: Briefly describe what's happening here
        total_duplicates = sum(len(dups) for dups in duplicates.values())

        # TODO: Briefly describe what's happening here
        total_rows = sum(len(df) for df in data.values())

        # TODO: Briefly describe this block
        if total_rows > 0 and total_duplicates > 0:

            # TODO: Briefly describe what's happening here
            dup_rate = total_duplicates / total_rows

            # Score up to -30 for duplicates
            score -= int(dup_rate * 30)

        return max(0, score)

    def missing_values(
        self, data: dict[str, pd.DataFrame] | pd.DataFrame
    ) -> dict[str, Any]:
        """
        TODO: Add function description.

        Args:
            data: A DataFrame or a dictionary of DataFrames.

        Returns:
            TODO: Describe the value being returned.
        """

        result = {}

        # TODO: Briefly describe this block
        for fname, df in data.items():

            # TODO: Briefly describe this block
            if isinstance(df, pd.DataFrame):

                # TODO: Briefly describe what's happening here
                total_cells = df.size

                # TODO: Briefly describe what's happening here
                total_missing = df.isnull().sum().sum()

                # TODO: Briefly describe what's happening here
                missing_per_col = df.isnull().sum().to_dict()

                result[fname] = {
                    "total_missing": int(total_missing),
                    "total_cells": int(total_cells),
                    "missing_per_column": missing_per_col,
                }

        return result

    def schema_validation(self, data, schema):
        """
        TODO: Add function description.

        Args:
            data: A DataFrame or a dictionary of DataFrames.
            schema: TODO: Add description of argument.

        Returns:
            TODO: Describe the value being returned.
        """

        result = {}

        # TODO: Briefly describe this block
        if not schema:

            for fname in data:

                # TODO: Briefly describe what's happening here
                result[fname] = True

            return result

        # TODO: Briefly describe this block
        for fname, df in data.items():

            # TODO: Briefly describe this block
            if not isinstance(df, pd.DataFrame):

                # TODO: Briefly describe what's happening here
                result[fname] = False

                continue

            valid = True

            # TODO: Briefly describe this block
            for col, dtype in schema.items():

                # TODO: Briefly describe this block
                if col not in df.columns:
                    valid = False
                    break

                # TODO: Briefly describe this block
                if dtype and not pd.api.types.is_dtype_equal(df[col].dtype, dtype):
                    valid = False
                    break

            result[fname] = valid

        return result

    def duplicate_detection(self, data):
        """
        TODO: Add function description.

        Args:
            data: A DataFrame or a dictionary of DataFrames.

        Returns:
            TODO: Describe the value being returned.
        """

        result = {}

        # TODO: Briefly describe this block
        for fname, df in data.items():

            # TODO: Briefly describe this block
            if isinstance(df, pd.DataFrame):

                # TODO: Briefly describe what's happening here
                dups = df.duplicated()

                # TODO: Briefly describe what's happening here
                result[fname] = df.index[dups].tolist()

        return result
