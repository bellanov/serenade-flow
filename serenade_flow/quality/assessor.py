"""Data Quality Assessor.

This file defines the DataQualityAssesor class. It defines a series of functions that are responsible
for assessing data quality for pipeline runs. It also provides scoring, missing value detection,
schema validation, and duplicate detection.

Typical usage example:

  from serenade_flow.quality import DataQualityAssessor
  import pandas as pd

  assessor = DataQualityAssessor()
  df = pd.DataFrame({"col1": [1, 2, None], "col2": ["a", "b", "c"]})
  schema = {"col1": "float64", "col2": "object"}
  report = assessor.assess(df, schema)
  print(f"Quality score: {report['score']}")
"""

from typing import Any, Hashable

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
        self,
        data: dict[Hashable, pd.DataFrame] | pd.DataFrame,
        schema: dict[str, Any] = {},
    ) -> dict[str, Any]:
        """
        Run all quality checks and return a report dict.

        Args:
            data: A DataFrame or a dictionary of DataFrames.
            schema: Optional schema dict mapping column names to expected data types (dtypes).

        Returns:
            A dictionary with quality assessment results containing 'score',
            'missing_values', 'schema_validation', and 'duplicates'.
        """

        # Process individual DataFrame as input
        if isinstance(data, pd.DataFrame):
            data = {"data": data}

        # Detect missing values across all DataFrames
        missing = self.missing_values(data)

        # Validate schema compliance for all DataFrames
        schema_valid = self.schema_validation(data, schema)

        # Identify duplicate rows in all DataFrames
        duplicates = self.duplicate_detection(data)

        # Calculate overall quality score based on all checks
        score = self.score(data, schema, missing, schema_valid, duplicates)

        return {
            "score": score,
            "missing_values": missing,
            "schema_validation": schema_valid,
            "duplicates": duplicates,
        }

    def score(
        self,
        data: dict[Hashable, pd.DataFrame] | pd.DataFrame,
        schema: dict[str, Any],
        missing: dict[Hashable, Any],
        schema_valid: dict[Hashable, bool],
        duplicates: dict[Hashable, list[int]],
    ):
        """
        Calculate a quality score from 0-100 based on missing values, schema validation, and duplicates.

        Args:
            data: A DataFrame or a dictionary of DataFrames.
            schema: Schema dict mapping column names to expected data types (dtypes).
            missing: Dictionary of missing value statistics from missing_values().
            schema_valid: Dictionary mapping DataFrame names to validation booleans.
            duplicates: Dictionary mapping DataFrame names to lists of duplicate row indices.

        Returns:
            An integer quality score from 0 to 100.
        """

        score = 100
        total_cells = 0
        total_missing = 0

        # Aggregate missing value statistics across all DataFrames
        for miss in missing.values():

            # Sum total missing cells across all DataFrames
            total_missing += miss["total_missing"]

            # Sum total cells across all DataFrames
            total_cells += miss["total_cells"]

        # Calculate missing value penalty if there are any cells
        if total_cells > 0:

            # Compute the proportion of missing values
            missing_rate = total_missing / total_cells

            # Score up to -40 for missing values
            score -= int(missing_rate * 40)

        # Apply penalty if any DataFrame fails schema validation
        if not all(schema_valid.values()):
            # Score -30 if any schema invalid
            score -= 30

        # Count total number of duplicate rows across all DataFrames
        total_duplicates = sum(len(dups) for dups in duplicates.values())

        # Count total number of rows across all DataFrames
        total_rows = sum(len(df) for df in data.values())

        # Calculate duplicate penalty if there are rows and duplicates
        if total_rows > 0 and total_duplicates > 0:

            # Compute the proportion of duplicate rows
            dup_rate = total_duplicates / total_rows

            # Score up to -30 for duplicates
            score -= int(dup_rate * 30)

        return max(0, score)

    def missing_values(
        self, data: dict[Hashable, pd.DataFrame] | pd.DataFrame
    ) -> dict[Hashable, Any]:
        """
        Detect and count missing values in DataFrames.

        Args:
            data: A DataFrame or a dictionary of DataFrames.

        Returns:
            A dictionary mapping DataFrame names to missing value statistics,
            including total_missing, total_cells, and missing_per_column.
        """

        result: dict[Hashable, Any] = {}

        # Process each DataFrame in the input
        for fname, df in data.items():

            # Only process valid DataFrame objects
            if isinstance(df, pd.DataFrame):

                # Count total number of cells in the DataFrame
                total_cells = df.size

                # Count total number of missing values across all columns
                total_missing = df.isnull().sum().sum()

                # Count missing values per column as a dictionary
                missing_per_col = df.isnull().sum().to_dict()

                result[fname] = {
                    "total_missing": int(total_missing),
                    "total_cells": int(total_cells),
                    "missing_per_column": missing_per_col,
                }

        return result

    def schema_validation(
        self, data: dict[Hashable, pd.DataFrame] | pd.DataFrame, schema: dict[str, Any]
    ) -> dict[Hashable, Any]:
        """
        Validate that DataFrames conform to the expected schema.

        Args:
            data: A DataFrame or a dictionary of DataFrames.
            schema: Dictionary mapping column names to expected pandas data types (dtypes).

        Returns:
            A dictionary mapping DataFrame names to boolean validation results.
        """

        result: dict[Hashable, Any] = {}

        # If no schema provided, all DataFrames are considered valid
        if not schema:

            for fname in data:

                # Mark all DataFrames as valid when no schema is specified
                result[fname] = True

            return result

        # Validate each DataFrame against the schema
        for fname, df in data.items():

            # Non-DataFrame objects fail validation
            if not isinstance(df, pd.DataFrame):

                # Mark invalid if not a DataFrame
                result[fname] = False

                continue

            valid = True

            # Check each column in the schema
            for col, dtype in schema.items():

                # Fail if required column is missing
                if col not in df.columns:
                    valid = False
                    break

                # Fail if column dtype doesn't match expected dtype
                if dtype and not pd.api.types.is_dtype_equal(df[col].dtype, dtype):
                    valid = False
                    break

            result[fname] = valid

        return result

    def duplicate_detection(
        self, data: dict[Hashable, pd.DataFrame] | pd.DataFrame
    ) -> dict[Hashable, Any]:
        """
        Detect duplicate rows in DataFrames.

        Args:
            data: A DataFrame or a dictionary of DataFrames.

        Returns:
            A dictionary mapping DataFrame names to lists of duplicate row indices.
        """

        result: dict[Hashable, Any] = {}

        # Process each DataFrame in the input
        for fname, df in data.items():

            # Only process valid DataFrame objects
            if isinstance(df, pd.DataFrame):

                # Identify duplicate rows using pandas duplicated()
                dups = df.duplicated()

                # Store list of indices for duplicate rows
                result[fname] = df.index[dups].tolist()

        return result
