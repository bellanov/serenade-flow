"""Tests for DataQualityAssessor."""

import pandas as pd
import pytest

from serenade_flow.quality import DataQualityAssessor


@pytest.fixture  # type: ignore
def assessor() -> DataQualityAssessor:
    """Create a DataQualityAssessor instance."""
    return DataQualityAssessor()


@pytest.fixture  # type: ignore
def sample_dataframe() -> pd.DataFrame:
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Alice", "Eve"],
            "age": [25, 30, 35, None, 40],
            "score": [85.5, 90.0, 88.5, 92.0, 87.0],
        }
    )


@pytest.fixture  # type: ignore
def sample_dataframe_with_duplicates() -> pd.DataFrame:
    """Create a DataFrame with duplicate rows."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 1, 2],
            "name": ["Alice", "Bob", "Charlie", "Alice", "Bob"],
            "age": [25, 30, 35, 25, 30],
        }
    )


@pytest.mark.unit  # type: ignore
def test_assess_with_dataframe(
    assessor: DataQualityAssessor, sample_dataframe: pd.DataFrame
):
    """Test assess method with a single DataFrame."""
    report = assessor.assess(sample_dataframe)

    assert "score" in report
    assert "missing_values" in report
    assert "schema_validation" in report
    assert "duplicates" in report

    assert isinstance(report["score"], (int, float))
    assert report["score"] >= 0
    assert report["score"] <= 100


@pytest.mark.unit  # type: ignore
def test_assess_with_dict(
    assessor: DataQualityAssessor, sample_dataframe: pd.DataFrame
):
    """Test assess method with a dict of DataFrames."""
    data = {"file1": sample_dataframe}
    report = assessor.assess(data)

    assert "file1" in report["missing_values"]
    assert "file1" in report["schema_validation"]
    assert "file1" in report["duplicates"]


@pytest.mark.unit  # type: ignore
def test_missing_values(assessor: DataQualityAssessor, sample_dataframe: pd.DataFrame):
    """Test missing_values detection."""
    result = assessor.missing_values({"test": sample_dataframe})

    assert "test" in result
    assert "total_missing" in result["test"]
    assert "total_cells" in result["test"]
    assert "missing_per_column" in result["test"]

    # Should detect 1 missing value in age column
    assert result["test"]["total_missing"] == 1
    assert result["test"]["missing_per_column"]["age"] == 1


@pytest.mark.unit  # type: ignore
def test_missing_values_no_missing(assessor: DataQualityAssessor):
    """Test missing_values with no missing data."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
        }
    )
    result = assessor.missing_values({"test": df})

    assert result["test"]["total_missing"] == 0


@pytest.mark.unit  # type: ignore
def test_schema_validation_no_schema(
    assessor: DataQualityAssessor, sample_dataframe: pd.DataFrame
):
    """Test schema_validation without providing a schema."""
    result = assessor.schema_validation({"test": sample_dataframe}, None)  # type: ignore

    assert "test" in result
    assert result["test"] is True


@pytest.mark.unit  # type: ignore
def test_schema_validation_valid_schema(
    assessor: DataQualityAssessor, sample_dataframe: pd.DataFrame
):
    """Test schema_validation with a valid schema."""
    schema = {
        "id": "int64",
        "name": "object",
        "age": "float64",
    }
    result = assessor.schema_validation({"test": sample_dataframe}, schema)

    assert "test" in result
    # Should be True if schema matches (may need adjustment based on actual dtypes)


@pytest.mark.unit  # type: ignore
def test_schema_validation_invalid_schema(
    assessor: DataQualityAssessor, sample_dataframe: pd.DataFrame
):
    """Test schema_validation with an invalid schema."""
    schema = {
        "id": "int64",
        "nonexistent_column": "object",
    }
    result = assessor.schema_validation({"test": sample_dataframe}, schema)

    assert "test" in result
    assert result["test"] is False


@pytest.mark.unit  # type: ignore
def test_duplicate_detection(
    assessor: DataQualityAssessor, sample_dataframe_with_duplicates: pd.DataFrame
):
    """Test duplicate_detection."""
    result = assessor.duplicate_detection({"test": sample_dataframe_with_duplicates})

    assert "test" in result
    assert isinstance(result["test"], list)
    # Should detect duplicates at indices 3 and 4 (0-indexed)


@pytest.mark.unit  # type: ignore
def test_duplicate_detection_no_duplicates(
    assessor: DataQualityAssessor, sample_dataframe: pd.DataFrame
):
    """Test duplicate_detection with no duplicates."""
    result = assessor.duplicate_detection({"test": sample_dataframe})

    assert "test" in result
    assert isinstance(result["test"], list)


@pytest.mark.unit  # type: ignore
def test_score_perfect_data(assessor: DataQualityAssessor):
    """Test score calculation with perfect data."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "value": [10, 20, 30],
        }
    )
    data = {"test": df}

    missing = assessor.missing_values(data)
    schema_valid = assessor.schema_validation(data, None)  # type: ignore
    duplicates = assessor.duplicate_detection(data)

    score = assessor.score(data, missing, schema_valid, duplicates)

    assert score == 100


@pytest.mark.unit  # type: ignore
def test_score_with_missing_values(assessor: DataQualityAssessor):
    """Test score calculation with missing values."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["A", None, "C"],  # 1 missing value out of 9 total cells
            "value": [10, 20, 30],
        }
    )
    data = {"test": df}

    missing = assessor.missing_values(data)
    schema_valid = assessor.schema_validation(data, None)  # type: ignore
    duplicates = assessor.duplicate_detection(data)

    score = assessor.score(data, missing, schema_valid, duplicates)

    # Should be less than 100 due to missing values
    assert score < 100
    assert score >= 0


@pytest.mark.unit  # type: ignore
def test_score_with_duplicates(assessor: DataQualityAssessor):
    """Test score calculation with duplicates."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 1, 2],
            "name": ["A", "B", "C", "A", "B"],
        }
    )
    data = {"test": df}

    missing = assessor.missing_values(data)
    schema_valid = assessor.schema_validation(data, None)  # type: ignore
    duplicates = assessor.duplicate_detection(data)

    score = assessor.score(data, missing, schema_valid, duplicates)

    # Should be less than 100 due to duplicates
    assert score < 100
    assert score >= 0


@pytest.mark.unit  # type: ignore
def test_score_with_invalid_schema(
    assessor: DataQualityAssessor, sample_dataframe: pd.DataFrame
):
    """Test score calculation with invalid schema."""
    data = {"test": sample_dataframe}
    schema = {"nonexistent": "object"}

    missing = assessor.missing_values(data)
    schema_valid = assessor.schema_validation(data, schema)
    duplicates = assessor.duplicate_detection(data)

    score = assessor.score(data, missing, schema_valid, duplicates)

    # Should be less than 100 due to invalid schema
    assert score < 100
    assert score >= 0


@pytest.mark.unit  # type: ignore
def test_assess_empty_dataframe(assessor: DataQualityAssessor):
    """Test assess with an empty DataFrame."""
    df = pd.DataFrame()
    report = assessor.assess(df)

    assert "score" in report
    assert isinstance(report["score"], (int, float))


@pytest.mark.unit  # type: ignore
def test_missing_values_empty_dataframe(assessor: DataQualityAssessor):
    """Test missing_values with empty DataFrame."""
    df = pd.DataFrame()
    result = assessor.missing_values({"test": df})

    assert "test" in result
    assert result["test"]["total_missing"] == 0
    assert result["test"]["total_cells"] == 0


@pytest.mark.unit  # type: ignore
def test_schema_validation_non_dataframe(assessor: DataQualityAssessor):
    """Test schema_validation with non-DataFrame input."""
    result = assessor.schema_validation({"test": "not a dataframe"}, None)  # type: ignore

    assert "test" in result
    assert result["test"] is True  # No schema means always valid


@pytest.mark.unit  # type: ignore
def test_duplicate_detection_empty_dataframe(assessor: DataQualityAssessor):
    """Test duplicate_detection with empty DataFrame."""
    df = pd.DataFrame()
    result = assessor.duplicate_detection({"test": df})

    assert "test" in result
    assert isinstance(result["test"], list)


@pytest.mark.unit  # type: ignore
def test_schema_validation_dtype_mismatch(assessor: DataQualityAssessor):
    """Test schema_validation with dtype mismatch (covers lines 217-218)."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "age": [25, 30, 35],
        }
    )
    # Schema expects int64 for age, but DataFrame has int64 (should match)
    # Let's create a mismatch by expecting float64 when we have int64
    schema = {
        "id": "int64",
        "name": "object",
        "age": "float64",  # Mismatch: DataFrame has int64, schema expects float64
    }
    result = assessor.schema_validation({"test": df}, schema)

    assert "test" in result
    # Should be False due to dtype mismatch
    assert result["test"] is False


@pytest.mark.unit  # type: ignore
def test_schema_validation_dtype_match(assessor: DataQualityAssessor):
    """Test schema_validation with matching dtypes."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "age": [25.0, 30.0, 35.0],  # float64
        }
    )
    schema = {
        "id": "int64",
        "name": "object",
        "age": "float64",  # Matches DataFrame dtype
    }
    result = assessor.schema_validation({"test": df}, schema)

    assert "test" in result
    assert result["test"] is True


@pytest.mark.unit  # type: ignore
def test_assess_with_multiple_dataframes(assessor: DataQualityAssessor):
    """Test assess method with multiple DataFrames in a dictionary."""
    df1 = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
        }
    )
    df2 = pd.DataFrame(
        {
            "id": [4, 5, 6],
            "value": [10, 20, 30],
        }
    )
    data = {"file1": df1, "file2": df2}
    report = assessor.assess(data)

    assert "score" in report
    assert "file1" in report["missing_values"]
    assert "file2" in report["missing_values"]
    assert "file1" in report["schema_validation"]
    assert "file2" in report["schema_validation"]
    assert "file1" in report["duplicates"]
    assert "file2" in report["duplicates"]


@pytest.mark.unit  # type: ignore
def test_missing_values_multiple_dataframes(assessor: DataQualityAssessor):
    """Test missing_values with multiple DataFrames."""
    df1 = pd.DataFrame({"col1": [1, 2, None]})
    df2 = pd.DataFrame({"col2": [None, "B", "C"]})
    data = {"df1": df1, "df2": df2}

    result = assessor.missing_values(data)

    assert "df1" in result
    assert "df2" in result
    assert result["df1"]["total_missing"] == 1
    assert result["df2"]["total_missing"] == 1


@pytest.mark.unit  # type: ignore
def test_schema_validation_multiple_dataframes(assessor: DataQualityAssessor):
    """Test schema_validation with multiple DataFrames."""
    df1 = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
    df2 = pd.DataFrame({"id": [3, 4], "value": [10, 20]})
    data = {"df1": df1, "df2": df2}
    schema = {"id": "int64", "name": "object"}

    result = assessor.schema_validation(data, schema)

    assert "df1" in result
    assert "df2" in result
    # df1 should pass (has id and name)
    assert result["df1"] is True
    # df2 should fail (missing name column)
    assert result["df2"] is False


@pytest.mark.unit  # type: ignore
def test_duplicate_detection_multiple_dataframes(assessor: DataQualityAssessor):
    """Test duplicate_detection with multiple DataFrames."""
    df1 = pd.DataFrame({"id": [1, 2, 1]})  # Has duplicates
    df2 = pd.DataFrame({"id": [3, 4, 5]})  # No duplicates
    data = {"df1": df1, "df2": df2}

    result = assessor.duplicate_detection(data)

    assert "df1" in result
    assert "df2" in result
    assert len(result["df1"]) > 0  # Has duplicates
    assert len(result["df2"]) == 0  # No duplicates


@pytest.mark.unit  # type: ignore
def test_score_multiple_dataframes(assessor: DataQualityAssessor):
    """Test score calculation with multiple DataFrames."""
    df1 = pd.DataFrame({"id": [1, 2, 3]})
    df2 = pd.DataFrame({"id": [4, 5, 6]})
    data = {"df1": df1, "df2": df2}

    missing = assessor.missing_values(data)
    schema_valid = assessor.schema_validation(data, None)  # type: ignore
    duplicates = assessor.duplicate_detection(data)

    score = assessor.score(data, missing, schema_valid, duplicates)

    assert score == 100  # Perfect data across both DataFrames
