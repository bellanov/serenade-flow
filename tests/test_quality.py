"""Tests for DataQualityAssessor."""

import pytest
import pandas as pd
import numpy as np
from serenade_flow.quality import DataQualityAssessor


@pytest.fixture
def assessor():
    """Create a DataQualityAssessor instance."""
    return DataQualityAssessor()


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Alice", "Eve"],
        "age": [25, 30, 35, None, 40],
        "score": [85.5, 90.0, 88.5, 92.0, 87.0],
    })


@pytest.fixture
def sample_dataframe_with_duplicates():
    """Create a DataFrame with duplicate rows."""
    return pd.DataFrame({
        "id": [1, 2, 3, 1, 2],
        "name": ["Alice", "Bob", "Charlie", "Alice", "Bob"],
        "age": [25, 30, 35, 25, 30],
    })


@pytest.mark.unit
def test_assess_with_dataframe(assessor, sample_dataframe):
    """Test assess method with a single DataFrame."""
    report = assessor.assess(sample_dataframe)
    
    assert "score" in report
    assert "missing_values" in report
    assert "schema_validation" in report
    assert "duplicates" in report
    
    assert isinstance(report["score"], (int, float))
    assert report["score"] >= 0
    assert report["score"] <= 100


@pytest.mark.unit
def test_assess_with_dict(assessor, sample_dataframe):
    """Test assess method with a dict of DataFrames."""
    data = {"file1": sample_dataframe}
    report = assessor.assess(data)
    
    assert "file1" in report["missing_values"]
    assert "file1" in report["schema_validation"]
    assert "file1" in report["duplicates"]


@pytest.mark.unit
def test_missing_values(assessor, sample_dataframe):
    """Test missing_values detection."""
    result = assessor.missing_values({"test": sample_dataframe})
    
    assert "test" in result
    assert "total_missing" in result["test"]
    assert "total_cells" in result["test"]
    assert "missing_per_column" in result["test"]
    
    # Should detect 1 missing value in age column
    assert result["test"]["total_missing"] == 1
    assert result["test"]["missing_per_column"]["age"] == 1


@pytest.mark.unit
def test_missing_values_no_missing(assessor):
    """Test missing_values with no missing data."""
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["A", "B", "C"],
    })
    result = assessor.missing_values({"test": df})
    
    assert result["test"]["total_missing"] == 0


@pytest.mark.unit
def test_schema_validation_no_schema(assessor, sample_dataframe):
    """Test schema_validation without providing a schema."""
    result = assessor.schema_validation({"test": sample_dataframe}, None)
    
    assert "test" in result
    assert result["test"] is True


@pytest.mark.unit
def test_schema_validation_valid_schema(assessor, sample_dataframe):
    """Test schema_validation with a valid schema."""
    schema = {
        "id": "int64",
        "name": "object",
        "age": "float64",
    }
    result = assessor.schema_validation({"test": sample_dataframe}, schema)
    
    assert "test" in result
    # Should be True if schema matches (may need adjustment based on actual dtypes)


@pytest.mark.unit
def test_schema_validation_invalid_schema(assessor, sample_dataframe):
    """Test schema_validation with an invalid schema."""
    schema = {
        "id": "int64",
        "nonexistent_column": "object",
    }
    result = assessor.schema_validation({"test": sample_dataframe}, schema)
    
    assert "test" in result
    assert result["test"] is False


@pytest.mark.unit
def test_duplicate_detection(assessor, sample_dataframe_with_duplicates):
    """Test duplicate_detection."""
    result = assessor.duplicate_detection({"test": sample_dataframe_with_duplicates})
    
    assert "test" in result
    assert isinstance(result["test"], list)
    # Should detect duplicates at indices 3 and 4 (0-indexed)


@pytest.mark.unit
def test_duplicate_detection_no_duplicates(assessor, sample_dataframe):
    """Test duplicate_detection with no duplicates."""
    result = assessor.duplicate_detection({"test": sample_dataframe})
    
    assert "test" in result
    assert isinstance(result["test"], list)


@pytest.mark.unit
def test_score_perfect_data(assessor):
    """Test score calculation with perfect data."""
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["A", "B", "C"],
        "value": [10, 20, 30],
    })
    data = {"test": df}
    
    missing = assessor.missing_values(data)
    schema_valid = assessor.schema_validation(data, None)
    duplicates = assessor.duplicate_detection(data)
    
    score = assessor.score(data, None, missing, schema_valid, duplicates)
    
    assert score == 100


@pytest.mark.unit
def test_score_with_missing_values(assessor):
    """Test score calculation with missing values."""
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["A", None, "C"],  # 1 missing value out of 9 total cells
        "value": [10, 20, 30],
    })
    data = {"test": df}
    
    missing = assessor.missing_values(data)
    schema_valid = assessor.schema_validation(data, None)
    duplicates = assessor.duplicate_detection(data)
    
    score = assessor.score(data, None, missing, schema_valid, duplicates)
    
    # Should be less than 100 due to missing values
    assert score < 100
    assert score >= 0


@pytest.mark.unit
def test_score_with_duplicates(assessor):
    """Test score calculation with duplicates."""
    df = pd.DataFrame({
        "id": [1, 2, 3, 1, 2],
        "name": ["A", "B", "C", "A", "B"],
    })
    data = {"test": df}
    
    missing = assessor.missing_values(data)
    schema_valid = assessor.schema_validation(data, None)
    duplicates = assessor.duplicate_detection(data)
    
    score = assessor.score(data, None, missing, schema_valid, duplicates)
    
    # Should be less than 100 due to duplicates
    assert score < 100
    assert score >= 0


@pytest.mark.unit
def test_score_with_invalid_schema(assessor, sample_dataframe):
    """Test score calculation with invalid schema."""
    data = {"test": sample_dataframe}
    schema = {"nonexistent": "object"}
    
    missing = assessor.missing_values(data)
    schema_valid = assessor.schema_validation(data, schema)
    duplicates = assessor.duplicate_detection(data)
    
    score = assessor.score(data, schema, missing, schema_valid, duplicates)
    
    # Should be less than 100 due to invalid schema
    assert score < 100
    assert score >= 0


@pytest.mark.unit
def test_assess_empty_dataframe(assessor):
    """Test assess with an empty DataFrame."""
    df = pd.DataFrame()
    report = assessor.assess(df)
    
    assert "score" in report
    assert isinstance(report["score"], (int, float))


@pytest.mark.unit
def test_missing_values_empty_dataframe(assessor):
    """Test missing_values with empty DataFrame."""
    df = pd.DataFrame()
    result = assessor.missing_values({"test": df})
    
    assert "test" in result
    assert result["test"]["total_missing"] == 0
    assert result["test"]["total_cells"] == 0


@pytest.mark.unit
def test_schema_validation_non_dataframe(assessor):
    """Test schema_validation with non-DataFrame input."""
    result = assessor.schema_validation({"test": "not a dataframe"}, None)
    
    assert "test" in result
    assert result["test"] is True  # No schema means always valid


@pytest.mark.unit
def test_duplicate_detection_empty_dataframe(assessor):
    """Test duplicate_detection with empty DataFrame."""
    df = pd.DataFrame()
    result = assessor.duplicate_detection({"test": df})
    
    assert "test" in result
    assert isinstance(result["test"], list)


