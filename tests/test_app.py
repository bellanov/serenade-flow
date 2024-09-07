"""Test Template."""

import pytest

from package import app


@pytest.mark.unit
def test_app():
    """Validate package is importable"""
    assert app.app() == "Hello World!"
