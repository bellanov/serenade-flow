#!/bin/bash
#
# Execute unit tests.

echo "Executing Unit Tests..."
# pytest tests/
coverage run -m pytest tests/
