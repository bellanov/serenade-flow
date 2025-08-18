#!/bin/bash
#
# Execute unit tests.

echo "Executing Unit Tests..."
python3 -m coverage run -m pytest tests/

echo "Generating Report..."
python3 -m coverage report -m

echo "Build HTML Report..."
python3 -m coverage html