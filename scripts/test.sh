#!/bin/bash
#
# Execute unit tests.

echo "Executing Unit Tests..."
coverage run -m pytest tests/

echo "Generating Report..."
coverage report -m
