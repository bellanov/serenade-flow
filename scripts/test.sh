#!/bin/bash
#
# Execute unit tests.

echo "Executing Unit Tests..."
coverage run -m pytest tests/
