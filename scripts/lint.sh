#!/bin/bash
#
# Lint the code base.

echo "Linting Package..."
pylint src/

echo "Linting Tests..."
pylint tests/