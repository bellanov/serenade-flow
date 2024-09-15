#!/bin/bash
#
# Lint the code base.

echo "Linting Package..."
pylint serenade_flow/

echo "Linting Tests..."
pylint tests/