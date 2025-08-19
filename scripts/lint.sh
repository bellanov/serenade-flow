#!/bin/bash
#
# Lint the code base.

echo "Linting Codebase..."
python3 -m flake8 . --count --max-complexity=10 --max-line-length=127 --statistics --exclude .venv
