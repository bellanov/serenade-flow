#!/bin/bash
#
# Lint Code Base.

# Lint Code
echo "Linting code base..."

# stop the build if there are Python syntax errors or undefined names
flake8 serenade_flow --count --select=E9,F63,F7,F82 --show-source --statistics
# exit-zero treats all errors as warnings. The GitHub editor is 127 chars wide
flake8 serenade_flow --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

# Lint Tests
echo "Linting tests..."

# stop the build if there are Python syntax errors or undefined names
flake8 tests --count --select=E9,F63,F7,F82 --show-source --statistics
# exit-zero treats all errors as warnings. The GitHub editor is 127 chars wide
flake8 tests --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

# Lint Examples
echo "Linting examples..."

# stop the build if there are Python syntax errors or undefined names
flake8 examples --count --select=E9,F63,F7,F82 --show-source --statistics
# exit-zero treats all errors as warnings. The GitHub editor is 127 chars wide
flake8 examples --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics