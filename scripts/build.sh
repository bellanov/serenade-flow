#!/bin/bash
#
# Build a Python distribution.

# Clean Existing Distributions
rm -rf dist/*

# Build
python3 -m build