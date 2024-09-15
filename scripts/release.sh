#!/bin/bash
#
# Publish a Release.

echo "Publishing Python Package..."
python3 -m twine upload --repository testpypi dist/*
