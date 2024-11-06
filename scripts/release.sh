#!/bin/bash
#
# Publish a Release.

REPOSITORY=${1}

# TODO: Parameterize repository and document usage token usage within .pypirc
echo "Publishing Python Package"
python3 -m twine upload --verbose --repository testpypi dist/*
