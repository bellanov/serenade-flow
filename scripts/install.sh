#!/bin/bash
#
# Install the Python package locally.

VERSION=${1}

pip uninstall -y andy-serenade-flow
pip install -i https://test.pypi.org/simple/ andy-serenade-flow==${VERSION}