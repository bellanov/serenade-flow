#!/bin/bash
#
# Format Code Base.

# Format Imports
echo "Formatting imports..."
isort examples
isort tests

# Format Code
echo "Formatting code base..."
black examples 

# Format Tests
echo "Formatting tests..."
black tests
