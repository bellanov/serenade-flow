#!/bin/bash
#
# Format Code Base.

# Format Imports
echo "Formatting imports..."
isort examples
isort tests
isort serenade_flow

# Format Code
echo "Formatting code base..."
black examples 
black serenade_flow

# Format Tests
echo "Formatting tests..."
black tests
