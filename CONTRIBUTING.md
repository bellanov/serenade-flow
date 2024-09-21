# Contributing Guide

Andy projects accept contributions via GitHub pull requests. This document outlines the process
to help get your contribution accepted.

## Requirements

The project is configured to operate in _Python >= 3.8_ enviornments.

## Development Workflow

First, a local project environment needs to be created, then the project's modules will be installed into locally into a virtual environment.

1. Clone the repository.

   ```sh
   git clone https://github.com/bellanov/serenade-flow.git
   cd serenade-flow
   ```

2. Create a virtual environment.

   ```sh
   # Create Virtual Environment
   python3 -m venv .venv

   # Activate Virtual Environment
   source .venv/bin/activate

   # Install Dependencies
   pip install -r requirements.txt 

   # Deactivate Virtual Environment
   deactivate
   ```

3. Make your changes and **build** the application.

   ```sh
   # Build a Python package distribution
   scripts/build.sh

   # Install the Python package locally, from testpypi.
   scripts/install.sh

   # Publish a distribution to PyPi (testpypi)
   scripts/release.sh

   # Execute Unit Tests
   scripts/test.sh

   # Execute Code Samples
   scripts/examples.sh

   # Lint Code Base
   scripts/lint.sh
   ```

4. Tag and version code changes. This will trigger a build in **Google Cloud Platform (GCP)** that will be associated with the pull request.

   ```sh
   git tag -a "1.2.3" -m "Version 1.2.3"
   git push --follow-tags
   ```