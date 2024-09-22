# Contributing Guide

SerenadeFlow is an ETL Pipeline Implementation powered by WASM. We welcome contributions via GitHub pull requests. This document outlines the process to help get your contribution accepted.

## Project Overview

SerenadeFlow extracts, transforms, and loads data from local or remote data sources. The project is hosted at [https://github.com/bellanov/serenade-flow](https://github.com/bellanov/serenade-flow).

## Requirements

The project is configured to operate in Python >= 3.8 environments.

## Development Workflow

1. Fork the repository on GitHub.

2. Clone your fork locally:

   ```sh
   git clone https://github.com/YOUR_USERNAME/serenade-flow.git
   cd serenade-flow
   ```

3. Create a virtual environment and install dependencies:

   ```sh
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

4. Create a branch for your feature:

   ```sh
   git checkout -b feature-name
   ```

5. Make your changes and commit them:

   ```sh
   git commit -am 'Add some feature'
   ```

6. Push to your fork:

   ```sh
   git push origin feature-name
   ```

7. Submit a pull request through the GitHub website.

## Building and Testing

Use the provided scripts in the `scripts/` directory for building, testing, and linting:

sh

Build a Python package distribution
scripts/build.sh

Install the Python package locally, from testpypi
scripts/install.sh

Publish a distribution to PyPi (testpypi)
scripts/release.sh

Execute Unit Tests
scripts/test.sh
Execute Code Samples
scripts/examples.sh

Lint Code Base
scripts/lint.sh

## Coding Standards

- Follow PEP 8 style guide for Python code.
- Use 4 spaces for indentation.
- Write clear, descriptive commit messages.
- Include comments in your code where necessary.
- Write unit tests for new features or bug fixes.

## Pull Request Process

1. Ensure any install or build dependencies are removed before the end of the layer when doing a build.
2. Update the README.md with details of changes to the interface, including new environment variables, exposed ports, useful file locations, and container parameters.
3. Increase the version numbers in any examples files and the README.md to the new version that this Pull Request would represent.
4. Your Pull Request will be reviewed by the maintainers. Make any requested changes.
5. Once approved, your Pull Request will be merged into the main branch.

## Reporting Issues

Use the [GitHub issue tracker](https://github.com/bellanov/serenade-flow/issues) to report bugs or suggest enhancements. Please provide a clear description and steps to reproduce for bugs.

## License

By contributing to SerenadeFlow, you agree that your contributions will be licensed under the MIT License that covers the project.
