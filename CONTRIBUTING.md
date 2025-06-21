# Contributing Guide

Andy projects accept contributions via GitHub pull requests. This document outlines the process
to help get your contribution accepted.

## Prerequisites

- `python` 3.00 or greater
- `pip`

## Issue Assignment

Before any development is undertaken, there should be a **GitHub Issue** created to properly *track* and *isolate* the changes. A branch is then created from the issue and it is to that branch that work will be submitted.

1. Consult the *[Andy Project Backlog](https://github.com/users/bellanov/projects/18/views/6)* to see any tickets that are **Unassigned**. If a task is available for this *repository*, **assign** it to yourself to reserve it. The backlog contains pending *GitHub Issues* that have yet to be scheduled for development within an iteration.

2. Once you have assigned yourself an issue, schedule the issue within an **Iteration** so it is visible on the *Kanban Board*. Once the issue is visible on the Kanban Board, drag the issue from **Backlog** to **In Progress**.

3. After the issue has been transitioned to **In Progress**, a **Branch** within the issue should be created to properly isolate changes. This branch will been the one to conduct development within, or open external *Pull Requests* against.

## PyPi API Key

An **API Key** is required to be able to publish releases to *testpypi*. Upon the assignment of an issue, the repository owner will be in touch.

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

3. Make your changes, increment the version in `pyproject.toml`, and **build** the application.

   ```sh
   # Build a Python package distribution
   scripts/build.sh

   # Publish a distribution to PyPi (testpypi)
   scripts/release.sh

   # Install the Python package locally, from testpypi.
   scripts/install.sh "<VERSION>"

   # Execute Unit Tests
   scripts/test.sh

   # Execute Code Samples
   scripts/examples.sh

   # Lint Code Base
   scripts/lint.sh
   ```

4. Tag and version code changes.

   ```sh
   git tag -a "1.2.3" -m "Version 1.2.3"
   git push --follow-tags
   ```