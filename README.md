# ETL Pipeline

ETL Pipeline Implementation. It *Extracts*, *Transforms*, and *Loads* data from both local and remote data sources.

## Requirements

The project is configured to build and test inside a `Docker` in a _Python 3.12_ environment.

## Development Workflow

First, a local project environment needs to be created, then the project's modules will be installed into locally into a virtual environment.

1. Clone the repository.

   ```sh
   git clone https://github.com/bellanov/etl-pipeline.git
   cd etl-pipeline
   ```

3. Create a virtual environment.

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

4. Make your changes and **build** the application.

   ```sh
   # Build & Unit Test Docker Image
   scripts/build.sh

   # Execute Docker Image
   scripts/deploy.sh

   # Teardown Docker Container
   scripts/teardown.sh

   # Purge Docker Image
   scripts/purge.sh

   # Lint Code Base
   scripts/lint.sh
   ```

5. Tag and version code changes. This will trigger a build in **Google Cloud Platform (GCP)** that will be associated with the pull request.

   ```sh
   git tag -a "1.2.3" -m "Version 1.2.3"
   git push --follow-tags
   ```