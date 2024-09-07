
FROM python:3.12-alpine

COPY . /app
WORKDIR /app

# Install dependencies
RUN pip install -r requirements.txt

# Execute Unit Tests / Validate Configuration
RUN pytest tests/

# Execute the application
ENTRYPOINT [ "scripts/entrypoint.sh" ]
