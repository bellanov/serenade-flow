"""
ETL Pipeline Implementation.

Extract, Load, and Transform data from local or remote data sources.
"""
from concurrent.futures import ThreadPoolExecutor
import logging
from flask import Flask, send_from_directory, request, jsonify
from flask_cors import CORS
from ariadne import ObjectType, make_executable_schema, graphql_sync
from ariadne.explorer import ExplorerGraphiQL
import os

import pandas as pd

# Pipeline Configuration
CONFIG = {}

# Configure Loggiing
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)-15s %(levelname)-8s %(message)s"
)

# Initialize Logging
logger = logging.getLogger("serenade-flow")


def configure(config: dict) -> dict:
    """Configure the ETL Pipeline."""
    logging.info("Configuring Pipeline")

    # TODO: Harden this block with schema validation
    CONFIG["data_format"] = config["data_format"]
    CONFIG["data_source"] = config["data_source"]
    CONFIG["data_source_path"] = config["data_source_path"]
    return CONFIG


def extract_local_data() -> pd.DataFrame:
    """Extract data from a local data source."""
    logging.info("Extracting Local Data")

    # TODO: Retrieve input from a directory of files
    local_data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35},
    ]
    return pd.DataFrame(local_data)


def extract_remote_data():
    """Extract data from a remote data source."""
    logging.info("Extracting Remote Data")
    return {}


def extract() -> pd.DataFrame:
    """Extract."""
    data_future = None
    data_payload = None

    with ThreadPoolExecutor() as executor:
        if CONFIG["data_source"] == "local":
            data_future = executor.submit(extract_local_data)
        elif CONFIG["data_source"] == "remote":
            data_future = executor.submit(extract_remote_data)

        data_payload = data_future.result()

    return data_payload


def load(data: pd.DataFrame, output_prefix: str):
    """Export data to CSV and JSON files."""
    logging.info("Loading Data")
    data.to_csv(f'{output_prefix}.csv', index=False)
    data.to_json(f'{output_prefix}.json', orient='records')
    return "Data loaded successfully"


# Add Flask app
from flask import Flask, request, jsonify
from flask_cors import CORS  # Import CORS

from flask import Flask, jsonify, request
from flask_cors import CORS
from ariadne import ObjectType, make_executable_schema, graphql_sync
from ariadne.explorer import ExplorerGraphiQL

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "http://localhost:8080", "supports_credentials": True}})

# Define GraphQL schema
type_defs = """
    type Query {
        pipelineStatus: String!
        getDataSources: [String!]!
        getTransformedData: [String!]!
    }

    type Mutation {
        startPipeline: String!
        stopPipeline: String!
        extractData(source: String!): String!
        transformData: String!
        loadData: String!
    }
"""

# Define resolvers
query = ObjectType("Query")
mutation = ObjectType("Mutation")

@query.field("pipelineStatus")
def resolve_pipeline_status(*_):
    return "Ready"

@mutation.field("startPipeline")
def resolve_start_pipeline(*_):
    # Add your pipeline start logic here
    return "Pipeline started"

@mutation.field("stopPipeline")
def resolve_stop_pipeline(*_):
    # Add your pipeline stop logic here
    return "Pipeline stopped"

schema = make_executable_schema(type_defs, [query, mutation])

# Create an instance of ExplorerGraphiQL
explorer_html = ExplorerGraphiQL().html(None)

@app.route("/graphql", methods=["GET"])
def graphql_playground():
    return explorer_html, 200

@app.route("/graphql", methods=["POST"])
def graphql_server():
    data = request.get_json()
    success, result = graphql_sync(
        schema,
        data,
        context_value=request,
        debug=app.debug
    )
    status_code = 200 if success else 400
    return jsonify(result), status_code

@app.route('/api/start-pipeline', methods=['POST'])
def start_pipeline():
    # Call your pipeline start function here
    return {"status": "started"}, 200

@app.route('/api/pipeline-status', methods=['GET'])
def get_pipeline_status():
    # Get your pipeline status here
    return {"status": "running"}, 200

@app.route('/')
def home():
    return "Welcome to SerenadeFlow ETL Pipeline"

@app.route('/hello')
def hello():
    return "Hello, World!"

@app.errorhandler(404)
def not_found(e):
    app.logger.error(f"404 error: {e}")
    return "404 Not Found", 404

@app.route('/test')
def test():
    return jsonify({"message": "Backend is reachable"}), 200

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)
