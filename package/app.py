"""
Simple Python Program.
"""
import logging

# Configure string format for consumption into logging platforms.
logging.basicConfig(
level=logging.DEBUG, format="%(asctime)-15s %(levelname)-8s %(message)s"
)

# Initialize
logger = logging.getLogger("python-template")


def app():
    """Simulates an application."""
    logger.info("EXECUTING application task")
    return "Hello World!"


app()
