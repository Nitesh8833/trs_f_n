import os
import logging

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def detect_and_set_env():
    """
    Detect environment (DEV, PROD, QA) from the file path and set it as an environment variable ENV.
    """
    try:
        # Get the absolute path of the current script
        current_file_path = os.path.abspath(__file__)
        logger.info(f"Current Python file path: {current_file_path}")

        # Detect environment from the path (case-insensitive)
        env_detected = None
        for env in ["DEV", "PROD", "QA"]:
            if env.lower() in current_file_path.lower():
                env_detected = env
                break

        # Default to "LOCAL" if nothing matches
        if not env_detected:
            env_detected = "LOCAL"

        # Set environment variable
        os.environ["ENV"] = env_detected
        logger.info(f"✅ Environment detected and set: {env_detected}")

        return env_detected

    except Exception as e:
        logger.error(f"❌ Error detecting environment: {str(e)}")
        raise


# Example usage
if __name__ == "__main__":
    env = detect_and_set_env()
    print(f"Environment Variable 'ENV' = {os.environ.get('ENV')}")
