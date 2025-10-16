import os
import logging

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console.setFormatter(formatter)
    logger.addHandler(console)


def detect_env_from_gcs_uri():
    """
    Detect environment (DEV, PROD, QA, LOCAL) from the gs:// URI of the current Python file.

    Steps:
    1. Get the absolute local path (e.g., /home/airflow/gcs/dags/DEV/my_script.py)
    2. Convert it to a gs:// URI using Composer bucket info
    3. Detect the environment keyword in that URI (DEV/PROD/QA)
    4. Store ENV and GS_PATH in environment variables
    """
    try:
        # Get the absolute path of the current script
        current_file_path = os.path.abspath(__file__)
        logger.info(f"Current Python file path: {current_file_path}")

        # Detect if running inside Composer
        if "/home/airflow/gcs/" not in current_file_path:
            logger.warning("⚠️ Not inside Composer GCS path. Using LOCAL mode.")
            os.environ["ENV"] = "LOCAL"
            os.environ["GS_PATH"] = current_file_path
            return {"env": "LOCAL", "gcs_uri": None}

        # Get Composer bucket name
        gcs_bucket = os.environ.get("GCS_BUCKET")
        if not gcs_bucket:
            # Try to infer bucket name from environment variables commonly set by Composer
            gcs_bucket = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "")
            # If still empty, ask user to set it once
            if not gcs_bucket:
                logger.warning("⚠️ Environment variable GCS_BUCKET not set. Please define it manually.")
                gcs_bucket = "<your-composer-bucket-name>"

        # Replace the local Composer path with gs://
        gs_uri = current_file_path.replace("/home/airflow/gcs/", f"gs://{gcs_bucket}/")
        logger.info(f"🟢 Computed GCS URI: {gs_uri}")

        # Detect environment from the GCS URI (case-insensitive)
        env_detected = "LOCAL"
        for env in ["DEV", "PROD", "QA"]:
            if env.lower() in gs_uri.lower():
                env_detected = env
                break

        # Set as environment variable
        os.environ["ENV"] = env_detected
        os.environ["GS_PATH"] = gs_uri

        logger.info(f"✅ Environment detected from GCS URI: {env_detected}")
        logger.info(f"✅ GCS Path set in ENV: {gs_uri}")

        return {"env": env_detected, "gcs_uri": gs_uri}

    except Exception as e:
        logger.error(f"❌ Error detecting environment from GCS URI: {str(e)}")
        raise


# Example usage
if __name__ == "__main__":
    result = detect_env_from_gcs_uri()
    print(f"Environment: {result['env']}")
    print(f"GCS URI: {result['gcs_uri']}")

***************************************************
import os
from google.cloud import storage

# Example: Set environment variable for authentication
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path/to/service_account.json"

def get_gsutil_uri(bucket_name, file_name):
    """
    Returns the gsutil URI (gs://bucket_name/file_name)
    for a specified file in a Google Cloud Storage bucket.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Construct gsutil URI
    gs_uri = f"gs://{blob.bucket.name}/{blob.name}"
    return gs_uri

if __name__ == "__main__":
    # Example bucket and file
    bucket_name = "your-bucket-name"
    file_name = "path/to/your_file.py"

    print("Fetching GCS URI...")
    uri = get_gsutil_uri(bucket_name, file_name)
    print(f"The gsutil URI for the file is: {uri}")
