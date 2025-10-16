import os
from google.cloud import storage

def list_dynamic_gcs_folder_structure():
    # 1️⃣ Detect current script path
    local_path = os.path.abspath(__file__)
    print(f"Local path: {local_path}")

    # 2️⃣ Get Composer bucket name dynamically
    # Cloud Composer automatically sets this env variable
    bucket_name = os.getenv("GCS_BUCKET")

    if not bucket_name:
        raise EnvironmentError("GCS_BUCKET not found. Not running in Composer?")

    # 3️⃣ Convert local DAG path to GCS prefix
    # /home/airflow/gcs/dags/...  →  dags/...
    gcs_prefix = local_path.replace("/home/airflow/gcs/", "")
    # remove file name to get folder only
    gcs_prefix = os.path.dirname(gcs_prefix)
    if not gcs_prefix.endswith("/"):
        gcs_prefix += "/"

    print(f"Detected bucket: {bucket_name}")
    print(f"Detected prefix: {gcs_prefix}")

    # 4️⃣ Initialize GCS client
    client = storage.Client()

    # 5️⃣ Use delimiter='/' to get folder-like hierarchy
    iterator = client.list_blobs(bucket_name, prefix=gcs_prefix, delimiter='/')

    print("\n📂 Files directly under:", gcs_prefix)
    for blob in iterator:
        print("  🗎", blob.name)

    print("\n📁 Subfolders under:", gcs_prefix)
    for sub_prefix in iterator.prefixes:
        print("  📂", sub_prefix)

    return {
        "bucket": bucket_name,
        "prefix": gcs_prefix,
        "files": [b.name for b in iterator],
        "folders": list(iterator.prefixes)
    }

# Call this inside Composer
if __name__ == "__main__":
    list_dynamic_gcs_folder_structure()
