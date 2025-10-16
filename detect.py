import os
from google.cloud import storage

def list_dynamic_gcs_folder_structure():
    # 1️⃣ Detect current script path
    local_path = os.path.abspath(__file__)
    print(f"📄 Local path: {local_path}")

    # 2️⃣ Try to get Composer bucket dynamically
    bucket_name = os.getenv("GCS_BUCKET")

    # 3️⃣ If not running in Composer, handle gracefully
    if not bucket_name:
        print("⚠️ Environment variable 'GCS_BUCKET' not found.")
        print("ℹ️ Not running in Cloud Composer — please set bucket_name manually if needed.")
        return {
            "bucket": None,
            "prefix": None,
            "files": [],
            "folders": []
        }

    # 4️⃣ Convert local path to GCS prefix
    # Example: /home/airflow/gcs/dags/...  →  dags/...
    gcs_prefix = local_path.replace("/home/airflow/gcs/", "")
    gcs_prefix = os.path.dirname(gcs_prefix)
    if not gcs_prefix.endswith("/"):
        gcs_prefix += "/"

    print(f"🪣 Detected bucket: {bucket_name}")
    print(f"📁 Detected prefix: {gcs_prefix}")

    # 5️⃣ Initialize GCS client
    client = storage.Client()

    # 6️⃣ Use delimiter='/' to get folder-like hierarchy
    iterator = client.list_blobs(bucket_name, prefix=gcs_prefix, delimiter='/')

    files = []
    for blob in iterator:
        files.append(blob.name)

    folders = list(iterator.prefixes)

    # 7️⃣ Print results
    print("\n📂 Files directly under:", gcs_prefix)
    for f in files:
        print("   🗎", f)

    print("\n📁 Subfolders under:", gcs_prefix)
    for sf in folders:
        print("   📂", sf)

    return {
        "bucket": bucket_name,
        "prefix": gcs_prefix,
        "files": files,
        "folders": folders
    }

# Run directly
if __name__ == "__main__":
    list_dynamic_gcs_folder_structure()
