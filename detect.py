import os
from google.cloud import storage
from typing import Dict, List, Optional

def detect_gcs_prefix_from_path(local_path: str) -> Optional[str]:
    """
    Try multiple heuristics to produce a gcs-style prefix (like 'dags/.../python_scripts/')
    from a local filesystem path.
    Returns prefix (ending with '/') or None if not detected.
    """
    # normalize
    p = os.path.normpath(local_path)

    # 1) Composer mount common path
    marker = os.path.normpath("/home/airflow/gcs")
    if p.startswith(marker):
        # remove mount and take dirname
        rel = p[len(marker) + 1:]  # +1 to remove the separator
        rel_dir = os.path.dirname(rel)
        return rel_dir + ("/" if not rel_dir.endswith("/") else "")

    # 2) look for 'dags/' anywhere in path (very common)
    idx = p.find(os.sep + "dags" + os.sep)
    if idx != -1:
        rel = p[idx+1:]  # include 'dags/...'
        rel_dir = os.path.dirname(rel)
        return rel_dir + ("/" if not rel_dir.endswith("/") else "")

    # 3) look for '/gcs/' marker anywhere
    idx = p.find(os.sep + "gcs" + os.sep)
    if idx != -1:
        rel = p[idx+1:]
        rel_dir = os.path.dirname(rel)
        return rel_dir + ("/" if not rel_dir.endswith("/") else "")

    # 4) As a last resort return the script directory (relative local path)
    rel_dir = os.path.dirname(p)
    return rel_dir + ("/" if not rel_dir.endswith("/") else "")


def list_dynamic_gcs_folder_structure() -> Dict[str, Optional[List[str]]]:
    """
    Detect bucket/prefix when possible. If GCS_BUCKET not present, attempt to infer
    prefix (the path after bucket) from the current script path using heuristics.
    Returns a dict with keys: bucket, prefix, files, folders.
    """
    local_path = os.path.abspath(__file__)
    print(f"Local path: {local_path}")

    bucket_name = os.getenv("GCS_BUCKET")  # Composer provides this
    prefix = None

    if bucket_name:
        # Composer environment: convert local path to prefix after /home/airflow/gcs/
        prefix = detect_gcs_prefix_from_path(local_path)
        print(f"Detected Composer bucket: {bucket_name}")
        print(f"Detected prefix: {prefix}")
    else:
        # Not Composer: attempt to infer prefix from path (dags/... or script dir)
        print("GCS_BUCKET env var not found — attempting to infer prefix from local path.")
        prefix = detect_gcs_prefix_from_path(local_path)
        print(f"Inferred prefix (path after bucket if this were in GCS): {prefix}")
        # bucket_name remains None — user asked only for path after bucket so that's fine.

    # If we have a bucket name, list from GCS; otherwise just return the inferred prefix
    files = []
    folders = []

    if bucket_name:
        client = storage.Client()
        iterator = client.list_blobs(bucket_name, prefix=prefix or "", delimiter='/')

        # gather files
        for blob in iterator:
            files.append(blob.name)

        # gather immediate subfolders
        folders = list(iterator.prefixes)

        print("\nFiles directly under:", prefix)
        for f in files:
            print("  ", f)

        print("\nSubfolders under:", prefix)
        for sf in folders:
            print("  ", sf)
    else:
        # No bucket — we only provide the inferred prefix/path (no GCS listing)
        print("\nNo bucket available. Skipping GCS list. Returning inferred prefix only.")

    return {
        "bucket": bucket_name,
        "prefix": prefix,
        "files": files,
        "folders": folders
    }


if __name__ == "__main__":
    result = list_dynamic_gcs_folder_structure()
    print("\nResult:", result)


*******************
    parser = argparse.ArgumentParser(description="Worker script on Dataproc")
    parser.add_argument("--app-env", help="Application environment", default=None)
    args = parser.parse_args()

    # Prefer the passed-in arg; if missing, fallback to an environment variable or default.
    app_env = args.app_env or os.getenv("APP_ENV", "dev")

    print("Resolved APP_ENV:", app_env)
