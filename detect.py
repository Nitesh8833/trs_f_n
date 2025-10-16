import os
import logging
from typing import Optional, Dict

try:
    import requests
except Exception:
    requests = None  # requests may not be installed in some minimal runtimes

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _get_instance_metadata(path: str, timeout: float = 1.0) -> Optional[str]:
    """
    Safe fetch from GCE metadata server. Returns None on failure.
    Requires requests library and network access to metadata server.
    """
    if requests is None:
        return None
    try:
        url = f"http://metadata.google.internal/computeMetadata/v1/{path}"
        headers = {"Metadata-Flavor": "Google"}
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code == 200:
            return resp.text
    except Exception:
        pass
    return None


def detect_composer_bucket() -> Optional[str]:
    """
    Try to detect Composer/GCS bucket name.
    Order of attempts:
      1) Environment variables commonly used (GCS_BUCKET, COMPOSER_BUCKET)
      2) GCE metadata instance attributes Composer might set (several keys tried)
      3) Try infer from composer environment name & region (less reliable)
    Returns None if unable to determine.
    """
    # 1) env vars
    for env_key in ("GCS_BUCKET", "COMPOSER_BUCKET", "COMPOSER_GCS_BUCKET"):
        val = os.getenv(env_key)
        if val:
            logger.debug("Detected bucket from env %s=%s", env_key, val)
            return val

    # 2) metadata server attempts (only works inside GCP)
    # Try some plausible metadata keys that might exist in Composer VMs
    meta_keys = [
        "instance/attributes/composer-bucket",
        "instance/attributes/composer_bucket",
        "instance/attributes/composer-environment",
        "instance/attributes/composer-environment-name",
        "project/project-id",
    ]
    # Attempt direct composer-bucket attr
    bucket = _get_instance_metadata("instance/attributes/composer-bucket")
    if bucket:
        return bucket

    # If composer-environment and region are available, try to reconstruct common bucket name patterns:
    env_name = _get_instance_metadata("instance/attributes/composer-environment")
    region = _get_instance_metadata("instance/attributes/google-compute-default-region") or "us-central1"
    project_id = _get_instance_metadata("project/project-id")
    if env_name and project_id:
        # Common Composer bucket patterns vary. We'll try a conservative guess:
        # * google sometimes creates buckets like: us-central1-<env>-bucket OR <project>-<env>-composer-bucket
        possible = [
            f"{region}-{env_name}-bucket",
            f"{project_id}-{env_name}-composer-bucket",
            f"{project_id}-{env_name}-bucket",
        ]
        # We can't verify existence here without making additional calls; return first guess.
        logger.debug("Inferred bucket candidates: %s", possible)
        return possible[0]

    # unable to detect
    return None


def find_gcs_mount_root() -> Optional[str]:
    """
    Find the local mount root that maps to GCS. This looks for standard Composer mounts
    and falls back to scanning parent folders for 'gcs' in path.
    Returns the mount root path (e.g., '/home/airflow/gcs') or None.
    """
    # Standard Composer mount root
    standard = "/home/airflow/gcs"
    if os.path.exists(standard) and os.path.isdir(standard):
        return standard

    # If not present, try to find any ancestor directory named 'gcs' in cwd or __file__ path
    # (useful if mount is placed elsewhere)
    candidates = []
    # check cwd
    cwd = os.getcwd()
    if "gcs" in cwd.split(os.sep):
        candidates.append(cwd)
    # check script file's ancestors - but we can't reference __file__ here; caller will pass actual path
    # We'll just try some plausible locations
    for path in ("/home/airflow", "/mnt/disks", "/opt"):
        if os.path.exists(path):
            # search up to depth 3 for a 'gcs' dir
            for root, dirs, _ in os.walk(path):
                if os.path.basename(root) == "gcs":
                    return root
                # limit scanning to avoid long walks
                if root.count(os.sep) - path.count(os.sep) > 3:
                    # prune
                    dirs[:] = []
    return None


def get_gs_uri_from_local_path(local_path: str, bucket_name: Optional[str]) -> str:
    """
    Convert a local path (absolute) into a gs:// URI, if possible.
    Recognizes first segment after the 'gcs' mount as {dags,data,plugins} and maps accordingly.
    If bucket_name is unknown, returns a descriptive string indicating failure.
    """
    local_path = os.path.abspath(local_path)
    mount_root = find_gcs_mount_root()
    if not mount_root:
        # nothing matched; check common pattern directly
        if local_path.startswith("/home/airflow/gcs/"):
            mount_root = "/home/airflow/gcs"
        else:
            return f"Local file path (no GCS mount found): {local_path}"

    if not local_path.startswith(mount_root + os.sep) and local_path != mount_root:
        # sometimes mount root may be '/home/airflow/gcs' and file path different; check containment
        # If the file is not under mount root, can't map reliably
        return f"Local file path (not under detected GCS mount {mount_root}): {local_path}"

    # compute the relative path under the mount root
    rel = local_path[len(mount_root) + 1:]  # strip trailing slash
    parts = rel.split(os.sep)
    if not parts:
        return f"Local file path (no relative parts under mount): {local_path}"

    first_segment = parts[0].lower()
    # the typical mapping: /home/airflow/gcs/<dags|data|plugins>/...
    if first_segment in ("dags", "data", "plugins"):
        if not bucket_name:
            # we can still return a partial mapping
            return f"gs://<unknown-bucket>/{first_segment}/" + "/".join(parts[1:])
        tail = "/".join(parts[1:])  # could be empty
        if tail:
            return f"gs://{bucket_name}/{first_segment}/{tail}"
        else:
            return f"gs://{bucket_name}/{first_segment}/"

    # If first segment isn't one of the common ones, map entire rel to bucket root if bucket known
    if bucket_name:
        return f"gs://{bucket_name}/{rel}"
    return f"Local file path (cannot map to gs://): {local_path}"


def detect_environment_from_gs_uri(gs_uri: str) -> str:
    """
    Simple heuristic to pick up 'DEV', 'QA', or 'PROD' from a gs uri.
    Returns uppercase env string or UNKNOWN.
    """
    for env in ("dev", "qa", "prod", "staging", "stage"):
        if env in gs_uri.lower():
            return env.upper()
    return "UNKNOWN"


def get_gsutil_info() -> Dict[str, str]:
    """
    Main convenience function to call from your script.
    Returns a dict with:
      - local_path
      - gs_uri (best effort)
      - detected_bucket (or empty)
      - detected_env (DEV/QA/PROD/UNKNOWN)
      - mount_root (or empty)
    """
    local_path = os.path.abspath(__file__)
    logger.info("Local file path: %s", local_path)

    bucket = detect_composer_bucket()
    if bucket:
        logger.info("Detected bucket name: %s", bucket)
    else:
        logger.info("Could not auto-detect bucket (will use placeholder if needed)")

    gs_uri = get_gs_uri_from_local_path(local_path, bucket)
    mount_root = find_gcs_mount_root() or ""

    env = detect_environment_from_gs_uri(gs_uri)

    info = {
        "local_path": local_path,
        "mount_root": mount_root,
        "detected_bucket": bucket or "",
        "gs_uri": gs_uri,
        "detected_env": env,
    }
    logger.info("GS mapping result: %s", info)
    return info


if __name__ == "__main__":
    info = get_gsutil_info()
    print("GS Info:", info)
