"""@bruin
name: fetch_gh_archive_to_gcs
type: python
@bruin"""

import requests
import os
import json
import concurrent.futures
import sys
import tempfile
from datetime import datetime
from google.cloud import storage

# 1. Fetch Bruin's Built-in Date Variables
# We slice [:10] to easily convert the ISO timestamp "YYYY-MM-DDTHH:MM:SS" to "YYYY-MM-DD"
bruin_start_date = os.environ.get("BRUIN_START_DATE")
if bruin_start_date:
    execution_date = bruin_start_date[:10] 
else:
    # Fallback for local testing outside of Bruin
    execution_date = datetime.now().strftime("%Y-%m-%d")

# 2. Fetch Bruin's Custom Variables
# Bruin passes all custom variables as a JSON string in 'BRUIN_VARS'
bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))

# Extract variables with fallbacks
GCS_BUCKET_NAME = bruin_vars.get("gcs_bucket", "data-engineering-488412-dev-raw-zone")
THREAD_MULTIPLIER = bruin_vars.get("thread_multiplier", 4)
GCS_PREFIX = "github_events"

# Dynamically calculate threads based on VM cores and your custom variable
VM_CORES = os.cpu_count() or 1
MAX_WORKERS = VM_CORES * THREAD_MULTIPLIER 

def download_and_upload_hour(hour: int) -> tuple[int, bool, str]:
    # ... (Keep the exact same download/upload logic from the previous script) ...
    file_name = f"{execution_date}-{hour}.json.gz"
    url = f"https://data.gharchive.org/{file_name}"
    
    temp_dir = tempfile.gettempdir()
    local_tmp_path = os.path.join(temp_dir, file_name)
    
    try:
        with requests.get(url, stream=True, timeout=60) as response:
            response.raise_for_status()
            with open(local_tmp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        gcs_blob_path = f"{GCS_PREFIX}/{execution_date}/{file_name}" 
        blob = bucket.blob(gcs_blob_path)
        blob.upload_from_filename(local_tmp_path)
        
        os.remove(local_tmp_path)
        return (hour, True, f"[{hour:02d}:00] Success: Uploaded to gs://{GCS_BUCKET_NAME}/{gcs_blob_path}")
        
    except Exception as e:
        if os.path.exists(local_tmp_path):
            os.remove(local_tmp_path)
        return (hour, False, f"[{hour:02d}:00] Failed: {e}")

def main():
    print(f"Executing pipeline for date: {execution_date}")
    print(f"Target Bucket: {GCS_BUCKET_NAME}")
    
    # ... (Keep the exact same ThreadPoolExecutor logic from the previous script) ...
    hours_to_download = list(range(24))
    failed_downloads = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_hour = {executor.submit(download_and_upload_hour, hour): hour for hour in hours_to_download}
        for future in concurrent.futures.as_completed(future_to_hour):
            hour, success, message = future.result()
            print(message)
            if not success:
                failed_downloads.append(hour)

    if failed_downloads:
        print(f"\nPipeline Error: Failed to process hours: {failed_downloads}")
        sys.exit(1)

if __name__ == "__main__":
    main()