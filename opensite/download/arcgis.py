import json
import time
import requests
import logging
from pathlib import Path
from opensite.constants import OpenSiteConstants
from opensite.model.node import Node
from opensite.download.base import DownloadBase
from opensite.logging.opensite import OpenSiteLogger

class ArcGISDownloader(DownloadBase):

    DOWNLOAD_INTERVAL_TIME = OpenSiteConstants.DOWNLOAD_INTERVAL_TIME

    def __init__(self, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        self.log_level = log_level
        self.shared_lock = shared_lock
        self.shared_metadata = shared_metadata if shared_metadata is not None else {}
        self.log = OpenSiteLogger("ArcGISDownloader", log_level, shared_lock)
        self.base_path = OpenSiteConstants.DOWNLOAD_FOLDER

    def get(self, url, target_file, subfolder=None, force=False) -> bool:
        """
        Handler for ArcGIS REST API pagination.
        url: The Feature Layer URL
        target_file: The intended destination filename
        subfolder: Optional sub-directory within the download folder
        force: Whether to overwrite existing files
        """

        # Path Resolution
        final_dir = Path(self.base_path)
        if subfolder:
            final_dir = final_dir / subfolder
        
        output_file = (final_dir / target_file).resolve()
        temp_output_file = output_file.with_suffix(output_file.suffix + '.tmp')

        # Idempotency check
        if output_file.exists() and not force:
            self.log.info(f"Skipping: {target_file} already exists.")
            return True

        self.log.info(f"Starting ArcGIS download: {target_file}")

        self.ensure_output_dir(output_file)

        feature_layer_url = url
        query_url = f"{feature_layer_url.rstrip('/')}/query"

        try:
            # Get Metadata (ObjectIdField)
            params = {"f": 'json'}
            response = self.attempt_post(feature_layer_url, params)
            meta = response.json()

            if 'objectIdField' not in meta:
                self.log.error(f"objectIdField missing from {feature_layer_url}")
                return False

            oid_field = meta['objectIdField']

            # Get Total Count
            count_params = {"f": 'json', "returnCountOnly": 'true', "where": '1=1'}
            response = self.attempt_post(query_url, count_params)
            count_result = response.json()

            if 'count' not in count_result:
                self.log.error(f"'count' missing from {query_url}")
                return False

            total_records = count_result['count']
            self.log.info(f"Downloading ArcGIS: {target_file} [{total_records} records]")

            # Pagination Loop (Object ID Offset)
            records_downloaded = 0
            last_oid = -1
            geojson = {"type": "FeatureCollection", "features": []}

            while records_downloaded < total_records:
                if self.shutdown_requested(): 
                    self.log.warning("Shutdown requested, quitting early")
                    return False

                query_params = {
                    "f": 'geojson',
                    "outFields": '*',
                    "outSR": 4326,
                    "returnGeometry": 'true',
                    "where": f"{oid_field} > {last_oid}",
                    "resultRecordCount": 2000
                }

                response = self.attempt_post(query_url, query_params)
                batch_data = response.json()

                if 'features' not in batch_data:
                    self.log.warning(f"Batch failed for {target_file}, retrying in 5s...")
                    time.sleep(5)
                    continue

                features = batch_data['features']
                if len(features) > 0:
                    geojson['features'].extend(features)
                    records_downloaded += len(features)
                    # Update OID for next chunk
                    last_oid = features[-1]['properties'][oid_field]
                    percent = (records_downloaded / total_records) * 100
                    self.log.info(f"Progress [{target_file}]: {percent:3.1f}% ({records_downloaded}/{total_records})")
                else:
                    # Service might be reporting incorrect count
                    break

            if records_downloaded != total_records:
                self.log.warning(f"Record mismatch for {target_file}: expected {total_records}, got {records_downloaded}")

            # 5. Finalize Atomically
            with open(temp_output_file, 'w', encoding='utf-8') as f:
                json.dump(geojson, f, indent=2)
            
            temp_output_file.rename(output_file)
            return True

        except Exception as e:
            self.log.error(f"ArcGIS download failed for {target_file}: {e}")
            if temp_output_file.exists():
                temp_output_file.unlink()
            return False

    def attempt_post(self, url, params, retries=5):
        for i in range(retries):
            try:
                r = requests.post(url, data=params, timeout=60)
                r.raise_for_status()
                return r
            except Exception as e:
                if i == retries - 1: raise e
                time.sleep(2 ** i)