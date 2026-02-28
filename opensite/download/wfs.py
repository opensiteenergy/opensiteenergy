import time
import logging
import urllib.parse
import xmltodict
import requests
import pandas as pd
import geopandas as gpd
from pathlib import Path
from requests import Request
from owslib.wfs import WebFeatureService

from opensite.constants import OpenSiteConstants
from opensite.download.base import DownloadBase
from opensite.logging.opensite import OpenSiteLogger

class WFSDownloader(DownloadBase):
    def __init__(self, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        self.log_level = log_level
        self.shared_lock = shared_lock
        self.shared_metadata = shared_metadata if shared_metadata is not None else {}
        self.log = OpenSiteLogger("WFSDownloader", log_level, shared_lock)
        self.base_path = OpenSiteConstants.DOWNLOAD_FOLDER

        # Scottish Gov / AWS required User-Agent
        self.headers = {'User-Agent': OpenSiteConstants.WFS_USER_AGENT}

    def guess_wfs_layer(self, wfs):
        """Finds a boundary/boundaries layer if no specific layer is requested."""
        layers = list(wfs.contents)
        for layer_id in layers:
            title = getattr(wfs[layer_id], 'title', '').lower()
            if 'boundary' in title or 'boundaries' in title:
                return layer_id
        return layers[0] if layers else None

    def get(self, url, target_file, subfolder=None, force=False, layer_name=None) -> bool:
        """
        Gets WFS content from url
        """

        # Path Resolution
        final_dir = Path(self.base_path)
        if subfolder:
            final_dir = final_dir / subfolder
        
        output_file = (final_dir / target_file).resolve()
        temp_output_file = output_file.parent / f"tmp-{output_file.name}"

        # Idempotency check
        if output_file.exists() and not force:
            self.log.info(f"Skipping: {target_file} already exists.")
            return True

        self.log.info(f"Starting WFS download: {target_file}")

        self.ensure_output_dir(output_file)

        try:
            # 2. Connect to WFS
            try:
                wfs = WebFeatureService(url=url, version='2.0.0', headers=self.headers)
                wfs_version = '2.0.0'
            except Exception:
                wfs = WebFeatureService(url=url, headers=self.headers)
                wfs_version = wfs.version

            # 3. Determine Endpoint and Layer
            getfeature_url = url
            methods = wfs.getOperationByName('GetFeature').methods
            for method in methods:
                if method['type'].lower() == 'get':
                    getfeature_url = method['url']
                    break

            layer = layer_name if layer_name else self.guess_wfs_layer(wfs)
            if not layer:
                self.log.error(f"No layers found in WFS: {url}")
                return False

            # Normalize CRS
            crs = "EPSG:4326"
            if hasattr(wfs[layer], 'crsOptions') and wfs[layer].crsOptions:
                crs = str(wfs[layer].crsOptions[0]).replace('urn:ogc:def:crs:', '').replace('::', ':').replace('OGC:1.3:CRS84', 'EPSG:4326')

            # 4. Hits Query for Pagination Planning
            params = {
                'SERVICE': 'WFS',
                'VERSION': wfs_version,
                'REQUEST': 'GetFeature',
                'RESULTTYPE': 'hits',
                'TYPENAME': layer
            }
            hit_url = getfeature_url.split('?')[0] + '?' + urllib.parse.urlencode(params)
            response = requests.get(hit_url, headers=self.headers)
            result = xmltodict.parse(response.text)

            root_key = 'wfs:FeatureCollection'
            total_records = int(result[root_key]['@numberMatched'])
            batch_size = int(result[root_key].get('@numberReturned', total_records))
            if batch_size == 0: batch_size = total_records

            self.log.info(f"Downloading WFS: {target_file} [{total_records} records] using layer {layer}")

            # 5. Paginated Download Loop
            dataframe, start_index, records_downloaded = None, 0, 0

            while records_downloaded < total_records:
                if self.shutdown_requested(): 
                    self.log.warning("Shutdown requested, quitting early")
                    return False

                records_to_download = min(batch_size, total_records - records_downloaded)
                
                wfs_request_url = Request('GET', getfeature_url, headers=self.headers, params={
                    'service': 'WFS',
                    'version': wfs_version,
                    'request': 'GetFeature',
                    'typename': layer,
                    'count': records_to_download,
                    'startIndex': start_index,
                }).prepare().url

                try:
                    df_batch = gpd.read_file(wfs_request_url).set_crs(crs)
                    
                    if dataframe is None:
                        dataframe = df_batch
                    else:
                        dataframe = pd.concat([dataframe, df_batch])

                    records_downloaded += records_to_download
                    start_index += records_to_download

                    # Progress log
                    percent = (records_downloaded / total_records) * 100
                    self.log.info(f"Progress [{target_file}]: {percent:3.1f}% ({records_downloaded}/{total_records})")

                except Exception as e:
                    self.log.warning(f"Batch failed {url} ({start_index}). Retrying with reduced count. Error: {e}")
                    records_to_download -= 1
                    total_records -= 1
                    if records_to_download <= 0: break

            # 6. Finalize Atomic Move
            if dataframe is not None:
                # WFS data is spatial; saving as GPKG is best for standardizing
                dataframe.to_file(temp_output_file, driver="GPKG")
                temp_output_file.rename(output_file)
                self.log.info(f"Successfully finalized: {target_file}")
                return True
            
            return False

        except Exception as e:
            self.log.error(f"WFS download failed for {target_file}: {e}")
            if temp_output_file.exists():
                temp_output_file.unlink()
            return False