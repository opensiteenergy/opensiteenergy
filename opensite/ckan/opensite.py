import hashlib
import os
import json
import logging
from pathlib import Path
from opensite.constants import OpenSiteConstants
from opensite.ckan.base import CKANBase
from opensite.logging.opensite import OpenSiteLogger
from opensite.download.opensite import OpenSiteDownloader

class OpenSiteCKAN(CKANBase):
    FORMATS = OpenSiteConstants.CKAN_FORMATS

    def __init__(self, url: str, apikey: str = None, log_level=logging.INFO):
        super().__init__(url, apikey, log_level)
        self.log = OpenSiteLogger("OpenSiteCKAN", log_level)

    def get_sites(self):
        """Gets all OpenSite sites from CKAN"""
        self.load()
        return self.query([OpenSiteConstants.SITES_YML_FORMAT])
    
    def download_sites(self, sites: list):
        """
        Downloads YML resources matching the provided site names/slugs.
        """

        self.load()
        # Use the constant for 'Open Site Energy YML'
        results = self.query([OpenSiteConstants.SITES_YML_FORMAT])
        
        downloader = OpenSiteDownloader()
        local_paths = []

        self.log.info(f"Searching for site YMLs: {sites}")

        for group_name, data in results.items():
            for dataset in data.get('datasets', []):
                pkg_slug = dataset.get('package_name')
                for res in dataset.get('resources', []):
                    url = res.get('url')
                    basename = os.path.basename(url)
                    file_slug = os.path.splitext(basename)[0]
                    if pkg_slug in sites or file_slug in sites:
                        self.log.info(f"Match found: '{pkg_slug}' ({basename})")
                        path = downloader.get(url, subfolder=group_name, force=True)
                        if path: local_paths.append(str(path))

        # Sites may be list of local/remote YMLs
        for site in sites:
            if site.endswith('.yml') and os.path.exists(site):
                local_paths.append(site)
            if site.startswith('http://') or site.startswith('https://'):
                tmp_path = downloader.get(site, subfolder=OpenSiteConstants.CACHE_FOLDER, force=True)
                permanent_path = Path(OpenSiteConstants.CACHE_FOLDER) / (hashlib.md5(site.encode('utf-8')).hexdigest() + '.yml')
                os.replace(tmp_path, permanent_path)
                local_paths.append(permanent_path)
                
        return local_paths
