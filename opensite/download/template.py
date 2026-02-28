import time
import logging
import urllib.parse
import xmltodict
import requests
from pathlib import Path
from requests import Request

from opensite.constants import OpenSiteConstants
from opensite.download.base import DownloadBase
from opensite.logging.opensite import OpenSiteLogger

class TemplateDownloader(DownloadBase):
    def __init__(self, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        self.log_level = log_level
        self.shared_lock = shared_lock
        self.shared_metadata = shared_metadata if shared_metadata is not None else {}
        self.log = OpenSiteLogger("TemplateDownloader", log_level, shared_lock)
        self.base_path = OpenSiteConstants.DOWNLOAD_FOLDER

    def get(self, url, target_file, subfolder=None, force=False, layer_name=None) -> bool:
        """
        Gets content from url
        """

        self.log.info(f"Getting content from {url}")

        return True