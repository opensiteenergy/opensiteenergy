import logging
from pathlib import Path
from opensite.constants import OpenSiteConstants
from opensite.model.node import Node
from opensite.download.base import DownloadBase
from opensite.logging.opensite import OpenSiteLogger
from opensite.download.arcgis import ArcGISDownloader
from opensite.download.wfs import WFSDownloader

class OpenSiteDownloader(DownloadBase):

    DOWNLOAD_INTERVAL_TIME = OpenSiteConstants.DOWNLOAD_INTERVAL_TIME

    def __init__(self, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        super().__init__(log_level=log_level, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteDownloader", log_level, shared_lock)
        self.base_path = OpenSiteConstants.DOWNLOAD_FOLDER

    def _handle_node_input(self, node: Node, filename: str = None, subfolder: str = "", force: bool = False):
        """
        Routes the node by checking node.format against the 
        CKAN_DEFAULT_DOWNLOADER whitelist.
        """
        # Ensure we are checking against a consistent case
        current_format = node.format
        target_file = filename or node.output

        # Check if the format is in our default list
        if current_format in OpenSiteConstants.CKAN_DEFAULT_DOWNLOADER:
            self.log.info(f"Using default downloader for {current_format}: {node.name}")
            force = (current_format in OpenSiteConstants.ALWAYS_DOWNLOAD)
            return self.get(node.input, target_file, subfolder, force)

        # Map specialized formats (e.g., ArcGIS, KML) to their handlers
        format_map = {
            'ArcGIS GeoServices REST API': ArcGISDownloader,
            'WFS': WFSDownloader,
        }

        # Use case-insensitive lookup for the specialized map
        handler_class = format_map.get(current_format)

        if handler_class:
            self.log.info(f"Routing {node.name} to {current_format} handler.")
            handler = handler_class(self.log_level, self.shared_lock)
            return handler.get(node.input, target_file, subfolder, force)

        # Fallback for anything else
        self.log.info(f"Format '{current_format}' not explicitly handled. Falling back to default.")
        return self.get_url(node.input, target_file, subfolder, force)

    def get_remote_size(self, node) -> int:
        """
        Overrides base size check. Only attempts network request if 
        the node is configured for external URL downloading.
        """
        # (i) Check if this node type/action is compatible with the CKAN downloader
        # We check the action or node_type against your constant
        if node.format not in OpenSiteConstants.CKAN_DEFAULT_DOWNLOADER:
            # If it's not a URL download (e.g., it's a local file op), skip size check
            return None

        # Verify we actually have a URL to check
        url = getattr(node, 'input', None)
        if not url or not isinstance(url, str) or not url.startswith('http'):
            return None

        # (ii) If yes, call the parent class method to get the actual bytes
        remote_size = super().get_remote_size(url)

        return remote_size