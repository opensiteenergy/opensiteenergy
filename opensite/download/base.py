import json
import logging
import os
import requests
import time
import sqlite3
from pathlib import Path
from typing import Union, Any
from opensite.logging.base import LoggingBase
from opensite.model.node import Node

class DownloadBase:
    
    DOWNLOAD_INTERVAL_TIME = 5

    def __init__(self, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        self.log = LoggingBase("DownloadBase", log_level, shared_lock)
        self.log_level = log_level
        self.shared_lock = shared_lock
        self.shared_metadata = shared_metadata if shared_metadata is not None else {}
        self.base_path = ""

    def shutdown_requested(self):
        """Checks whether shutdown has been requested"""

        if os.path.exists("stop.signal"): return True
        return False
    
    def ensure_output_dir(self, file_path):
        """Utility to make sure the destination exists."""
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    def get(self, input_data: Any, filename: str = None, subfolder: str = "", force: bool = False):
        """
        Routes the request based on the type of input_data.
        """

        if isinstance(input_data, Node):
            return self._handle_node_input(input_data, filename, subfolder, force)
        
        if isinstance(input_data, str):
            return self.get_url(input_data, filename, subfolder, force)
        
        return self._handle_non_string_input(input_data, filename, subfolder, force)

    def get_remote_size(self, url: str) -> int:
        """
        Retrieves the file size in bytes using an HTTP HEAD request with 
        identity encoding to force a Content-Length response.
        """
        # Identity tells the server NOT to compress the response, 
        # which often forces it to reveal the true Content-Length.
        headers = {'Accept-Encoding': 'identity'}
        
        try:

            if self.shutdown_requested(): 
                self.log.warning("Shutdown requested, quitting early")
                return None

            # 1. Try HEAD request first
            response = requests.head(
                url, 
                headers=headers, 
                allow_redirects=True, 
                timeout=10
            )
            
            size = response.headers.get('Content-Length')

            # 2. Fallback to GET with stream=True if HEAD is blocked or missing size
            if not size or response.status_code != 200:
                with requests.get(
                    url, 
                    headers=headers, 
                    stream=True, 
                    allow_redirects=True, 
                    timeout=10
                ) as r:
                    size = r.headers.get('Content-Length')
            
            if size:
                size_bytes = int(size)
                # Success!
                return size_bytes
            
            # If we still have no size, it's likely chunked/dynamic
            return -1

        except Exception as e:
            if hasattr(self, 'log'):
                self.log.warning(f"Network error checking size for {url}: {e}")
            return -1
        
    def format_size(self, size_bytes: int) -> str:
        """Helper to convert bytes to human readable string."""
        if size_bytes < 0: return "Unknown size"
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} TB"

    def _handle_node_input(self, node: Node, filename: str, subfolder: str, force: bool):
        """
        Default implementation for a Node: extract its input string and output path.
        """
        url = node.input
        target_file = filename or node.output
        return self.get(url, target_file, subfolder, force)
    
    def _handle_non_string_input(self, input_data: Any, filename: str, subfolder: str, force: bool):
        """
        Default behavior for non-strings. To be overridden by subclasses 
        like OpenSiteDownloader.
        """
        raise NotImplementedError("This downloader does not support non-string inputs.")

    def get_url(self, url: str, filename: str = None, subfolder: str = "", force: bool = False):
        """
        Downloads a file safely using a .tmp shadow file.
        Uses the URL's basename if filename is not provided.
        """

        if not filename:
            filename = os.path.basename(url)
            # Basic cleanup in case of URL parameters like ?v=1.0
            filename = filename.split('?')[0]

        destination = self.base_path / subfolder / filename
        
        if destination.exists() and not force:
            if self.check_download_valid(str(destination)):
                self.log.info(f"{filename}: File exists, skipping")
                return destination
            else:
                return None

        destination.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = destination.with_suffix(destination.suffix + '.tmp')

        if tmp_path.exists(): tmp_path.unlink()

        try:
            self.log.info(f"Downloading: {url}")
            
            # Get total size from headers if available (fallback to our cached _remote_size)
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0))
                
                downloaded = 0
                last_log_time = time.time()
                
                with open(tmp_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if self.shutdown_requested(): 
                            self.log.warning("Shutdown requested, quitting early")
                            return None
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            
                            # Progress reporting - log every DOWNLOAD_INTERVAL_TIME seconds to avoid flooding terminal
                            current_time = time.time()
                            if current_time - last_log_time > self.DOWNLOAD_INTERVAL_TIME:
                                if total_size > 0:
                                    percent = (downloaded / total_size) * 100
                                    mb_done = downloaded / (1024 * 1024)
                                    self.log.info(f"Progress [{filename}]: {percent:.1f}% ({mb_done:.1f} MB)")
                                else:
                                    mb_done = downloaded / (1024 * 1024)
                                    self.log.info(f"Progress [{filename}]: {mb_done:.1f} MB (Unknown total)")
                                
                                last_log_time = current_time

            final_mb = downloaded / (1024 * 1024)
            self.log.info(f"Completed [{filename}]: {final_mb:.1f} MB")
            
            if Path(destination).exists() and not Path(tmp_path).exists():
                self.log.info(f"File {filename} already finalized. Skipping move.")
                return self.check_download_valid(str(destination))
                    
            os.replace(tmp_path, destination)

            while True:
                if Path(destination).exists(): break
                self.log.warning(f"Waiting for {destination} to be created")
                time.sleep(1)

            return self.check_download_valid(str(destination))

        except Exception as e:
            self.log.error(f"Download failed: {e}")
            if tmp_path.exists():
                tmp_path.unlink()
            return None

    def check_geojson_valid(self, file_path):
        """
        Checks whether GeoJSON file is JSON valid
        """

        try:
            json_data = json.load(open(file_path))
            return True
        except:
            self.log.error(f"{os.path.basename(file_path)} is invalid GeoJSON, deleting.")
            os.remove(file_path)
            return False

    def check_gpkg_valid(self, file_path):
        """
        Checks whether GPKG file is valid
        """

        try:
            with sqlite3.connect(file_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='gpkg_contents';")
                if not cursor.fetchone():
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='geometry_columns';")
                    if cursor.fetchone():
                        self.log.warning(f"{os.path.basename(file_path)} is SpatiaLite, not GeoPackage. Skipping.")
                        return file_path
                    
                    self.log.error(f"{os.path.basename(file_path)} is not a valid GeoPackage, deleting.")
                    os.remove(file_path)
                    return None

                # Robust query: only use tables that are guaranteed to exist
                cursor.execute("""
                    SELECT table_name, data_type 
                    FROM gpkg_contents;
                """)
                result = cursor.fetchall()

                if len(result) == 0:
                    self.log.error(f"{os.path.basename(file_path)} has no registered layers, deleting.")
                    os.remove(file_path)
                    return None
                
                return result

        except sqlite3.DatabaseError as e:
            # Check specifically for corruption vs. just a lock
            if "malformed" in str(e):
                self.log.error(f"CORRUPTION: {os.path.basename(file_path)} is malformed. Deleting.")
                os.remove(file_path)
            else:
                self.log.error(f"LOCK/ACCESS ISSUE: {os.path.basename(file_path)} - {e}")
            return None

    def check_download_valid(self, file_path):
        """
        Checks whether file is valid
        Only GPKG currently checked
        """

        if not Path(file_path).exists(): return None

        if file_path.endswith('.gpkg'):
            return self.check_gpkg_valid(file_path)

        return file_path
