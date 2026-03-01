import os
import logging
import yaml
from pathlib import Path
from opensite.logging.base import LoggingBase

class ProcessBase:
    def __init__(self, node, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        self.node = node
        self.log_level = log_level
        self.log = LoggingBase("ProcessBase", log_level, shared_lock)
        self.base_path = ""
        self.shared_lock = shared_lock
        self.shared_metadata = shared_metadata if shared_metadata is not None else {}

    def get_top_variable(self, file_path):
        """Get topmost variable from yaml file - needed to determine osm-export-tool layer name"""
        with open(file_path, 'r') as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)
            
        if data and isinstance(data, dict):
            # Use next(iter()) to efficiently get the first key
            return next(iter(data))
        return None

    def run(self):
        """Main entry point for the process."""
        raise NotImplementedError("Subclasses must implement run()")

    def ensure_output_dir(self, file_path):
        """Utility to make sure the destination exists."""
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    def get_full_path(self, path_str: str) -> Path:
        """Helper to resolve paths against the base_path."""
        path = Path(path_str)
        if not path.is_absolute() and self.base_path:
            return (Path(self.base_path) / path).resolve()
        return path.resolve()
    