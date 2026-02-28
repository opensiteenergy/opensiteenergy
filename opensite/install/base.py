import logging
import os
import requests
import time
import sqlite3
from pathlib import Path
from typing import Union, Any
from opensite.logging.base import LoggingBase
from opensite.model.node import Node

class InstallBase:
    
    def __init__(self, node, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        self.node = node
        self.log = LoggingBase("InstallBase", log_level, shared_lock)
        self.base_path = ""
        self.output_path = ""
        self.log_level = log_level
        self.shared_lock = shared_lock
        self.shared_metadata = shared_metadata if shared_metadata is not None else {}

