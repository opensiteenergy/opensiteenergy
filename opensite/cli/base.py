import argparse
import logging
from opensite.logging.base import LoggingBase

class BaseCLI:
    def __init__(self, description: str, log_level=logging.INFO):
        self.log = LoggingBase("BaseCLI", log_level)
        self.parser = argparse.ArgumentParser(description=description)
        self.args = None

    def add_standard_args(self):
        """Standard arguments used across the application."""
        pass

    def parse(self):
        """Parse arguments and store them in self.args."""
        self.args = self.parser.parse_args()
        return self.args
    
