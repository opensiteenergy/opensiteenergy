import logging
import os
import subprocess
from pathlib import Path
from opensite.constants import OpenSiteConstants
from opensite.logging.base import LoggingBase

class OutputBase:
    def __init__(self, node, log_level=logging.INFO, overwrite=False, shared_lock=None, shared_metadata=None):
        self.node = node
        self.log = LoggingBase("OutputBase", log_level, shared_lock)
        self.base_path = ""
        self.log_level = log_level
        self.overwrite = overwrite
        self.shared_lock = shared_lock
        self.shared_metadata = shared_metadata if shared_metadata is not None else {}

    def get_layer_from_file_path(self, filename):
        """
        Converts file name to layer name
        For exported files we try and simplify layer name by using filename
        """

        return Path(filename).stem

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

    def get_crs_default(self):
        """
        Get default CRS as number - for use in PostGIS
        """

        return OpenSiteConstants.CRS_DEFAULT.replace('EPSG:', '')
    
    def get_crs_output(self):
        """
        Get output CRS as number - for use in PostGIS
        """

        return OpenSiteConstants.CRS_OUTPUT.replace('EPSG:', '')

    def convert_file(self, input_path, output_path):
        """
        Converts file format from input_path with format-specific extension to output_path with format-specific extension
        """

        # Base ogr2ogr Command
        cmd = [
            "ogr2ogr",
            output_path,
            input_path
        ]

        self.log.info(f"Converting file {os.path.basename(input_path)} to file {os.path.basename(output_path)}")

        try:
            # Execute shell command
            subprocess.run(cmd, capture_output=True, text=True, check=True)

            self.log.info(f"{os.path.basename(input_path)} converted to {os.path.basename(output_path)}")
            return True
        
        except subprocess.CalledProcessError as e:
            self.log.error(f"ogr2ogr Conversion Error: {e} Subprocess cmd: {cmd}")
            return False

    def convert_node_input_to_output_files(self, node):
        """
        Converts node.input file to node.output file
        """

        shp_extensions      = ['dbf', 'prj', 'shx']
        input               = node.input
        output              = node.output
        temp_output         = 'tmp-' + output
        input_path          = Path(self.base_path) / input
        output_path         = Path(self.base_path) / output
        temp_output_path    = Path(self.base_path) / temp_output

        if temp_output_path.exists(): temp_output_path.unlink()
        # Handle temp secondary files if SHP
        for shp_extension in shp_extensions:
            shp_secondary_file_temp = Path(self.base_path) / temp_output.replace('.shp', f".{shp_extension}")
            if shp_secondary_file_temp.exists(): shp_secondary_file_temp.unlink()

        file_exists = False
        if output_path.exists(): file_exists = True

        # Handle secondary files if SHP
        if output.endswith('.shp'):
            for shp_extension in shp_extensions:
                shp_secondary_file = Path(self.base_path) / output.replace('.shp', f".{shp_extension}")
                if not shp_secondary_file.exists(): file_exists = False

        if not self.node.input:
            self.log.error(f"{node.name} has no input field")
            return False
        
        if not input_path.exists():
            self.log.error(f"Input file {input_path} does not exist, unable to run file conversion")
            return False

        if self.convert_file(input_path, temp_output_path):
            os.replace(temp_output_path, output_path)
            # Handle temp secondary files if SHP
            for shp_extension in shp_extensions:
                shp_secondary_file_temp = Path(self.base_path) / temp_output.replace('.shp', f".{shp_extension}")
                shp_secondary_file_final = Path(self.base_path) / output.replace('.shp', f".{shp_extension}")
                if shp_secondary_file_temp.exists(): os.replace(shp_secondary_file_temp, shp_secondary_file_final)
            return True
        else:
            self.log.error(f"Failed to convert {input} to temp file {temp_output}")
            return False
