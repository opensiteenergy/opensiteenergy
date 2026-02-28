import json
import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from opensite.constants import OpenSiteConstants
from opensite.install.base import InstallBase
from opensite.logging.opensite import OpenSiteLogger

class OpenSiteTileserver(InstallBase):

    def __init__(self, node, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteTileserver", log_level, shared_lock)
        self.base_path = OpenSiteConstants.DOWNLOAD_FOLDER
        self.output_path = OpenSiteConstants.OUTPUT_FOLDER

    def update_json_file_paths(self, file_path, prefix=""):
        """
        Updates the source field in a json file with a path_prefix then saves updated json back to original file
        If field already has path_prefix, do nothing
        """

        if not os.path.exists(file_path):
            self.log.error(f"File not found: {file_path}")
            return False

        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
        except Exception as e:
            self.log.error(f"Failed to load JSON: {e}")
            return False

        def walk_and_prefix(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if key == 'source' and isinstance(value, str):
                        if not value.startswith(prefix):
                            obj[key] = f"{prefix}{value}"
                    else:
                        walk_and_prefix(value)
            elif isinstance(obj, list):
                for item in obj:
                    walk_and_prefix(item)

        # Process the data
        walk_and_prefix(data)

        # Atomic write-back
        try:
            fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(file_path))
            with os.fdopen(fd, 'w') as tmp:
                json.dump(data, tmp, indent=4) # Added indent for readability
            
            os.replace(temp_path, file_path)
            self.log.info(f"Successfully updated 'source' fields in {file_path}")
            return True
        except Exception as e:
            if 'temp_path' in locals() and os.path.exists(temp_path):
                os.remove(temp_path)
            self.log.error(f"Failed to save JSON: {e}")
            return False


    def run(self):
        """
        Installs tileserver-related core files and generates overall basemap
        """

        self.log.info("Installing tileserver files")

        coastline_folder            = OpenSiteConstants.BASEMAP_FOLDER_DEST / 'coastline'
        landcover_folder            = OpenSiteConstants.BASEMAP_FOLDER_DEST / 'landcover'
        basename_pbf                = os.path.basename(self.node.input)
        basename_mbtiles            = basename_pbf.replace(".osm.pbf", ".mbtiles")
        basemap_pbf                 = OpenSiteConstants.OSM_DOWNLOAD_FOLDER / basename_pbf
        basemap_mbtiles             = OpenSiteConstants.OUTPUT_BASEMAP_FOLDER / basename_mbtiles
        basemap_tileserver_mbtiles  = OpenSiteConstants.TILESERVER_DATA_FOLDER / basename_mbtiles
        basemap_tmp_mbtiles         = OpenSiteConstants.OUTPUT_BASEMAP_FOLDER / f"tmp-{basename_mbtiles}"
        fonts_tmp_folder            = OpenSiteConstants.TILESERVER_OUTPUT_FOLDER / 'tmp-fonts'
        fonts_tmp_fonts_folder      = fonts_tmp_folder / 'fonts'

        if not OpenSiteConstants.OUTPUT_BASEMAP_FOLDER.exists():
            self.log.info("Creating basemap folder")
            OpenSiteConstants.OUTPUT_BASEMAP_FOLDER.mkdir(parents=True, exist_ok=True)

        if not OpenSiteConstants.TILESERVER_DATA_FOLDER.exists():
            self.log.info("Creating tileserver data folder")
            OpenSiteConstants.TILESERVER_DATA_FOLDER.mkdir(parents=True, exist_ok=True)

        if basemap_tmp_mbtiles.exists(): basemap_tmp_mbtiles.unlink()
        if fonts_tmp_folder.exists(): shutil.rmtree(fonts_tmp_folder)

        if not basemap_pbf.exists():
            self.log.error(f"{basename_pbf} missing from OSM downloads folder - required to install tileserver files")
            return False
        
        if not OpenSiteConstants.BASEMAP_FOLDER_DEST.exists():

            self.log.info("Copying tileserver basemap folder to build directory")

            try:
                shutil.copytree(str(OpenSiteConstants.BASEMAP_FOLDER_SRC), str(OpenSiteConstants.BASEMAP_FOLDER_DEST))
                self.log.info(f"Successfully copied {str(OpenSiteConstants.BASEMAP_FOLDER_SRC)} to {str(OpenSiteConstants.BASEMAP_FOLDER_DEST)}")
            except FileExistsError:
                self.log.error("Error: Destination directory already exists.")
                return False
            except Exception as e:
                self.log.error(f"An error occurred: {e}")
                return False

        if not OpenSiteConstants.TILESERVER_SPRITES_DEST.exists():

            self.log.info("Copying tileserver sprites folder to tileserver output directory")

            try:
                shutil.copytree(str(OpenSiteConstants.TILESERVER_SPRITES_SRC), str(OpenSiteConstants.TILESERVER_SPRITES_DEST))
                self.log.info(f"Successfully copied {str(OpenSiteConstants.TILESERVER_SPRITES_SRC)} to {str(OpenSiteConstants.TILESERVER_SPRITES_DEST)}")
            except FileExistsError:
                self.log.error("Error: Destination directory already exists.")
                return False
            except Exception as e:
                self.log.error(f"An error occurred: {e}")
                return False

        if coastline_folder.exists() and landcover_folder.exists():
            self.log.info("Coastline and landcover folders exists, skipping download")
        else:

            self.log.info("Downloading coastline and landcover data")
            cmd = [f"./{OpenSiteConstants.SHELL_COASTLINE_LANDCOVER}"]

            try:

                # Execute shell command to download coastline and landcover data for whole earth
                subprocess.run(cmd, cwd=str(OpenSiteConstants.BASEMAP_FOLDER_DEST), capture_output=True, text=True, check=True)
                self.log.info(f"Ran {OpenSiteConstants.SHELL_COASTLINE_LANDCOVER} to generate coastline and landcover data")

            except subprocess.CalledProcessError as e:
                self.log.error(f"subprocess error when running '{OpenSiteConstants.SHELL_COASTLINE_LANDCOVER}' {' '.join(cmd)} {e.stderr}")
                return False

            except (Exception) as e:
                self.log.error(f"General error: {e}")
                return False

        if basemap_mbtiles.exists():

            self.log.info(f"{os.path.basename(basemap_mbtiles)} already exists, skipping creation")

        else:

            self.log.info(f"Generating {os.path.basename(basemap_mbtiles)}...")

            try:

                self.log.info("Generating global coastline mbtiles as initial map")

                # Prefix paths in config file to use correct location
                self.update_json_file_paths(str(OpenSiteConstants.TILEMAKER_COASTLINE_CONFIG), f"{str(OpenSiteConstants.BASEMAP_FOLDER_DEST)}/")

                cmd = ([
                    "tilemaker", 
                    "--input",      str(basemap_pbf), 
                    "--output",     str(basemap_tmp_mbtiles), 
                    "--bbox",       OpenSiteConstants.TILEMAKER_BBOX_UK, 
                    "--process",    str(OpenSiteConstants.TILEMAKER_COASTLINE_PROCESS), 
                    "--config",     str(OpenSiteConstants.TILEMAKER_COASTLINE_CONFIG) 
                ])

                subprocess.run(cmd, capture_output=True, text=True, check=True)

            except subprocess.CalledProcessError as e:
                self.log.error(f"Subprocess error when generating global coastline mbtiles: {' '.join(cmd)} {e.stderr}")
                return False
            except (Exception) as e:
                self.log.error(f"General error when generating global coastline mbtiles: {e}")
                return False

            try:

                self.log.info(f"Merging {basename_pbf} into global coastline mbtiles to form final map")

                cmd = ([
                    "tilemaker", 
                    "--input",      str(basemap_pbf), 
                    "--output",     str(basemap_tmp_mbtiles), 
                    "--merge", 
                    "--process",    str(OpenSiteConstants.TILEMAKER_OMT_PROCESS), 
                    "--config",     str(OpenSiteConstants.TILEMAKER_OMT_CONFIG)
                ])

                subprocess.run(cmd, capture_output=True, text=True, check=True)

            except subprocess.CalledProcessError as e:
                self.log.error(f"Subprocess error when merging {basename_pbf} into global coastline mbtiles: {' '.join(cmd)} {e.stderr}")
                return False
            except (Exception) as e:
                self.log.error(f"General error when merging {basename_pbf} into global coastline mbtiles: {e}")
                return False

            try:
                self.log.info(f"Copying {os.path.basename(basemap_tmp_mbtiles)} to {os.path.basename(basemap_mbtiles)}")
                os.replace(str(basemap_tmp_mbtiles), str(basemap_mbtiles))
            except (Exception) as e:
                self.log.error(f"General error when copying {os.path.basename(basemap_tmp_mbtiles)} to {os.path.basename(basemap_mbtiles)}: {e}")
                return False

        if not basemap_tileserver_mbtiles.exists():

            self.log.info(f"Tileserver basemap missing, copying {str(basemap_mbtiles)} to {str(basemap_tileserver_mbtiles)}")

            shutil.copy(basemap_mbtiles, basemap_tileserver_mbtiles)


        if OpenSiteConstants.TILESERVER_FONTS_FOLDER.exists():

            self.log.info(f"{str(OpenSiteConstants.TILESERVER_FONTS_FOLDER)} already exists, skipping creation")

        else:
            
            try:

                self.log.info(f"Git cloning from {OpenSiteConstants.TILESERVER_FONTS_GITHUB}")

                cmd = (["git", "clone", OpenSiteConstants.TILESERVER_FONTS_GITHUB, os.path.basename(fonts_tmp_folder)])

                subprocess.run(cmd, cwd=str(OpenSiteConstants.TILESERVER_OUTPUT_FOLDER), capture_output=True, text=True, check=True)
                shutil.move(str(fonts_tmp_fonts_folder), str(OpenSiteConstants.TILESERVER_FONTS_FOLDER))
                shutil.rmtree(fonts_tmp_folder)

            except subprocess.CalledProcessError as e:
                self.log.error(f"Subprocess error when git cloning from {OpenSiteConstants.TILESERVER_FONTS_GITHUB}: {' '.join(cmd)} {e.stderr}")
                return False
            except (Exception) as e:
                self.log.error(f"General error when git cloning from {OpenSiteConstants.TILESERVER_FONTS_GITHUB}: {e}")
                return False

        return True