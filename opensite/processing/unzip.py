import zipfile
import shutil
import os
import logging
import time
from pathlib import Path
from .base import ProcessBase
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger

class OpenSiteUnzipper(ProcessBase):
    def __init__(self, node, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.base_path = OpenSiteConstants.DOWNLOAD_FOLDER
        self.log = OpenSiteLogger("OpenSiteUnzipper", log_level, shared_lock)

    def run(self) -> bool:
        # Resolve absolute paths
        input_zip = self.get_full_path(self.node.input)
        output_file = self.get_full_path(self.node.output)
        input_zip_basename = input_zip.name

        # Graceful Skipping Logic (Idempotency)
        if output_file.exists():
            # Optional: Check if the zip was modified AFTER the output was created
            # This ensures that if you download a NEWER zip, it will re-unzip.
            zip_mtime = input_zip.stat().st_mtime if input_zip.exists() else 0
            out_mtime = output_file.stat().st_mtime
            
            if out_mtime > zip_mtime:
                self.log.info(f"{output_file.name}: Skipping, already exists and is up to date")
                return True
            else:
                self.log.info(f"{output_file.name}: Overwriting - source zip is newer")

        self.log.info(f"Unzipping {input_zip_basename}")

        if not input_zip.exists():
            self.log.error(f"Source zip not found at: {input_zip}")
            return False

        target_ext = output_file.suffix
        if not target_ext:
            self.log.error(f"Output path {output_file} has no extension.")
            return False

        self.ensure_output_dir(output_file)
        
        # 3. Use output_file name as the work directory
        work_dir = output_file.parent / f"{output_file.name}-tmp"
        
        try:
            # Clean up potential debris from previous failed runs
            if work_dir.exists():
                if work_dir.is_dir():
                    shutil.rmtree(work_dir)
                else:
                    work_dir.unlink()
                
            work_dir.mkdir(parents=True, exist_ok=True)
            
            with zipfile.ZipFile(input_zip, 'r') as zip_ref:
                self.log.info(f"Extracting into directory: {work_dir.name}/")
                zip_ref.extractall(work_dir)

            matches = []
            for p in work_dir.rglob("*"):
                if p.suffix.lower() == target_ext.lower():
                    if target_ext.lower() == ".shp":
                        # Pull the SHP and all its siblings (.shx, .dbf, .prj, etc.)
                        # p.stem is the filename without the extension
                        siblings = list(p.parent.glob(f"{p.stem}.*"))
                        matches.extend(siblings)
                    else:
                        # For GPKG, GeoJSON, etc., just take the single file
                        matches.append(p)

            # 5. The Switcharoo (Renaming everything to match the output_file)
            if target_ext.lower() == ".shp":
                # Get the base name we want (e.g., "amalgamated" if output_file is "amalgamated.shp")
                target_stem = output_file.stem 
                
                for match in matches:
                    # Construct the new filename using the target stem + the original extension
                    # e.g., 'original.dbf' becomes 'amalgamated.dbf'
                    new_filename = f"{target_stem}{match.suffix.lower()}"
                    dest_path = output_file.parent / new_filename
                    
                    # Move and rename simultaneously
                    shutil.move(str(match), str(dest_path))
                
                self.log.info(f"Successfully moved and renamed Shapefile set to {target_stem}.*")
            
            else:
                # Standard logic for single-file formats (GPKG, GeoJSON, etc.)
                source_file = max(matches, key=lambda p: p.stat().st_size)
                shutil.move(str(source_file), str(output_file))
                
            shutil.rmtree(work_dir)
            
            self.log.info(f"Successfully finalized: {output_file.name}")
            return True

        except Exception as e:
            self.log.error(f"Unzip process failed for {input_zip_basename}: {e}")
            return False