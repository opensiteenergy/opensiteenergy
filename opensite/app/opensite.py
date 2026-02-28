import docker
import os
import secrets
import shutil
import signal
import subprocess
import sys
import threading
import time
import traceback
import uvicorn
import yaml
from fastapi import FastAPI, Response
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from pathlib import Path
from opensite.app.routes import OpenSiteRouter
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger
from opensite.cli.opensite import OpenSiteCLI
from opensite.ckan.opensite import OpenSiteCKAN
from opensite.model.graph.opensite import OpenSiteGraph
from opensite.queue.opensite import OpenSiteQueue
from opensite.postgis.opensite import OpenSitePostGIS
from opensite.processing.spatial import OpenSiteSpatial
from colorama import Fore, Style, init
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware

init()

class GlobalNoCacheMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        
        path = request.url.path.lower()
        no_cache_extensions = (".json", ".mbtiles")
        is_index = path in ["/", "/index.html"]
        
        if path.endswith(no_cache_extensions) or is_index:
            # no-store: Do not save to disk
            # no-cache: Revalidate with server every time
            # max-age=0: Expire immediately
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
            
        return response
        
class IgnoreDevToolsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        # List of annoying dev-only paths to silence
        noise_paths = [
            ".well-known/appspecific/com.chrome.devtools.json",
            ".css.map",
            ".js.map"
        ]
        
        if any(path in request.url.path for path in noise_paths):
            # Return 204 No Content: tells the browser "I heard you, but there's nothing here"
            # This prevents the 404 log entry in FastAPI
            return Response(status_code=204)
            
        return await call_next(request)
    
class ForceDownloadMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/outputfiles"):
            # This tells the browser: "Don't open this, save it!"
            response.headers["Content-Disposition"] = "attachment"
        return response

@asynccontextmanager
async def lifespan(app: FastAPI):
    orchestrator = app.state.orchestrator
    orchestrator.setup()
    yield
    try:
        orchestrator.log.info("Uvicorn signaling shutdown...")
        orchestrator.stop()
    except Exception as e:
        # We use print or a basic logger here because the orchestrator's 
        # logger might already be shutting down.
        print(f"Error during lifespan shutdown: {e}")

class OpenSiteApplication:
    def __init__(self, log_level=OpenSiteConstants.LOGGING_LEVEL):
        self.log_level = os.getenv("OPENSITE_LOG_LEVEL", log_level)
        self.log = OpenSiteLogger("OpenSiteApplication", self.log_level)
        self.app = FastAPI()
        self.app.state.orchestrator = self
        self.ensure_secret_key()
        self.app.add_middleware(SessionMiddleware, secret_key=os.getenv("OPENSITE_SECRET_KEY"))
        self.app.add_middleware(GlobalNoCacheMiddleware)
        self.app.add_middleware(IgnoreDevToolsMiddleware)
        self.app.add_middleware(ForceDownloadMiddleware)
        self.default_config = "defaults.yml"
        self.server = None
        self.serverport = None
        self.should_exit = False
        self.processing_start = None
        self.processing_stop = None
        self.queue = None
        self.graph = None
        self.processing_thread = None
        self.build_running = False

    def setup(self):
        self.app.state.log = self.log
        self.stop_event = threading.Event()
        folder_app = Path('opensite') / 'app'
        folder_static = str(folder_app / 'static')
        folder_templates = str(folder_app / 'templates')
        folder_layers = str(OpenSiteConstants.OUTPUT_LAYERS_FOLDER)

        self.init_environment()
        self._cleanup_signals()

        self.app.mount("/static", StaticFiles(directory=folder_static), name="static")
        self.app.mount("/outputfiles", StaticFiles(directory=folder_layers), name="outputfiles")
        self.app.state.templates = Jinja2Templates(directory=folder_templates)
        self.app.state.processing_start = self.processing_start
        self.app.include_router(OpenSiteRouter)

        self.log.info(f"{Fore.GREEN}{'='*60}{Style.RESET_ALL}")
        self.log.info(f"{Fore.GREEN}{'*'*17} APPLICATION INITIALIZED {'*'*18}{Style.RESET_ALL}")
        self.log.info(f"{Fore.GREEN}{'='*60}{Style.RESET_ALL}")

    def _cleanup_signals(self):
        """Removes any stale signal files from previous runs."""
        signal_file = Path("stop.signal")
        if signal_file.exists():
            try:
                signal_file.unlink()
                self.log.info("Cleared stale stop.signal file.")
            except Exception as e:
                self.log.error(f"Failed to clear stop.signal: {e}")

    def build_start(self, build_config=None):
        """Triggers the long-running process in its own thread."""
        if self.processing_thread and self.processing_thread.is_alive():
            self.log.warning("Build already in progress!")
            return False

        self.log.info("[OpenSiteApplication] Starting build...")
        self.stop_event.clear()
        self.build_running = True
        self.processing_thread = threading.Thread(target=self.build_run, args=(build_config,))
        self.processing_thread.start()
        return True

    def build_run(self, config_json: dict):
        """Run main build"""
        try:

            self.processing_start = None
            self.processing_stop = None

            # Only use logging file for each individual build
            # if Path(OpenSiteConstants.LOGGING_FILE).exists():
            #     os.remove(OpenSiteConstants.LOGGING_FILE)

            if config_json['purgeall']:
                self.log.info("Build config triggering purgeall and reinitialisation of environment")
                self.purgeall()
                self.init_environment()

            with open(self.default_config, 'r') as f:
                default_config_values = yaml.safe_load(f) or {}

            tileserver_used = ('web' in default_config_values['outputformats'])
            
            ckan = OpenSiteCKAN(default_config_values['ckan'])
            ckan.load()
            self.graph = OpenSiteGraph( None, \
                                        default_config_values['outputformats'], \
                                        config_json['clip'], \
                                        default_config_values['snapgrid'], \
                                        log_level=self.log_level)
            self.graph.add_yamls(config_json['sites'])
            self.graph.update_metadata(ckan)
            self.graph.explode()

            # Run processing queue
            self.queue = OpenSiteQueue(self.graph, log_level=self.log_level, overwrite=False, stop_event=self.stop_event)
            self.processing_start = time.time()
            success = self.queue.run()

            # If processing completed successfully, restart tileserver
            if success: 
                if tileserver_used: self.restart_tileserver()

            self.log.info("Processing queue has completed")
            self.build_running = False
            self.processing_stop = time.time()

        except Exception as e:
            self.log.error(f"Pipeline failed: {e}")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line_number = exc_tb.tb_lineno
            full_stack = traceback.format_exc()
            self.log.error(f"Error on line {line_number}: {e}")
            self.log.debug(full_stack)


    def build_nodes(self, last_index=0):
        """Gets latest state of processing nodes"""

        new_logs = []
        current_index = last_index

        if not self.graph: return {}

        data = self.graph.to_json()
        data['process_started'] = self.processing_start
        data['process_stopped'] = self.processing_stop

        if os.path.exists(OpenSiteConstants.LOGGING_FILE):
            with open(OpenSiteConstants.LOGGING_FILE, "r") as f:
                # Skip lines we've already seen
                lines = f.readlines()
                new_lines = lines[last_index:]
                
                for line in new_lines:
                    parts = line.split(" ", 1)
                    timestamp = parts[0] if len(parts) > 0 else ""
                    content = parts[1] if len(parts) > 1 else line
                    
                    new_logs.append({"time": timestamp, "msg": content.strip()})
                
                current_index = len(lines)
        data['logs'] = new_logs
        data['next_index'] = current_index

        return data

    def build_stop(self):
        """Sends a stop signal to the active worker."""
        self.log.info("[OpenSiteApplication] Stop signal received. Signalling worker...")
        self.stop_event.set()

        Path("stop.signal").write_text("STOP")

        postgis = OpenSitePostGIS(log_level=self.log_level)
        postgis.cancel_own_queries()

        self.build_running = False
        self.processing_start = None
        self.processing_stop = None

    def ensure_secret_key(self):
        env_path = ".env"
        key_name = "OPENSITE_SECRET_KEY"
        
        # 1. Check if it's already in the current environment
        if os.getenv(key_name):
            return

        # 2. Generate a secure 32-byte hex key
        new_key = secrets.token_hex(32)
        self.log.info(f"No {key_name} found. Generating a new one...")

        # 3. Append to .env file (creating it if it doesn't exist)
        try:
            # Check if the file exists and ends with a newline
            prefix = ""
            if os.path.exists(env_path):
                with open(env_path, "r") as f:
                    content = f.read()
                    if content and not content.endswith("\n"):
                        prefix = "\n"
            
            with open(env_path, "a") as f:
                f.write(f"{prefix}{key_name}={new_key}\n")
                
            # 4. Inject it into the current process so it's available immediately
            os.environ[key_name] = new_key
            self.log.info(f"Successfully saved {key_name} to {env_path}")
            
        except Exception as e:
            self.log.error(f"Failed to save secret key: {e}")
            

    def _handle_exit(self, signum, frame):
        """Unified handler for SIGINT (Ctrl-C) and SIGTERM (Systemd)"""
        # signum 2 = SIGINT, signum 15 = SIGTERM
        self.log.info(f"[!] Signal {signum} received. Initiating graceful shutdown...")

        self.should_exit = True
        if self.server:
            self.server.should_exit = True

    def start(self, port=8000):
        """Start headless server with signal awareness"""
        # Register signals for both manual and system-level termination
        signal.signal(signal.SIGINT, self._handle_exit)
        signal.signal(signal.SIGTERM, self._handle_exit)
        self.serverport = port

        self.log.info(f"Starting headless server on port {self.serverport}")
        
        config = uvicorn.Config(
            app=self.app, 
            host="0.0.0.0", 
            port=self.serverport, 
            log_level="debug",
            access_log=False,
        )
        self.server = uvicorn.Server(config)
        self.server.install_signal_handlers = False

        # Run the server in a thread to keep main thread open for signal polling
        server_thread = threading.Thread(target=self.server.run, daemon=True)
        server_thread.start()

        self._run_main_loop()

    def _run_main_loop(self):
        """Main loop that keeps the process alive until termination"""
        self.log.info(f"Main loop active on port {self.serverport}. Press Ctrl-C to stop")

        try:
            while not self.should_exit:
                # This keeps the main thread alive to receive OS signals
                time.sleep(0.5)
        finally:
            self.stop()

    def stop(self):
        """Master shutdown command for both the server and any active builds."""
        self.log.info(f"Shutdown initiated for port {self.serverport}")

        # Always signal server and main loop to stop regardless of whether build is running
        if self.server:
            self.server.should_exit = True
        self.should_exit = True

        # Only perform build cleanup if build is active and we haven't already signalled stop
        if self.stop_event.is_set() or not self.build_running:
            return

        self.build_stop()

    def show_elapsed_time(self):
        """Shows elapsed time since object was created, ie. when process started"""

        processing_time = time.time() - self.processing_start
        processing_time_minutes = round(processing_time / 60, 1)
        processing_time_hours = round(processing_time / (60 * 60), 1)
        time_text = f"{processing_time_minutes} minutes ({processing_time_hours} hours) to complete"
        self.log.info(f"{Fore.YELLOW}Completed processing - {time_text}{Style.RESET_ALL}")
        print("")

    def init_environment(self):
        """Creates required system folders defined in constants."""
        folders = OpenSiteConstants.ALL_FOLDERS
        for folder in folders:
            if not folder.exists():
                folder.mkdir(parents=True, exist_ok=True)

        spatial = OpenSiteSpatial(None)
        spatial.create_processing_grid()
        spatial.create_output_grid()
        spatial.create_processing_grid_buffered_edges()

    def delete_folder(self, folder_path):
        """Deletes the specified directory and all its contents."""
        try:
            # We use ignore_errors=False to ensure we catch permission issues
            shutil.rmtree(folder_path)
            self.log.info(f"Successfully deleted: {folder_path}")
            return True
        except FileNotFoundError:
            self.log.warning(f"The folder {folder_path} does not exist.")
        except PermissionError:
            self.log.error(f"Error: Permission denied when trying to delete {folder_path}.")
        except Exception as e:
            self.log.error(f"An unexpected error occurred: {e}")

        return False
    
    def purgeall(self):
        """Purge all download files and opensite database tables"""

        self.purgedownloads()
        self.purgeoutputs()
        self.purgeinstalls()
        self.purgetileserver()
        self.purgedb()

        self.log.info("[purgeall] completed")
        return True
    
    def show_success_message(self, outputformats):
        """Gets final message text"""

        final_message = f"""
{Fore.MAGENTA + Style.BRIGHT}{'='*60}\n{'*'*10} OPEN SITE ENERGY BUILD PROCESS COMPLETE {'*'*9}\n{'='*60}{Style.RESET_ALL}
\nFinal layers created at:\n\n{Fore.CYAN + Style.BRIGHT}{OpenSiteConstants.OUTPUT_LAYERS_FOLDER}{Style.RESET_ALL}\n\n\n"""

        if 'web' in outputformats:
            final_message += f"""To view constraint layers as map, enter:\n\n{Fore.CYAN + Style.BRIGHT}./viewbuild.sh{Style.RESET_ALL}\n\n\n"""
        
        if 'qgis' in outputformats:
            final_message += f"""QGIS file created at:\n\n{Fore.CYAN + Style.BRIGHT}{str(Path(OpenSiteConstants.OUTPUT_FOLDER) / OpenSiteConstants.OPENSITEENERGY_SHORTNAME)}.qgs{Style.RESET_ALL}\n\n"""

        print(final_message)

    def purgetileserver(self):
        """Purge all tileserver files"""

        tileserver_folder = Path(OpenSiteConstants.TILESERVER_OUTPUT_FOLDER).resolve()

        self.delete_folder(tileserver_folder)
        self.log.info("[purgetileserver] completed")

        return True

    def purgeinstalls(self):
        """Purge all install files"""

        installs_folder = Path(OpenSiteConstants.INSTALL_FOLDER).resolve()

        self.delete_folder(installs_folder)
        self.log.info("[purgeinstalls] completed")

        return True

    def purgedownloads(self):
        """Purge all download files"""

        downloads_folder = Path(OpenSiteConstants.DOWNLOAD_FOLDER).resolve()

        self.delete_folder(downloads_folder)
        self.log.info("[purgedownloads] completed")

        return True

    def purgeoutputs(self):
        """Purge all output files"""

        outputs_folder = Path(OpenSiteConstants.OUTPUT_FOLDER).resolve()

        self.delete_folder(outputs_folder)
        self.log.info("[purgeoutput] completed")

        return True

    def purgedb(self):
        """Purge all opensite database tables"""

        postgis = OpenSitePostGIS()
        postgis.purge_database()
        self.log.info("[purgedb] completed")

        return True

    def early_check_area(self, areas):
        """
        If boundaries table exist, check area is valid
        Returns True if OSM boundaries table doesn't exist yet - 
        a further check will be run once it has been created 
        """

        postgis = OpenSitePostGIS()
        if postgis.table_exists(OpenSiteConstants.OPENSITE_OSMBOUNDARIES):
            for area in areas:
                country = postgis.get_country_from_area(area)
                if country is None: return False
            return True
        return True

    def run(self):
        """
        Runs OpenSite application
        """

        # Initialise CLI and check for key switches that may require user interaction
        cli = OpenSiteCLI(log_level=self.log_level) 

        if not cli.get_server():
            tables_purged = False
            if cli.purgedb:
                print(f"\n{Fore.RED}{Style.BRIGHT}{'='*60}")
                print(f"WARNING: You are about to delete all opensite tables")
                print(f"This includes registry, branch, and all spatial data tables.")
                print(f"{'='*60}{Style.RESET_ALL}\n")
                
                confirm = input(f"Type {Style.BRIGHT}'yes'{Style.RESET_ALL} to delete all OpenSite data: ").strip().lower()
                if confirm == 'yes':
                    self.purgedb()
                    tables_purged = True
                else:
                    self.log.warning("Purge aborted. No tables were harmed.")

            if cli.purgeall:
                print(f"\n{Fore.RED}{Style.BRIGHT}{'='*60}")
                print(f"WARNING: You are about to delete all downloads and opensite tables")
                print(f"This includes registry, branch, and all spatial data tables.")
                print(f"{'='*60}{Style.RESET_ALL}\n")
                
                confirm = input(f"Type {Style.BRIGHT}'yes'{Style.RESET_ALL} to delete all downloads and OpenSite data: ").strip().lower()
                if confirm == 'yes':
                    self.purgeall()
                    tables_purged = True
                else:
                    self.log.warning("Purge aborted. No files or tables were harmed.")

            if tables_purged: self.init_environment()

        if cli.get_server():
            self.start(port=cli.get_server())
            return
        
        # Attempt to check clipping area (if set) is valid
        if cli.get_clip():
            if not self.early_check_area(cli.get_clip()):
                self.log.error(f"At least one area in '{cli.get_clip()}' not found in boundary database, clipping will not be possible.")
                self.log.error(f"Please select the name of a different clipping area.")
                self.log.error(f"******** ABORTING ********")
                exit()

        # Initialize CKAN open data repository to use throughout
        # CKAN may or may not be used to provide site YML configuration
        ckan = OpenSiteCKAN(cli.get_current_value('ckan'))
        site_ymls = ckan.download_sites(cli.get_sites())

        tileserver_used = ('web' in cli.get_outputformats())
        
        # Initialize data model for session
        graph = OpenSiteGraph(  cli.get_overrides(), \
                                cli.get_outputformats(), \
                                cli.get_clip(), \
                                cli.get_snapgrid(), \
                                log_level=self.log_level)
        graph.add_yamls(site_ymls)
        graph.update_metadata(ckan)

        # Generate all required processing steps
        graph.explode()

        # Generate initial processing graph
        graph.generate_graph_preview()

        # If not '--graphonly', run processing queue
        queue = OpenSiteQueue(graph, log_level=self.log_level, overwrite=cli.get_overwrite())

        # Main processing loop
        success = False
        if not cli.get_graphonly(): 
            
            self.processing_start = time.time()
            success = queue.run(preview=cli.get_preview())
            self.processing_stop = time.time()

            # Show elapsed time at end
            self.show_elapsed_time()

            if success:

                # Wait till very end before copying main web index page to FastAPI templates folder
                if tileserver_used: self.restart_tileserver()

                self.show_success_message(cli.get_outputformats())

    def staging2live_tileserver(self):
        """Makes tileserver staging files live"""

        shutil.copy('tileserver/index.html', str(Path('opensite') / "app" / "templates" / "index.html"))

        if OpenSiteConstants.TILESERVER_LIVE_FOLDER.exists() and OpenSiteConstants.TILESERVER_LIVE_FOLDER.is_dir():
            OpenSiteConstants.TILESERVER_LIVE_FOLDER.rename(OpenSiteConstants.TILESERVER_DEPRECATED_FOLDER)
        if OpenSiteConstants.TILESERVER_OUTPUT_FOLDER.exists() and OpenSiteConstants.TILESERVER_OUTPUT_FOLDER.is_dir():
            OpenSiteConstants.TILESERVER_OUTPUT_FOLDER.rename(OpenSiteConstants.TILESERVER_LIVE_FOLDER)
        if OpenSiteConstants.TILESERVER_DEPRECATED_FOLDER.exists() and OpenSiteConstants.TILESERVER_DEPRECATED_FOLDER.is_dir():
            shutil.rmtree(OpenSiteConstants.TILESERVER_DEPRECATED_FOLDER)

    def is_running_in_docker(self):
        """Checks if current script is running inside Docker container"""
        path = '/proc/self/cgroup'
        return (
            os.path.exists('/.dockerenv') or
            (os.path.isfile(path) and any('docker' in line for line in open(path)))
        )

    def restart_tileserver(self):
        """Copies main tileserver page and triggers restart of tileserver"""

        # Switch over staging and live tileserver folders here
        self.staging2live_tileserver()

        self.log.info("Triggering tileserver-gl to restart so it loads new config and mbtiles")
        # If running as server, create 'RESTARTSERVICES' file to trigger systemd restart of tileserver-gl
        Path("RESTARTSERVICES").write_text("RESTART")

        try:

            if self.is_running_in_docker():
                self.log.info("Detected Docker context, restarting tileserver-gl via SDK")
                client = docker.from_env()
                container = client.containers.get("opensiteenergy-tileserver")
                container.restart()
                self.log.info("Restart signal sent successfully")
            else:
                self.log.info("Restarting tileserver-gl via bash script")
                subprocess.run(["./local-tileserver.sh"], check=True, capture_output=True)

        except subprocess.CalledProcessError as e:
            self.log.error(f"Problem restarting tileserver-gl {e}")

    def shutdown(self, message="Process complete"):
        """Clean exit point for the application."""

        self.log.info(message)
