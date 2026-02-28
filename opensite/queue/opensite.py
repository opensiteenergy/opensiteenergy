import json
import os
import threading
import uvicorn
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import logging
import multiprocessing
import time
from datetime import datetime, timezone, timedelta
from typing import List
from pathlib import Path
from opensite.logging.opensite import OpenSiteLogger
from opensite.model.node import Node
from opensite.constants import OpenSiteConstants
from opensite.install.opensite import OpenSiteInstaller
from opensite.download.opensite import OpenSiteDownloader
from opensite.processing.unzip import OpenSiteUnzipper
from opensite.processing.concatenate import OpenSiteConcatenator
from opensite.processing.run import OpenSiteRunner
from opensite.processing.importer import OpenSiteImporter
from opensite.processing.spatial import OpenSiteSpatial
from opensite.output.opensite import OpenSiteOutput
from colorama import Fore, Style, init

init()

def shutdown_requested():
    """Checks whether shutdown has been requested"""

    if os.path.exists("stop.signal"): return True
    return False

class OpenSiteQueue:

    DOWNLOAD_RETRY_INTERVAL         = 30
    DOWNLOAD_RETRY_TOTALATTEMPTS    = 10
    SHUTDOWN_TIME_DELAY             = 10

    def __init__(self, graph, max_workers=None, log_level=logging.DEBUG, overwrite=False, stop_event=None):
        self.graph = graph
        self.action_groups = self.graph.get_action_groups()
        self.terminal_status = self.graph.get_terminal_status()
        self.log_level = log_level
        self.overwrite = overwrite
        self.log = OpenSiteLogger("OpenSiteQueue", self.log_level)
        self.stop_event = stop_event
        self.process_started = None
        self.shutdownstatus = None

        # Resource Scaling
        self.cpus = os.cpu_count() or 1
        if self.cpus > 1: self.cpus -= 1
        self.cpu_workers = max_workers or self.cpus
        self.io_workers = self.cpu_workers * 4  # Higher concurrency for network/disk
        
        self.graph.log.info(f"Processor ready. CPU Workers: {self.cpu_workers}, I/O Workers: {self.io_workers}")

    def shutdown(self, io_exec, cpu_exec):
        """Clean exit point for the application."""

        if self.stop_event: self.stop_event.set()
        cpu_exec.shutdown(wait=False, cancel_futures=True)
        io_exec.shutdown(wait=False, cancel_futures=True)

    def check_shutdown(self):
        """Checks whether to shutdown"""

        if not self.stop_event: return False

        if self.stop_event.is_set():
            self.log.warning("[OpenSiteQueue] Stop has been requested so quitting worker loop")
            return True
        
        return False
    
    def _fetch_filesizes_parallel(self, nodes: List[Node]):
        """Helper to fetch remote sizes for a list of nodes in parallel."""
        # Only check nodes that are downloads and don't have a cached size
        nodes_to_check = [
            n for n in nodes 
            if n.action == 'download' and not hasattr(n, '_remote_size')
        ]
        
        if not nodes_to_check:
            return

        def fetch_task(node):
            self.log.info(f"Getting file size: {node.input}")
            downloader = OpenSiteDownloader()
            # This calls the logic we just fixed with 'identity' headers
            node._remote_size = downloader.get_remote_size(node)
            self.log.info(f"File size {node._remote_size}: {node.input}")

        # Max 20 threads is usually a sweet spot for network I/O 
        # without triggering rate limits on most servers
        with ThreadPoolExecutor(max_workers=20) as executor:
            # list() forces the main thread to wait for all results
            list(executor.map(fetch_task, nodes_to_check))
        
        # Now the code only reaches this line once all threads are done
        self.log.info("All file sizes fetched.")

    def _fetch_db_sizes(self, nodes: List[Node]):
        """Fetch database table sizes for all preprocess nodes in one batch query."""
        
        # Filter nodes that need a DB size check
        nodes_to_check = [
            n for n in nodes 
            if n.action == 'preprocess' and not hasattr(n, '_db_table_size')
        ]
        
        if not nodes_to_check:
            return

        # Extract the table names we need to look for
        table_names = [n.table_name for n in nodes_to_check if hasattr(n, 'table_name')]
        
        if not table_names:
            return

        self.log.info(f"Fetching database sizes for {len(table_names)} tables...")

        # Single query to get sizes for all tables in the list
        query = """
            SELECT relname, pg_total_relation_size(c.oid) 
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' 
            AND relname = ANY(%s);
        """

        conn = self.postgis.pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (table_names,))
                # Create a lookup dictionary: { 'table_name': size_in_bytes }
                size_map = {row[0]: row[1] for row in cursor.fetchall()}
                
                # Assign sizes back to nodes
                for node in nodes_to_check:
                    node._db_table_size = size_map.get(node.table_name, 0)
                    if node._db_table_size > 0:
                        self.log.info(f"Table {node.table_name} size: {node._db_table_size} bytes")
                    
        finally:
            self.postgis.pool.putconn(conn)

        self.log.info("All database table sizes fetched.")
        
    def set_node_status(self, node, status):
        """
        Adds necessary node log entries depending on status
        """

        node.status = status
        log_keys = {k for d in node.log for k in d.keys()}
        if status == 'processing':
            if 'started' not in log_keys:
                node.log.append({'started': datetime.now(timezone.utc).isoformat()})
        if status == 'processed':
            if 'completed' not in log_keys:
                node.log.append({'completed': datetime.now(timezone.utc).isoformat()})
            # Add duration - we assume started is [0] and completed is [1]
            if 'started' in log_keys:
                node.log.append({'duration': str(datetime.fromisoformat(node.log[1]['completed']) - datetime.fromisoformat(node.log[0]['started']))})

        return node

    def sync_global_status(self, node_urn: str, status: str):
        """
        Updates target node and all its global 'clones' to specified status
        """
        node = self.graph.find_node_by_urn(node_urn)
        g_urn = node.global_urn

        # Update the specific node
        node = self.set_node_status(node, status)
        
        # Sync all clones sharing the same global_urn
        if g_urn:
            clones = self.graph.find_nodes_by_props({'global_urn': g_urn})
            for c_dict in clones:
                # Skip the one we just updated
                if c_dict['urn'] == node_urn:
                    continue
                c_node = self.graph.find_node_by_urn(c_dict['urn'])
                c_node = self.set_node_status(c_node, status)

    @staticmethod
    def process_cpu_task(args):
        """
        Static wrapper for ProcessPoolExecutor. 
        Handles Amalgamate, Import, Buffer, and Run.
        """
        urn, \
        global_urn, \
        name, \
        title, \
        node_type, \
        format, \
        input, \
        action, \
        output, \
        custom_properties, \
        log_level, \
        overwrite, \
        shared_lock, \
        shared_metadata = args
         
        if shutdown_requested(): return urn, 'cancelled'

        logger = OpenSiteLogger("OpenSiteQueue", log_level, shared_lock)

        logger.info(f"[CPU:{action}] {name}")

        node = Node(    urn=urn, \
                        global_urn=global_urn, \
                        name=name, \
                        title=title, \
                        node_type=node_type, \
                        format=format, \
                        input=input, \
                        action=action, \
                        output=output, \
                        custom_properties=custom_properties)

        try:

            if action == 'run':
                runner = OpenSiteRunner(node, log_level, shared_lock, shared_metadata)
                success = runner.run()

            if action == 'import':
                importer = OpenSiteImporter(node, log_level, shared_lock, shared_metadata)
                success = importer.run()

            if action == 'buffer':
                spatializer = OpenSiteSpatial(node, log_level, shared_lock, shared_metadata)
                success = spatializer.buffer()

            if action == 'invert':
                spatializer = OpenSiteSpatial(node, log_level, shared_lock, shared_metadata)
                success = spatializer.invert()

            if action == 'distance':
                spatializer = OpenSiteSpatial(node, log_level, shared_lock, shared_metadata)
                success = spatializer.distance()

            if action == 'preprocess':
                spatializer = OpenSiteSpatial(node, log_level, shared_lock, shared_metadata)
                success = spatializer.preprocess()

            if action == 'amalgamate':
                spatializer = OpenSiteSpatial(node, log_level, shared_lock, shared_metadata)
                success = spatializer.amalgamate()

            if action == 'postprocess':
                spatializer = OpenSiteSpatial(node, log_level, shared_lock, shared_metadata)
                success = spatializer.postprocess()

            if action == 'clip':
                spatializer = OpenSiteSpatial(node, log_level, shared_lock, shared_metadata)
                success = spatializer.clip()

            if action == 'output':
                spatializer = OpenSiteOutput(node, log_level, overwrite, shared_lock, shared_metadata)
                success = spatializer.run()

            if success: return urn, 'processed'
            else: return urn, 'failed'

        except Exception:
            return urn, 'failed'
        
    def process_io_task(self, node: Node, log_level, shared_lock, shared_metadata):
        """
        Standard method for ThreadPoolExecutor.
        Handles Download, Unzip, and Concatenate.
        """

        self.graph.log.info(f"[I/O:{node.action}] {node.name}")

        if shutdown_requested(): return 'cancelled'

        # Use shared_metadata for concatenator as needs access to cross-process variables

        try:
            success = False
            
            if node.action == 'install':
                installer = OpenSiteInstaller(node, log_level, shared_lock, shared_metadata)
                success = installer.run()

            elif node.action == 'download':
                downloader = OpenSiteDownloader(log_level, shared_lock, shared_metadata)
                # As lowest-level downloads are important to efficient parallelism
                # we retry failed downloads
                for attempts in range(self.DOWNLOAD_RETRY_TOTALATTEMPTS):
                    success = downloader.get(node)
                    if success: break
                    if shutdown_requested(): return 'cancelled'
                    self.graph.log.info(f"[I/O:{node.action}] {node.name} Download attempt {attempts + 1} failed - retrying after {self.DOWNLOAD_RETRY_INTERVAL} seconds")
                    time.sleep(self.DOWNLOAD_RETRY_INTERVAL)

            elif node.action == 'unzip':
                unzipper = OpenSiteUnzipper(node, log_level, shared_lock, shared_metadata)
                success = unzipper.run()

            elif node.action == 'concatenate':
                concatenator = OpenSiteConcatenator(node, log_level, shared_lock, shared_metadata)
                success = concatenator.run()

            if success: return 'processed'
            else: return 'failed'

        except Exception as e:

            return 'failed'

    def run(self, preview=False):
        """
        Main orchestration loop. Uses a continuous sweep to pipeline
        I/O and CPU tasks simultaneously.
        """
        self.graph.log.info(f"Starting orchestration with {self.io_workers} I/O threads and {self.cpu_workers} CPU processes.")
        
        if os.path.exists("stop.signal"): os.remove("stop.signal")

        # Track active futures: {future: urn}
        active_tasks = {}
        
        # Use a Manager for shared locks across processes
        manager = multiprocessing.Manager()
        shared_lock = manager.Lock()
        shared_metadata = manager.dict()

        # Track number of unfinished nodes on every run
        number_unfinished = None

        # Keep executors open for the duration of the run to allow pipelining
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.io_workers) as io_exec, \
             concurrent.futures.ProcessPoolExecutor(max_workers=self.cpu_workers) as cpu_exec:
            
            unfinishednodes = None

            while True:

                # Check whether loop is due to be shutdown
                if self.check_shutdown(): 
                    self.shutdown(io_exec, cpu_exec)
                    self.log.warning("[OpenSiteQueue] Quitting main worker loop")
                    return

                # 1. Get nodes that are ready to run (Dependencies met)
                ready_nodes = self.get_runnable_nodes(actions=None, checksizes=True)
                
                # Filter out nodes that are already currently in flight
                new_nodes = [n for n in ready_nodes if n.urn not in active_tasks.values()]

                # Submit new tasks to the appropriate executor
                for node in new_nodes:
                    # If runnable node has no action, automatically process it
                    if not node.action: 
                        node = self.set_node_status(node, 'processed')
                    else:
                        node = self.set_node_status(node, 'processing')
                    if node.global_urn: self.sync_global_status(node.urn, node.status)

                    self.graph.generate_graph_preview()
        
                    if node.action in self.action_groups['io_bound']:
                        future = io_exec.submit(self.process_io_task, node, self.log_level, shared_lock, shared_metadata)
                        active_tasks[future] = node.urn
                        self.graph.log.debug(f"Submitted I/O task: {node.name}")
                        
                    elif node.action in self.action_groups['cpu_bound']:
                        # Prepare the task args for the Process pool
                        task_args = (
                            node.urn,
                            node.global_urn,
                            node.name,
                            node.title,
                            node.node_type,
                            node.format,
                            node.input,
                            node.action,
                            node.output,
                            node.custom_properties,
                            self.log_level,
                            self.overwrite,
                            shared_lock,
                            shared_metadata,
                        )
                        future = cpu_exec.submit(self.process_cpu_task, task_args)
                        active_tasks[future] = node.urn
                        self.graph.log.debug(f"Submitted CPU task: {node.name}")

                # If no tasks are running and nothing is ready, check for completion or stalls
                if not active_tasks:
                    unfinished = [n for n in self.graph.find_nodes_by_props() 
                                 if n.get('status') not in ['processed', 'failed']]
                    
                    if not unfinished:
                        self.graph.log.info(f"{Fore.GREEN}{'='*60}{Style.RESET_ALL}")
                        self.graph.log.info(f"{Fore.GREEN}{'*'*19} PROCESSING COMPLETE {'*'*20}{Style.RESET_ALL}")
                        self.graph.log.info(f"{Fore.GREEN}{'='*60}{Style.RESET_ALL}")
                        return True
                    else:
                        if unfinishednodes == len(unfinished):
                            self.graph.log.warning(f"Queue stalled. {len(unfinished)} nodes unfinished")
                            return False
                        
                    unfinishednodes = len(unfinished)

                # Wait for at least one task to complete
                # This is the "Pipelining Engine" - it yields as soon as any task finishes
                done, _ = concurrent.futures.wait(
                    active_tasks.keys(), 
                    timeout=1.0, # Brief timeout to allow periodic "Ready Node" re-scanning
                    return_when=concurrent.futures.FIRST_COMPLETED
                )

                # Process completed tasks and update the graph
                for future in done:
                    urn = active_tasks.pop(future)
                    try:
                        # result for CPU tasks is (urn, status), for IO tasks usually just status
                        result = future.result()
                        # Normalize status extraction
                        status = result[1] if isinstance(result, tuple) else result
                        
                        # # Reset any 'failed' nodes to 'unprocessed' so we keep retrying
                        # if status == 'failed': status = 'unprocessed'

                        self.sync_global_status(urn, status)
                        
                        # Generate preview to show incremental progress
                        self.graph.generate_graph_preview()
                        
                    except Exception as e:
                        self.graph.log.error(f"Task for URN {urn} generated an exception: {e}")
                        self.sync_global_status(urn, "failed")

                # Tiny sleep to prevent high CPU usage on the main thread
                time.sleep(0.05)
    
    def get_runnable_nodes(self, actions=None, checksizes=True) -> List[Node]:
        """
        Finds nodes ready for execution. 
        Ensures only one node per global_urn is added to the batch.
        """
        runnable = []
        seen_global_urns = set()
        
        # Get all node dictionaries from the graph
        node_dicts = self.graph.find_nodes_by_props({})

        for d in node_dicts:
            node = self.graph.find_node_by_urn(d['urn'])
            g_urn = node.global_urn

            # Skip if already finished or already in this batch
            if node.status in self.terminal_status or g_urn in seen_global_urns:
                continue

            # Filter by action if specified
            if actions and node.action not in actions:
                continue

            if g_urn is None:
                children = getattr(node, 'children', [])
                if all(child.status == 'processed' for child in children):
                    runnable.append(node)
                continue

            # Find all nodes in the graph that share this same global_urn
            clones = [
                self.graph.find_node_by_urn(n['urn']) 
                for n in self.graph.find_nodes_by_props({'global_urn': g_urn})
            ]

            # A node is only runnable if EVERY child of EVERY clone is 'processed'
            all_clones_ready = True
            for clone in clones:
                children = getattr(clone, 'children', [])
                if not all(child.status == 'processed' for child in children):
                    all_clones_ready = False
                    break
            
            if all_clones_ready:
                runnable.append(node)
                if g_urn:
                    seen_global_urns.add(g_urn)

        # Define the sort key (which now uses the cached values)
        def get_priority_weight(node: Node):
            is_download = (node.action == 'download')
            is_import = (node.action == 'import')
            is_db_size_dependent = (node.action in ['preprocess', 'buffer'])

            action_weight = 0 if is_download else 1
            
            try:
                format_weight = OpenSiteConstants.DOWNLOADS_PRIORITY.index(node.format)
            except (ValueError, AttributeError):
                format_weight = len(OpenSiteConstants.DOWNLOADS_PRIORITY) + 1
                
            # Determine which size to use
            size_val = 0
            if is_download:
                # Use the cached remote file size
                size_val = getattr(node, '_remote_size', 0)
            elif is_import:
                if node.format == OpenSiteConstants.OSM_YML_FORMAT:
                    # If OSM import, difficult to know exact size of 
                    # dataset until imported - so use size of parent OSM file
                    file_path = Path(OpenSiteConstants.OSM_DOWNLOAD_FOLDER) / os.path.basename(node.custom_properties['osm'])
                else:
                    file_path = Path(OpenSiteConstants.DOWNLOAD_FOLDER) / node.input
                if file_path.exists():
                    size_val = file_path.stat().st_size
            elif is_db_size_dependent:
                # Use the database table size
                # Assuming you've stored the result of pg_total_relation_size on the node
                size_val = getattr(node, '_db_table_size', 0)

            size_weight = -size_val if size_val and size_val > 0 else 0

            return (action_weight, format_weight, size_weight)

        # Order by size (filesize or database size) using pre-fetch request (may not always work)
        if checksizes:
            self._fetch_filesizes_parallel(runnable)
            self._fetch_db_sizes(runnable)
            runnable.sort(key=get_priority_weight)

        return runnable