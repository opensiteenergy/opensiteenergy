import json
import logging
from ckanapi import RemoteCKAN
from opensite.logging.base import LoggingBase

class CKANBase:
    FORMATS = []

    def __init__(self, url: str, apikey: str = None, log_level=logging.INFO):
        self.url = url
        self.apikey = apikey
        self._raw_cache = [] 
        self.log = LoggingBase("CKANBase", log_level)

    def load(self, target_group='data-explorer'):
        """
        The master entry point. 
        Connects to CKAN and hydrates the cache. 
        Fails loudly if any step fails.
        """

        self.log.info(f"Initializing CKAN connection: {self.url}")
        try:
            remote = RemoteCKAN(self.url, apikey=self.apikey)
            
            self.log.info(f"Fetching package names from group: {target_group}...")
            package_names = remote.action.package_list(id=target_group)
            
            self._raw_cache = {}
            for name in package_names:
                self.log.debug(f"Hydrating: {name}")
                self._raw_cache[name] = remote.action.package_show(id=name)
            
            self.log.info(f"Success. Cached {len(self._raw_cache)} packages.")
            
        except Exception as e:
            self.log.error(f"CRITICAL CKAN ERROR: {e}")
            raise SystemExit(f"Terminating: Could not load data from {self.url}")

    def query(self, formats=None):
        """
        Filters the local cache and organizes datasets by group.
        Captures group titles to allow graph-node title syncing.
        """
        target_formats = formats if formats is not None else self.FORMATS
        results = {}

        for name, pkg in self._raw_cache.items():
            matching_resources = [
                res for res in pkg.get('resources', []) 
                if res.get('format') in target_formats
            ]

            if matching_resources:
                pkg_title = pkg.get('title', name)
                groups = pkg.get('groups', [])
                
                # Extract group info safely
                if groups:
                    group_name = groups[0].get('name')
                    group_title = groups[0].get('title', group_name)
                else:
                    group_name = 'default'
                    group_title = 'Default Group'

                # Initialize group structure with both name and title
                if group_name not in results:
                    results[group_name] = {
                        'group_title': group_title,
                        'datasets': []
                    }

                results[group_name]['datasets'].append({
                    'package_name': name,
                    'title': pkg_title,
                    'url': f"{self.url}/dataset/{name}", # Added for completeness
                    'resources': matching_resources,
                    'extras': pkg['extras']
                })

        return results

