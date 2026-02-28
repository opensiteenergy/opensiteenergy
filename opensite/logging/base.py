import logging
import sys
import multiprocessing
from pathlib import Path
from opensite.constants import OpenSiteConstants
from colorama import Fore, Style, init

init()

class ColorFormatter(logging.Formatter):
    """Custom Formatter to add colors to log levels for Terminal only."""
    
    FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    LEVEL_COLORS = {
        logging.DEBUG:      Fore.WHITE,
        logging.INFO:       Fore.BLUE,
        logging.WARNING:    Fore.YELLOW,
        logging.ERROR:      Fore.RED,
    }

    def format(self, record):
        color = self.LEVEL_COLORS.get(record.levelno, Style.RESET_ALL)
        log_fmt = f"{color}{self.FORMAT}{Style.RESET_ALL}"
        # We create a temporary formatter with the colorized string
        formatter = logging.Formatter(log_fmt, datefmt='%Y-%m-%d %H:%M:%S')
        return formatter.format(record)

class LoggingBase:
    # Class-level variables to ensure we only ever create ONE of each handler
    _console_handler = None
    _file_handler = None

    def __init__(self, name: str, level=logging.DEBUG, lock: multiprocessing.Lock = None):
        self.mark_counter = 1
        self.lock = lock
        
        # Use a clean name. Padding is now handled by the Formatter below.
        self.logger = logging.getLogger(name.strip())
        self.logger.setLevel(level)
        self.logger.propagate = False
        
        # Only attach handlers if the logger doesn't have them yet
        if not self.logger.handlers:
            self._setup_shared_handlers()

    def _setup_shared_handlers(self):
        """Initializes and attaches global handlers if they don't exist."""
        
        # --- SHARED CONSOLE HANDLER ---
        if LoggingBase._console_handler is None:
            LoggingBase._console_handler = logging.StreamHandler(sys.stdout)
            LoggingBase._console_handler.setFormatter(ColorFormatter())
        
        # --- SHARED FILE HANDLER ---
        if LoggingBase._file_handler is None:
            # Ensure path is absolute to avoid Uvicorn/Systemd relative path confusion
            log_path = Path(OpenSiteConstants.LOGGING_FILE).resolve()
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            LoggingBase._file_handler = logging.FileHandler(str(log_path))
            
            # Use -21s in the format string to handle the padding automatically
            clean_formatter = logging.Formatter(
                '%(asctime)s [%(levelname)-8s] %(name)-21s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            LoggingBase._file_handler.setFormatter(clean_formatter)

        # Attach the global handlers to this specific logger instance
        self.logger.addHandler(LoggingBase._console_handler)
        self.logger.addHandler(LoggingBase._file_handler)

    def mark(self):
        """General mark function to indicate place in code reached"""
        self.error(f"{self.mark_counter} reached")
        self.mark_counter += 1
        
    def debug(self, msg: str):
        if self.lock:
            with self.lock:
                self.logger.debug(msg)
        else:
            self.logger.debug(msg)

    def info(self, msg: str):
        if self.lock:
            with self.lock:
                self.logger.info(msg)
        else:
            self.logger.info(msg)

    def warning(self, msg: str):
        if self.lock:
            with self.lock:
                self.logger.warning(msg)
        else:
            self.logger.warning(msg)

    def error(self, msg: str):
        if self.lock:
            with self.lock:
                self.logger.error(msg)
        else:
            self.logger.error(msg)

