import argparse
import json
import logging
import logging.config
import multiprocessing
import os
import re
import socket
import sys
import time
import traceback
from multiprocessing import Lock, Process, Queue
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, NoReturn
from urllib.parse import urlparse

import yaml

from pydcache.util import admin, kerberos, ostools, psycopg
from pydcache.util import ssh
from pydcache.config import get_config_path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

# Global locks
print_lock = Lock()
kinit_lock = Lock()

CONFIG_FILE = os.getenv("DCACHE_CONFIG")   # None → auto-resolved via get_config_path()
SSH_HOST = "fndca"
SSH_PORT = 24223
SSH_USER = "enstore"
HOSTNAME = socket.getfqdn()

def safe_json_deserializer(v):
    try:
        return json.loads(v.decode('utf-8')) if v else None
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        return None  # Or log the error


class Worker(Process):

    def __init__(
        self,
        queue: Queue,
        config: Dict[str, Any],
        date: str
    ) -> None:
        """Initialize the worker.

        Args:
            queue: Task queue
            config: Configuration dictionary
        """
        super().__init__()
        self.queue = queue
        self.config = config
        self.date = date

    def run(self) -> None:
        """Process tasks from the queue."""
        ssh_conn = None
        try:
            ssh_conn = ssh.get_shell(
                self.config["admin"].get("host", SSH_HOST),
                self.config["admin"].get("port", SSH_PORT),
                self.config["admin"].get("user", SSH_USER)
            )

            for rc_entry in iter(self.queue.get, None):
                self._process_file(ssh_conn, rc_entry)
        except Exception as exc:
            error_string = traceback.format_exc()
            logger.error(f"Worker failed: {error_string}")
            logger.error(f"{self.config}")
        finally:
            for conn in (ssh_conn,):
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

    def _process_file(
        self,
        ssh,
        rc_entry: str
    ) -> None:
        """Process a single rc entry

        Args:
            ssh: SSH connection
            rc_entry: rc entry string
        """
        # 0000C22369BF691D461E9EB4DF3A843C20A1@world-net-*/* m=51 r=0 [rw-stkendca2005-1] [Waiting for stage: rw-stkendca2005-1 10.06 17:40:57] {0,}

        parts = rc_entry.split()
        pnfsid = parts[0].split("@")[0].strip()
        pool = re.sub("[\[-\]]","",parts[3].strip())
        day =  parts[8].strip()
        if day == self.date:
            admin.execute_admin_command(
                ssh,
                f"\\s {pool} rh kill {pnfsid}"
                )

def main() -> None:

    """Main function to process files that are already on tape."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Process files that are already on tape and update their status"
    )

    parser.add_argument(
        "--cpu-count",
        type=int,
        default=10,
        help="Number of worker processes to spawn"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    parser.add_argument(
        "-i", "--instance",
        help='instance',
        default="public_prd",
        metavar="INSTANCE"
    )

    parser.add_argument(
        "-d", "--date",
        help='date to select on in form of MM.DD',
        default="10.06",
        metavar="DATA"
    )

    args = parser.parse_args()

    # Load configuration
    config = {}
    try:
        config_path = get_config_path()
        if config_path.stat().st_mode != 33152:
            logger.error(
                "Config file %s permissions too permissive, should be 0600",
                config_path
            )
            sys.exit(1)

        config = yaml.safe_load(config_path.read_text())
        if not config:
            logger.error("Failed to load configuration from %s", config_path)
            sys.exit(1)

    except (OSError, IOError) as exc:
        if isinstance(exc, FileNotFoundError):
            logger.error("Config file %s does not exist", exc)
        else:
            logger.error("Error reading config file: %s", exc)
        sys.exit(1)
    except yaml.YAMLError as exc:
        logger.error("Error parsing config file: %s", exc)
        sys.exit(1)

    logger.info("Starting processing")

    # Start Kerberos ticket refresh worker
    kinit_worker = kerberos.KinitWorker()
    kinit_worker.start()

    ssh_connection = ssh.get_shell(
        config["admin"].get("host", SSH_HOST),
        config["admin"].get("port", SSH_PORT),
        config["admin"].get("user", SSH_USER)
        )

    rc_ls = admin.execute_admin_command(
        ssh_connection,
        f"\\s PoolManager rc ls"
        )

    ssh_connection.close()

    # Set up worker processes
    queue: Queue = Queue(maxsize=100)
    workers = [
        Worker(queue, config, args.date)
        for _ in range(args.cpu_count)
    ]

    for worker in workers:
        worker.start()

    for line in rc_ls:
        if not line:
            continue
        queue.put(line)

    for _ in range(args.cpu_count):
        queue.put(None)

    for worker in workers:
        worker.join()

    # Clean up
    kinit_worker.stop()


if __name__ == "__main__":
    main()
