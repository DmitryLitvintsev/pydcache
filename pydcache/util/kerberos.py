from __future__ import annotations

import logging
import os
import socket
import sys
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple, NoReturn

from pydcache.util.ostools import execute_command

logger = logging.getLogger(__name__)

# Constants
UUID_STR = str(uuid.uuid4())
HOSTNAME = socket.getfqdn()
KRB5CCNAME = f"/tmp/krb5cc_root.migration-{UUID_STR}"


# Environment setup
os.environ["KRB5CCNAME"] = KRB5CCNAME

def kinit() -> None:
    """Create Kerberos ticket for admin shell access."""
    cmd = f"/usr/bin/kinit -k host/{HOSTNAME}"
    if execute_command(cmd) != 0:
        logger.error(f"Failed to initialize Kerberos ticket {cmd}")
        sys.exit(1)

class KinitWorker(Process):
    """Worker process to maintain Kerberos tickets."""

    def __init__(self) -> None:
        """Initialize the worker."""
        super().__init__()
        self.stop = False
        self.kinit_lock = Lock()

    def run(self) -> None:
        """Periodically refresh Kerberos tickets."""
        while not self.stop:
            with self.kinit_lock:
                kinit()
                time.sleep(14400)  # 4 hours
