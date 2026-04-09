from __future__ import annotations

import argparse
import errno
import logging
import multiprocessing
import os
import re
import socket
import subprocess
import sys
import time
import uuid
from multiprocessing import Lock, Process, Queue
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, NoReturn
from urllib.parse import urlparse

import paramiko
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import yaml

logger = logging.getLogger(__name__)

# Constants
UUID_STR = str(uuid.uuid4())
CONFIG_FILE = os.getenv("DCACHE_CONFIG", "dcache.yaml")
HOSTNAME = socket.getfqdn()
SSH_HOST = "fndca"
SSH_PORT = 24223
SSH_USER = "enstore"
KRB5CCNAME = f"/tmp/krb5cc_root.migration-{UUID_STR}"


def get_shell(
    host: str = SSH_HOST,
    port: int = SSH_PORT,
    user: str = SSH_USER
) -> paramiko.SSHClient:
    """Create and return an admin shell connection.

    Args:
        host: SSH host to connect to
        port: SSH port number
        user: SSH username

    Returns:
        Connected SSH client

    Raises:
        paramiko.SSHException: If connection fails
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=host,
        port=port,
        username=user,
        gss_auth=True,
        gss_kex=True
    )
    return ssh
