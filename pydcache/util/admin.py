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

# Compiled regex pattern
PNFSID_PATTERN = re.compile(r"[A-F0-9]{36}")

def execute_admin_command(ssh: paramiko.SSHClient, cmd: str) -> List[str]:
    """Execute a command on the admin shell.

    Args:
        ssh: Connected SSH client
        cmd: Command to execute

    Returns:
        List of output lines with whitespace stripped
    """
    stdin, stdout, stderr = ssh.exec_command(cmd)
    return [
        line.strip().replace(r"\r", "\n")
        for line in stdout.readlines()
        if line.strip().replace(r"\r", "\n")
    ]


def is_cached(ssh: paramiko.SSHClient, pnfsid: str) -> bool:
    """Check if a file is cached.

    Args:
        ssh: Connected SSH client
        pnfsid: PNFS ID to check

    Returns:
        True if file is cached, False otherwise
    """
    result = execute_admin_command(ssh, f"\\sn cacheinfoof {pnfsid}")
    return bool(result)


def get_locations(ssh: paramiko.SSHClient, pnfsid: str) -> List[str]:
    """Get storage locations for a PNFS ID.

    Args:
        ssh: Connected SSH client
        pnfsid: PNFS ID to check

    Returns:
        List of storage locations
    """
    result = execute_admin_command(ssh, f"\\sn cacheinfoof {pnfsid}")
    if result:
        return result[0].split()
    return []


def mark_precious(ssh: paramiko.SSHClient, pnfsid: str) -> bool:
    """Mark a file as precious on all locations.

    Args:
        ssh: Connected SSH client
        pnfsid: PNFS ID to mark

    Returns:
        True if operation succeeded
    """
    execute_admin_command(ssh, f"\\sl {pnfsid} rep set precious {pnfsid}")
    return True


def mark_precious_on_location(
    ssh: paramiko.SSHClient,
    pool: str,
    pnfsid: str
) -> bool:
    """Mark a file as precious on a specific pool.

    Args:
        ssh: Connected SSH client
        pool: Pool name
        pnfsid: PNFS ID to mark

    Returns:
        True if operation succeeded
    """
    result = execute_admin_command(
        ssh,
        f"\\s {pool} rep set precious {pnfsid}"
    )
    logger.info("Marked precious %s %s %s", pnfsid, pool, result)
    return True


def clear_file_cache_location(
    ssh: paramiko.SSHClient,
    pool: str,
    pnfsid: str
) -> bool:
    """Clear a file's cache location.

    Args:
        ssh: Connected SSH client
        pool: Pool name
        pnfsid: PNFS ID to clear

    Returns:
        True if operation succeeded
    """
    result = execute_admin_command(
        ssh,
        f"\\sn clear file cache location {pnfsid} {pool}"
    )
    logger.info("Cleared file cache location %s %s %s", pnfsid, pool, result)
    return True


def get_precious_fraction(ssh: paramiko.SSHClient, pool: str) -> float:
    """Get the fraction of precious data on a pool.

    Args:
        ssh: Connected SSH client
        pool: Pool name

    Returns:
        Percentage of precious data (0-100)
    """
    result = execute_admin_command(ssh, f"\\s {pool} info -a")
    for line in result:
        if "Precious" in line:
            return float(re.sub(r"[\[-\]]", "", line.split()[-1]))
    return 0.0


def get_active_pools_in_pool_group(
    ssh: paramiko.SSHClient,
    pgroup: str
) -> List[str]:
    """Get list of active pools in a pool group.

    Args:
        ssh: Connected SSH client
        pgroup: Pool group name

    Returns:
        List of active pool names
    """
    result = execute_admin_command(
        ssh,
        f"\\s PoolManager psu ls pgroup -a {pgroup}"
    )
    pools = []
    has_pool_list = False

    for line in result:
        if not line.strip():
            continue
        if "nested groups" in line:
            break
        if line.strip().startswith("poolList :"):
            has_pool_list = True
            continue
        if has_pool_list:
            parts = line.split()
            if "mode=disabled" in parts[1]:
                continue
            pools.append(parts[0].strip())

    return pools
