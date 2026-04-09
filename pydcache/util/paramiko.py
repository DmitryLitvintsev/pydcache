from __future__ import annotations

import logging
import paramiko
from typing import Any, Dict, List, Optional, Tuple, NoReturn

logger = logging.getLogger(__name__)

SSH_HOST = "fndca"
SSH_PORT = 24223
SSH_USER = "enstore"

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
