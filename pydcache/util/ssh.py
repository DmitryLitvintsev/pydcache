from __future__ import annotations

import logging
import paramiko
from typing import Optional

logger = logging.getLogger(__name__)

SSH_HOST = "fndca"
SSH_PORT = 24223
SSH_USER = "enstore"


def get_shell(
    host: str = SSH_HOST,
    port: int = SSH_PORT,
    user: str = SSH_USER,
) -> paramiko.SSHClient:
    """Create and return a dCache admin shell connection via SSH/GSS.

    Args:
        host: SSH host to connect to
        port: SSH port number
        user: SSH username

    Returns:
        Connected SSHClient

    Raises:
        paramiko.SSHException: If connection fails
    """
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=host,
        port=port,
        username=user,
        gss_auth=True,
        gss_kex=True,
    )
    return client

