from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, NoReturn


logger = logging.getLogger(__name__)


def execute_command(cmd: str) -> int:
    """Execute a shell command and return its exit code.

    Args:
        cmd: Command to execute.

    Returns:
        The command's return code.
    """
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            text=True
        )
        process.communicate()
        return process.returncode
    except subprocess.SubprocessError as exc:
        logger.error("Command execution failed: %s", exc)
        return 1


def get_path(pnfsid: str) -> Optional[str]:
    """Get filesystem path for a PNFS ID.

    Args:
        pnfsid: PNFS ID to look up

    Returns:
        Path if found, None otherwise
    """
    try:
        path = Path(f"/pnfs/fnal.gov/usr/.(pathof)({pnfsid})")
        return path.read_text().strip()
    except (IOError, OSError) as exc:
        logger.error("Failed to get path for %s: %s", pnfsid, exc)
        return None
