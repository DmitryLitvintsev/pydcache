"""pydcache.config – configuration file deployment helpers."""

from __future__ import annotations

import logging
import os
import shutil
import stat
from pathlib import Path

logger = logging.getLogger(__name__)

# Bundled example / template config shipped inside the package
_TEMPLATE = Path(__file__).parent / "pydcache.yaml"

# Environment variable that overrides automatic path resolution
_ENV_VAR = "DCACHE_CONFIG"


def _default_config_path() -> Path:
    """Return the platform-appropriate config path for the current user.

    * root (uid 0)  → /etc/pydcache/pydcache.yaml
    * regular user  → ~/.config/pydcache/pydcache.yaml  (XDG Base Dir spec)
    """
    if os.getuid() == 0:
        return Path("/etc/pydcache/pydcache.yaml")
    xdg_config = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    return xdg_config / "pydcache" / "pydcache.yaml"


def get_config_path() -> Path:
    """Return the resolved config path, deploying the template if needed.

    Resolution order:

    1. ``$DCACHE_CONFIG`` environment variable (no auto-deploy, must exist).
    2. Platform default (``/etc/pydcache/`` or ``~/.config/pydcache/``);
       the bundled template is copied there on first use.

    The deployed file is always created with mode **0600**.

    Returns:
        Path to the config file to load.

    Raises:
        FileNotFoundError: If ``$DCACHE_CONFIG`` is set but does not exist.
    """
    env_override = os.environ.get(_ENV_VAR)
    if env_override:
        path = Path(env_override)
        if not path.exists():
            raise FileNotFoundError(
                f"{_ENV_VAR}={path} — file does not exist"
            )
        return path

    path = _default_config_path()
    if not path.exists():
        _deploy_template(path)
    return path


def _deploy_template(dest: Path) -> None:
    """Copy the bundled template to *dest* and set permissions to 0600.

    Args:
        dest: Destination path (parent directories are created as needed).

    Raises:
        OSError: If the directory cannot be created or the file written.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(_TEMPLATE, dest)
    dest.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 0600
    logger.info(
        "Deployed example config to %s — please fill in your credentials.",
        dest,
    )
