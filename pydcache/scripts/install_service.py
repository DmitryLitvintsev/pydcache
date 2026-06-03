"""Install the cta-nanny systemd service unit file."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

# Bundled unit file template
_UNIT_TEMPLATE = Path(__file__).parent.parent / "config" / "cta-nanny.service"
_SERVICE_NAME = "cta-nanny.service"


def _find_cta_nanny_exec() -> str:
    """Return the absolute path to the cta-nanny executable."""
    path = shutil.which("cta-nanny")
    if path:
        return path
    # Fallback: same bin/ dir as this Python interpreter
    candidate = Path(sys.executable).parent / "cta-nanny"
    if candidate.exists():
        return str(candidate)
    raise FileNotFoundError(
        "Cannot locate the cta-nanny executable. "
        "Make sure pydcache is installed and its bin/ directory is on PATH."
    )


def _system_unit_dir() -> Path:
    return Path("/etc/systemd/system")


def _user_unit_dir() -> Path:
    xdg = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    return xdg / "systemd" / "user"


def main() -> None:
    """Install cta-nanny.service and print next steps."""
    is_root = os.getuid() == 0

    # Resolve real executable path and bake it into the unit file
    try:
        exec_path = _find_cta_nanny_exec()
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    unit_text = _UNIT_TEMPLATE.read_text()
    unit_text = unit_text.replace("CTA_NANNY_EXEC", exec_path)

    if is_root:
        dest_dir = _system_unit_dir()
        systemctl_scope = []          # system scope (default)
        enable_cmd = ["systemctl", "enable", "--now", _SERVICE_NAME]
    else:
        dest_dir = _user_unit_dir()
        systemctl_scope = ["--user"]
        enable_cmd = ["systemctl", "--user", "enable", "--now", _SERVICE_NAME]

    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / _SERVICE_NAME

    dest.write_text(unit_text)
    print(f"Installed unit file → {dest}")

    if is_root:
        # Reload systemd and (optionally) enable the service
        subprocess.run(["systemctl", "daemon-reload"], check=True)
        print("\nUnit file installed. Review it before enabling:\n")
        print(f"  nano {dest}\n")
        print("Then enable and start the service:")
        print(f"  systemctl enable --now {_SERVICE_NAME}")
        print(f"\nUseful commands:")
        print(f"  systemctl status  {_SERVICE_NAME}")
        print(f"  systemctl restart {_SERVICE_NAME}")
        print(f"  journalctl -u {_SERVICE_NAME} -f")
    else:
        subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)
        print("\nUnit file installed (user scope). Review it before enabling:\n")
        print(f"  nano {dest}\n")
        print("Then enable and start the service:")
        print(f"  systemctl --user enable --now {_SERVICE_NAME}")
        print(f"\nUseful commands:")
        print(f"  systemctl --user status  {_SERVICE_NAME}")
        print(f"  systemctl --user restart {_SERVICE_NAME}")
        print(f"  journalctl --user -u {_SERVICE_NAME} -f")
        print(
            "\nNOTE: for a user service to survive logout, run once as root:\n"
            f"  loginctl enable-linger {os.environ.get('USER', 'youruser')}"
        )


if __name__ == "__main__":
    main()

