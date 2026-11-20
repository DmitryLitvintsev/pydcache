"""Check that every console script in pyproject.toml points to a real main().

Catches copy-paste slips such as a command pointing to another script's
module. Uses only the source files (no imports), so it runs without the
package dependencies installed.
"""

import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent

# Commands deliberately named differently from their module
# ("command": "module"); all others must follow dcache-foo -> dcache_foo.
NAME_EXCEPTIONS = {}


def console_scripts():
    """Return (command, "module:function") pairs from [project.scripts]."""
    text = (ROOT / "pyproject.toml").read_text()
    section = re.search(r"^\[project\.scripts\]\s*$(.*?)(?=^\[|\Z)",
                        text, re.MULTILINE | re.DOTALL)
    assert section, "no [project.scripts] section in pyproject.toml"
    return re.findall(r'^\s*([\w.-]+)\s*=\s*"([^"]+)"',
                      section.group(1), re.MULTILINE)


SCRIPTS = console_scripts()


def test_console_scripts_found():
    assert SCRIPTS, "no console scripts found in [project.scripts]"


@pytest.mark.parametrize("command,target", SCRIPTS)
def test_console_script_target_exists(command, target):
    module, _, function = target.partition(":")
    assert function, f"{command}: target {target!r} has no ':function'"

    path = ROOT / Path(*module.split(".")).with_suffix(".py")
    assert path.is_file(), f"{command}: module file {path} does not exist"

    tree = ast.parse(path.read_text(), filename=str(path))
    defined = {node.name for node in tree.body
               if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    assert function in defined, \
        f"{command}: {function}() is not defined in {path.name}"


@pytest.mark.parametrize("command,target", SCRIPTS)
def test_console_script_matches_module_name(command, target):
    """dcache-foo must point to module dcache_foo (catches a command
    pointing to another, existing script)."""
    module = target.partition(":")[0].rsplit(".", 1)[-1]
    expected = NAME_EXCEPTIONS.get(command, command.replace("-", "_"))
    assert module == expected, \
        f"{command} points to module {module}, expected {expected} " \
        "(if intended, add it to NAME_EXCEPTIONS)"
