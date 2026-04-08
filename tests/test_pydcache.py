"""Basic smoke tests for the pydcache package."""

import pydcache


def test_version():
    """Package exposes a __version__ string."""
    assert isinstance(pydcache.__version__, str)
    assert pydcache.__version__ != ""
