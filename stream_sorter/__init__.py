"""Dispatcharr Stream Sort plugin package."""

# plugin.py historically imports analyze_assigned_streams from analyzer.py.
# Install the cache-aware implementation before plugin.py is imported while
# keeping the mature low-level analyzer functions in their original module.
from .incremental import install as _install_incremental_analyzer

_install_incremental_analyzer()
