from pathlib import Path

import stream_sorter.incremental as incremental
import stream_sorter.plugin as plugin


def test_plugin_uses_incremental_analyzer_directly():
    assert plugin.analyze_assigned_streams is incremental.analyze_assigned_streams

    source = (Path(__file__).parents[1] / "stream_sorter" / "plugin.py").read_text()
    assert "from .incremental import" in source
    assert "analyze_assigned_streams" in source
    assert "from .analyzer import ANALYSIS_CACHE_PATH, analyze_assigned_streams" not in source
