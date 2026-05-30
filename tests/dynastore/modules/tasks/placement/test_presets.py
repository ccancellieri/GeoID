from dynastore.modules.tasks.placement.presets import PLACEMENT_PRESET_RUNNER, list_placement_presets


def test_cloud_off_loads_to_cloud_run():
    assert PLACEMENT_PRESET_RUNNER["cloud"] == "gcp_cloud_run"


def test_onprem_off_loads_to_worker_queue():
    assert PLACEMENT_PRESET_RUNNER["onprem"] == "worker_queue"


def test_presets_are_registered():
    names = list_placement_presets()
    assert "cloud" in names and "onprem" in names
