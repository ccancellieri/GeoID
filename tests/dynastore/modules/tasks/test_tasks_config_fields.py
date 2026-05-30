from dynastore.modules.tasks.tasks_config import TasksPluginConfig


def test_runner_and_dispatcher_fields_present_with_defaults():
    cfg = TasksPluginConfig()
    assert cfg.background_runner_concurrency == 100
    assert cfg.dispatcher_batch_size == 10
    assert cfg.dispatcher_claim_reject_backoff_seconds == 30
    assert cfg.task_timeout_seconds == 3600
