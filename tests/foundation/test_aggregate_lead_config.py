import pytest
from pydantic import ValidationError

from notable_person_finder.config.models import AggregateLeadConfig, TasksConfig


def test_aggregate_lead_config_defaults() -> None:
    config = AggregateLeadConfig()
    assert config.promising_domain_threshold == 2
    assert config.digest_limit == 10
    assert config.starvation_days == 14
    assert config.reminder_interval_days == 0


def test_tasks_config_composes_aggregate_lead() -> None:
    tasks = TasksConfig()
    assert isinstance(tasks.aggregate_lead, AggregateLeadConfig)


def test_aggregate_lead_config_rejects_zero_domain_threshold() -> None:
    with pytest.raises(ValidationError):
        AggregateLeadConfig(promising_domain_threshold=0)
