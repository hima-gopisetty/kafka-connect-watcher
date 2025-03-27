import json
from os import path

import pytest
import yaml

from kafka_connect_watcher.config import Config


@pytest.mark.parametrize(
    ["config_path", "expected"],
    (
        (
            "test_config.yaml",
            {
                "x-scan_backoff_enabled": True,
                "x-scan_backoff_max_interval": 300,
                "x-scan_backoff_multiplier": 2,
                "clusters": [
                    {
                        "hostname": "localhost",
                        "port": 8083,
                        "interval": 5,
                        "evaluation_rules": [
                            {"auto_correct_actions": [{"action": "restart"}]},
                            {
                                "auto_correct_actions": [
                                    {
                                        "action": "pause",
                                        "notify": [{"target": "sns.main_topic"}],
                                    }
                                ]
                            },
                        ],
                    }
                ],
                "notification_channels": {
                    "sns": {
                        "main_topic": {
                            "topic_arn": "arn:aws:sns:eu-west-1:123456789:test-sns-topic"
                        }
                    }
                },
            },
        ),
    ),
)
def test_config_parsing(config_path, expected):
    actual = Config(path.abspath(f"tests/fixtures/configs/{config_path}"))
    assert actual.config == expected


@pytest.mark.parametrize(
    ["initial_interval", "failure_detected", "expected_interval"],
    [
        (60, True, 120),
        (120, True, 240),
        (240, True, 300),  # should cap at max
        (300, True, 300),  # already at max
        (240, False, 60),  # reset to base
    ],
)
def test_adjust_scan_interval_from_yaml(
    initial_interval, failure_detected, expected_interval
):
    config = Config(path.abspath(f"tests/fixtures/configs/test_config.yaml"))
    config.set_scan_intervals()

    config.scan_intervals = initial_interval

    config.adjust_scan_interval(failure_detected)

    assert config.scan_intervals == expected_interval
