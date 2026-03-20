# This file is part of kafka_tools.
#
# Developed for the Rubin Observatory.
# This product includes software developed by the Rubin Observatory Project
# (https://rubinobservatory.org/).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from unittest.mock import MagicMock, patch

import lsst.ts.kafka_tools.mocks.consumer_responses as mcr
from click.testing import CliRunner
from lsst.ts.kafka_tools.cli import main
from lsst.ts.kafka_tools.consumers import (
    consumer_group_lag,
    consumer_groups_lag_by_prefix,
)
from lsst.ts.kafka_tools.mocks.mock_admin_client import MockAdminClient


def test_top_group() -> None:
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(main, ["consumers"])
        assert result.exit_code == 0


@patch("lsst.ts.kafka_tools.consumers.generate_admin_client", spec=True)
def test_summarize_consumers(mock_gen_admin_client: MagicMock) -> None:
    mock_gen_admin_client.return_value = MockAdminClient()

    runner = CliRunner()
    result = runner.invoke(main, ["consumers", "--timeout", 1, "local", "summary"])
    assert result.exit_code == 0
    assert result.stdout == mcr.summary

    result = runner.invoke(
        main, ["consumers", "--timeout", 1, "local", "summary", "--no-telegraph-filter"]
    )
    assert result.exit_code == 0
    assert result.stdout == mcr.summary_no_filter


@patch("lsst.ts.kafka_tools.consumers.generate_admin_client", spec=True)
def test_list_consumers(mock_gen_admin_client: MagicMock) -> None:
    mock_gen_admin_client.return_value = MockAdminClient()

    runner = CliRunner()
    result = runner.invoke(main, ["consumers", "--timeout", 1, "local", "list"])
    assert result.exit_code == 0
    assert result.stdout == mcr.list_all

    result = runner.invoke(
        main, ["consumers", "--timeout", 1, "local", "list", "--no-connector-filter"]
    )
    assert result.exit_code == 0
    assert result.stdout == mcr.list_all_no_filter

    result = runner.invoke(
        main, ["consumers", "--timeout", 1, "local", "list", "--active"]
    )
    assert result.exit_code == 0
    assert result.stdout == mcr.list_active

    result = runner.invoke(
        main, ["consumers", "--timeout", 1, "local", "list", "--inactive"]
    )
    assert result.exit_code == 0
    assert result.stdout == mcr.list_inactive

    result = runner.invoke(
        main, ["consumers", "--timeout", 1, "local", "list", "--regex", "1$"]
    )
    assert result.exit_code == 0
    assert result.stdout == mcr.list_regex_exclusive

    result = runner.invoke(
        main,
        [
            "consumers",
            "--timeout",
            1,
            "local",
            "list",
            "--regex",
            "1$",
            "--regex-exclusive",
        ],
    )
    assert result.exit_code == 0
    assert result.stdout == mcr.list_regex_exclusive

    result = runner.invoke(
        main,
        [
            "consumers",
            "--timeout",
            1,
            "local",
            "list",
            "--regex",
            "1$",
            "--regex-inclusive",
        ],
    )
    assert result.exit_code == 0
    assert result.stdout == mcr.list_regex_inclusive


@patch("lsst.ts.kafka_tools.consumers.generate_admin_client", spec=True)
def test_delete_consumers(mock_gen_admin_client: MagicMock) -> None:
    mac = MockAdminClient()
    mock_gen_admin_client.return_value = mac

    mac.set_empty_consumers_to_stable()

    runner = CliRunner()
    result = runner.invoke(main, ["consumers", "--timeout", 1, "local", "delete"])
    assert result.exit_code == 0
    assert result.stdout == "No consumers to delete.\n"

    mac.reset_empty_consumers()

    result = runner.invoke(main, ["consumers", "--timeout", 1, "local", "delete"])
    assert result.exit_code == 0
    assert result.stdout == mcr.delete_consumers

    result = runner.invoke(main, ["consumers", "--timeout", 1, "local", "delete"])
    assert result.exit_code == 0
    assert result.stdout == mcr.delete_consumers

    result = runner.invoke(
        main,
        [
            "consumers",
            "--timeout",
            1,
            "local",
            "delete",
            "--regex",
            "3$",
            "--regex-inclusive",
        ],
    )
    assert result.exit_code == 0
    assert result.stdout == mcr.delete_consumers_regex_inclusive

    result = runner.invoke(
        main,
        [
            "consumers",
            "--timeout",
            1,
            "local",
            "delete",
            "--regex",
            "consumer1*",
            "--regex-exclusive",
        ],
    )
    assert result.exit_code == 0
    assert result.stdout == mcr.delete_consumers_regex_exclusive

    result = runner.invoke(
        main, ["consumers", "--timeout", 1, "local", "delete", "--regex", "consumer1*"]
    )
    assert result.exit_code == 0
    assert result.stdout == mcr.delete_consumers_regex_exclusive


@patch("lsst.ts.kafka_tools.consumers.generate_admin_client", spec=True)
def test_describe_consumers(mock_gen_admin_client: MagicMock) -> None:
    mac = MockAdminClient()
    mock_gen_admin_client.return_value = mac

    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "consumers",
            "--timeout",
            1,
            "local",
            "describe",
            "consumer1,consumer5",
        ],
    )
    assert result.exit_code == 0
    assert result.stdout == mcr.describe_consumers

    result = runner.invoke(
        main,
        [
            "consumers",
            "--timeout",
            1,
            "local",
            "describe",
            "consumer5",
        ],
    )
    assert result.exit_code == 0
    assert result.stdout == mcr.describe_consumers_single

    result = runner.invoke(
        main,
        [
            "consumers",
            "--timeout",
            1,
            "local",
            "describe",
            "--summary",
            "consumer1,consumer5",
        ],
    )
    assert result.exit_code == 0
    assert result.stdout == mcr.describe_consumers_summary

    result = runner.invoke(
        main,
        [
            "consumers",
            "--timeout",
            1,
            "local",
            "describe",
            "--summary",
            "consumer5",
        ],
    )
    assert result.exit_code == 0
    assert result.stdout == mcr.describe_consumers_summary_single


@patch("lsst.ts.kafka_tools.consumers.generate_admin_client", spec=True)
def test_consumer_group_lag(mock_gen_admin_client: MagicMock) -> None:
    mock_gen_admin_client.return_value = MockAdminClient()
    ctxobj = {"site": "local", "timeout": 1000}

    # Group with committed offsets
    result = consumer_group_lag(ctxobj, "consumer1")
    assert result["group_id"] == "consumer1"
    assert result["total_lag"] == 5
    assert len(result["partitions"]) == 2
    by_key = {(p["topic"], p["partition"]): p for p in result["partitions"]}
    assert by_key[("topic1", 0)] == {
        "topic": "topic1",
        "partition": 0,
        "committed": 5,
        "end_offset": 10,
        "lag": 5,
    }
    assert by_key[("topic2", 0)] == {
        "topic": "topic2",
        "partition": 0,
        "committed": 3,
        "end_offset": 3,
        "lag": 0,
    }

    # Group with no committed offsets
    result = consumer_group_lag(ctxobj, "consumer5")
    assert result == {"group_id": "consumer5", "total_lag": 0, "partitions": []}


@patch("lsst.ts.kafka_tools.consumers.generate_admin_client", spec=True)
def test_consumer_groups_lag_by_prefix(mock_gen_admin_client: MagicMock) -> None:
    mock_gen_admin_client.return_value = MockAdminClient()
    ctxobj = {"site": "local", "timeout": 1000}

    # Prefix that matches nothing
    result = consumer_groups_lag_by_prefix(ctxobj, "nonexistent")
    assert result == {"prefix": "nonexistent", "total_lag": 0, "groups": []}

    # "consumer1" matches consumer1, consumer10, consumer11, consumer13
    result = consumer_groups_lag_by_prefix(ctxobj, "consumer1")
    assert result["prefix"] == "consumer1"
    # Only consumer1 has committed offsets, so total lag is 5
    assert result["total_lag"] == 5
    group_ids = {g["group_id"] for g in result["groups"]}
    assert group_ids == {"consumer1", "consumer10", "consumer11", "consumer13"}
    consumer1_result = next(g for g in result["groups"] if g["group_id"] == "consumer1")
    assert consumer1_result["total_lag"] == 5
    assert len(consumer1_result["partitions"]) == 2
    # Groups without offsets get empty partition lists
    for gid in ("consumer10", "consumer11", "consumer13"):
        group = next(g for g in result["groups"] if g["group_id"] == gid)
        assert group["total_lag"] == 0
        assert group["partitions"] == []
