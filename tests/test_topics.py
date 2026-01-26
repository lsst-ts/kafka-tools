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

import os
import pathlib
from unittest.mock import MagicMock, patch

from click.testing import CliRunner
from confluent_kafka import TopicPartition
from lsst.ts.kafka_tools.cli import main
from lsst.ts.kafka_tools.mocks.ceph_events import ceph_event_single_put
from lsst.ts.kafka_tools.mocks.mock_admin_client import MockAdminClient
from lsst.ts.kafka_tools.mocks.mock_message import MockMessage
from lsst.ts.kafka_tools.mocks.topic_responses import (
    csc_filtered_topics,
    list_topics,
    name_delete,
    name_file_delete,
    name_list_delete,
    partition_expansion,
    regex_delete,
    regex_filtered_topics,
)


def test_top_group() -> None:
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(main, ["topics"])
        assert result.exit_code == 0


@patch("lsst.ts.kafka_tools.topics.generate_admin_client", spec=True)
def test_topics_list(mock_gen_admin_client: MagicMock) -> None:
    mock_gen_admin_client.return_value = MockAdminClient()

    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(main, ["topics", "local", "list"])
        assert result.exit_code == 0
        assert result.stdout == list_topics

        result = runner.invoke(main, ["topics", "local", "list", "--name", "ATAOS"])
        assert result.exit_code == 0
        assert result.stdout == csc_filtered_topics

        result = runner.invoke(main, ["topics", "local", "list", "--regex", "3$"])
        assert result.exit_code == 0
        assert result.stdout == regex_filtered_topics

        result = runner.invoke(
            main, ["topics", "local", "list", "--regex", "3$", "--name", "ATAOS"]
        )
        assert result.exit_code == 2


@patch("lsst.ts.kafka_tools.topics.generate_admin_client", spec=True)
def test_topics_delete(mock_gen_admin_client: MagicMock) -> None:
    mock_gen_admin_client.return_value = MockAdminClient()

    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            main, ["topics", "local", "delete", "--regex", "3$"], input="N"
        )
        assert result.exit_code == 255

        result = runner.invoke(
            main, ["topics", "local", "delete", "--regex", "3$"], input="y"
        )
        assert result.exit_code == 0
        assert result.stdout == regex_delete

        result = runner.invoke(
            main, ["topics", "local", "delete", "--name", "topic1"], input="y"
        )
        assert result.exit_code == 0
        assert result.stdout == name_delete

        result = runner.invoke(
            main,
            ["topics", "local", "delete", "--name-list", "topic2,ATAOS"],
            input="y",
        )
        assert result.exit_code == 0
        assert result.stdout == name_list_delete

        ofile = pathlib.Path("topic_list.txt")
        ofile.write_text(os.linesep.join(["topic1", "topic2"]))

        result = runner.invoke(
            main,
            ["topics", "local", "delete", "--name-file", "topic_list.txt"],
            input="y",
        )
        assert result.exit_code == 0
        assert result.stdout == name_file_delete

    result = runner.invoke(main, ["topics", "local", "delete"])
    assert result.exit_code == 2

    result = runner.invoke(
        main, ["topics", "local", "delete", "--regex", "3$", "--name", "ATAOS"]
    )
    assert result.exit_code == 2


@patch("lsst.ts.kafka_tools.topics.generate_admin_client", spec=True)
def test_topics_partitions(mock_gen_admin_client: MagicMock) -> None:
    mock_gen_admin_client.return_value = MockAdminClient()
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            main, ["topics", "local", "set-partitions", "ATAOS", "8"]
        )
        assert result.exit_code == 0
        assert result.stdout == partition_expansion


@patch("lsst.ts.kafka_tools.topics.Consumer")
@patch("lsst.ts.kafka_tools.topics.create_config")
def test_topics_query(
    mock_create_config: MagicMock,
    mock_consumer_cls: MagicMock,
) -> None:

    mock_create_config.return_value = {}
    consumer = MagicMock()
    mock_consumer_cls.return_value = consumer
    partition = MagicMock()
    partition.id = 0

    consumer.list_topics.return_value.topics = {
        "lsst.s3.raw.lsstcam": MagicMock(partitions={0: partition})
    }
    consumer.offsets_for_times.return_value = [
        TopicPartition("lsst.s3.raw.lsstcam", 0, 0)
    ]

    inside_window_ts = 1768282330000  # 2026-01-13T05:32:10 UTC
    after_window_ts = inside_window_ts + 10 * 60 * 1000  # +10 minutes

    consumer.poll.side_effect = [
        MockMessage(inside_window_ts, ceph_event_single_put),  # should be included
        MockMessage(after_window_ts, ceph_event_single_put),  # should not
        None,  # end of messages
    ]

    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            main,
            [
                "topics",
                "local",
                "query",
                "2026-01-13-05:32",
                "2026-01-13-05:33",
                "lsst.s3.raw.lsstcam",
            ],
        )

        assert result.exit_code == 0

        # only one message should appear
        assert result.stdout.count("ObjectCreated:Put") == 1
        assert "LSSTCam/file.fits" in result.stdout
