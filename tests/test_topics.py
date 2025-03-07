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

from click.testing import CliRunner
from lsst.ts.kafka_tools.cli import main
from lsst.ts.kafka_tools.mocks.mock_admin_client import MockAdminClient
from lsst.ts.kafka_tools.mocks.topic_responses import (
    csc_filtered_topics,
    list_topics,
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
            main, ["topics", "test", "list", "--regex", "3$", "--name", "ATAOS"]
        )
        assert result.exit_code == 2
