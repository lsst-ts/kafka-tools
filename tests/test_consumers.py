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
            main, ["consumers", "--timeout", 1, "local", "list", "--regex", "3$"]
        )
    assert result.exit_code == 0
    assert result.stdout == mcr.list_regex


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

    result = runner.invoke(main,["consumers", "--timeout", 1, "local", "delete"])
    assert result.exit_code == 0
    assert result.stdout == mcr.delete_consumers

    result = runner.invoke(
            main, ["consumers", "--timeout", 1, "local", "delete", "--regex", "3$"]
        )
    print(result)
    assert result.exit_code == 0
    assert result.stdout == mcr.delete_consumers_regex
