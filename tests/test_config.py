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

import lsst.ts.kafka_tools.mocks.configs_responses as mcr
from click.testing import CliRunner
from lsst.ts.kafka_tools.cli import main
from lsst.ts.kafka_tools.mocks.mock_admin_client import MockAdminClient


def test_top_group() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["config"])
    assert result.exit_code == 0


@patch("lsst.ts.kafka_tools.configs.generate_admin_client", spec=True)
def test_summarize_consumers(mock_gen_admin_client: MagicMock) -> None:
    mock_gen_admin_client.return_value = MockAdminClient()

    runner = CliRunner()
    result = runner.invoke(main, ["config", "local", "brokers", "2"])
    assert result.exit_code == 0
    assert result.stdout == mcr.broker_config
