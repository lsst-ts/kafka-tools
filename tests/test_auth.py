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

import pathlib

from click.testing import CliRunner
from jproperties import Properties
from lsst.ts.kafka_tools.cli import main


def test_top_group() -> None:
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(main, ["auth"])
        assert result.exit_code == 0


def test_create_auth_files() -> None:
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(main, ["auth", "create-prop-files", "--auth-dir", "."])
        assert result.exit_code == 0
        pwd = pathlib.Path(".")
        prop_files = list(pwd.glob("*.properties"))
        assert len(prop_files) == 4
        with pathlib.Path("./kafka-aclient-bts.properties").open("rb") as bfile:
            props = Properties()
            props.load(bfile, encoding="utf-8")
            assert len(list(props.keys())) == 5
            assert props["sasl.mechanism"].data == "SCRAM-SHA-512"
        with pathlib.Path("./kafka-aclient-local.properties").open("rb") as lfile:
            props = Properties()
            props.load(lfile, encoding="utf-8")
            assert len(list(props.keys())) == 1
