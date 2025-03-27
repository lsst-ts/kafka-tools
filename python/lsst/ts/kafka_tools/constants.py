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

import dataclasses
import pathlib

__all__ = ["ListConsumerOpts", "ListTopicsOpts", "SITES"]


SITES = ["tts", "bts", "summit", "local", "envvar"]


@dataclasses.dataclass
class ListConsumerOpts:
    regex: str | None
    regex_mode: str
    no_connector_filter: bool
    consumer_state: str


@dataclasses.dataclass
class ListTopicsOpts:
    regex: str | None
    name: str | None
    name_list: str | None
    name_file: pathlib.Path | None
