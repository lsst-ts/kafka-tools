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

from __future__ import annotations

import concurrent.futures
from typing import Any, TypedDict

__all__ = ["DoneAndNotDoneFutures", "ScriptContext", "ScriptOptions"]

ScriptOptions = dict[str, Any]
DoneAndNotDoneFutures = tuple[
    set[concurrent.futures.Future], set[concurrent.futures.Future]
]


class ScriptContext(TypedDict):
    site: str
    timeout: int
