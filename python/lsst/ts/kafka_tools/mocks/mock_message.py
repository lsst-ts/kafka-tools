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

from typing import Optional, Tuple


class MockMessage:
    def __init__(self, ts_ms: int, value: bytes):
        self._ts = ts_ms
        self._value = value

    def timestamp(self) -> Tuple[int, int]:
        # confluent-kafka returns (timestamp_type, timestamp_ms)
        return (0, self._ts)

    def key(self) -> Optional[bytes]:
        return None

    def value(self) -> Optional[bytes]:
        return self._value

    def error(self) -> None:
        return None
