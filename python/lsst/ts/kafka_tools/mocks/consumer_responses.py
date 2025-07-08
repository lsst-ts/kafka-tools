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

__all__ = [
    "delete_consumers",
    "delete_consumers_regex_inclusive",
    "delete_consumers_regex_exclusive",
    "list_active",
    "list_all",
    "list_all_no_filter",
    "list_inactive",
    "list_regex_inclusive",
    "list_regex_exclusive",
    "summary",
    "summary_no_filter",
]

summary = """Found 9 consumers
7 active, 2 inactive
"""

summary_no_filter = """Found 10 consumers
8 active, 2 inactive
"""

list_all_no_filter = """telegraf-kafka-maintel  STABLE
consumer1               STABLE
consumer5               STABLE
consumer2               STABLE
consumer6               STABLE
consumer9               STABLE
consumer11              STABLE
consumer21              STABLE
consumer13              EMPTY
consumer10              EMPTY
"""

list_all = """consumer1   STABLE
consumer5   STABLE
consumer2   STABLE
consumer6   STABLE
consumer9   STABLE
consumer11  STABLE
consumer21  STABLE
consumer13  EMPTY
consumer10  EMPTY
"""

list_active = """consumer1   STABLE
consumer5   STABLE
consumer2   STABLE
consumer6   STABLE
consumer9   STABLE
consumer11  STABLE
consumer21  STABLE
"""

list_inactive = """consumer13  EMPTY
consumer10  EMPTY
"""

list_regex_inclusive = """consumer1   STABLE
consumer11  STABLE
consumer21  STABLE
"""

list_regex_exclusive = """consumer5   STABLE
consumer2   STABLE
consumer6   STABLE
consumer9   STABLE
consumer13  EMPTY
consumer10  EMPTY
"""

delete_consumers = """Found 2 consumers to delete
2 deleted successfully, 0 not successfully deleted
"""

delete_consumers_regex_inclusive = """Found 1 consumers to delete
1 deleted successfully, 0 not successfully deleted
"""

delete_consumers_regex_exclusive = """No consumers to delete.
"""

describe_consumers = """consumer1
Topics:
topic1
topic2
topic3

consumer5
Topics:
lsst.sal.ATAOS.logevent_heartbeat
lsst.sal.ATAOS.timestamp

"""

describe_consumers_single = """consumer5
Topics:
lsst.sal.ATAOS.logevent_heartbeat
lsst.sal.ATAOS.timestamp

"""
