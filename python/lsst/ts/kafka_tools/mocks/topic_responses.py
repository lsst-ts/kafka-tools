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

__all__ = ["csc_filtered_topics", "list_topics", "regex_filtered_topics"]


list_topics = """lsst.sal.ATAOS.logevent_heartbeat
lsst.sal.ATAOS.logevent_summaryState
topic1.attribute1
topic1.attribute2
topic1.attribute3
topic2.attribute1
topic2.attribute2
topic2.attribute3
"""

csc_filtered_topics = """lsst.sal.ATAOS.logevent_heartbeat
lsst.sal.ATAOS.logevent_summaryState
"""

regex_filtered_topics = """topic1.attribute3
topic2.attribute3
"""
