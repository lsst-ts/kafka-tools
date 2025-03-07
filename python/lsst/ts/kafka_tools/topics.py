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

import re

from .constants import ListTopicsOpts
from .helpers import generate_admin_client
from .type_hints import ScriptContext

__all__ = ["get_topics", "list_topics"]


def get_topics(ctxobj: ScriptContext) -> list[str]:
    client = generate_admin_client(ctxobj["site"])
    topics = client.list_topics()
    return topics


def list_topics(ctxobj: ScriptContext, opts: ListTopicsOpts) -> None:
    """List topics from system and possibly filter the list.

    Parameters
    ----------
    ctxobj : ScriptContext
        The context object from the CLI invocation.
    opts : ListTopicsOpts
        CLI options from the invocation.
    """
    client = generate_admin_client(ctxobj["site"])
    result = client.list_topics()
    regex = None
    if opts.regex is not None:
        regex = re.compile(repr(opts.regex)[1:-1])
    for topic in sorted(result.topics.keys()):
        print_topic = True
        if opts.name is not None:
            print_topic = opts.name in topic
        if regex is not None:
            print_topic = regex.search(topic) is not None
        if print_topic:
            print(topic)
