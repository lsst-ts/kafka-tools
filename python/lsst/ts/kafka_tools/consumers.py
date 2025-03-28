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
import re

from confluent_kafka import ConsumerGroupState
from confluent_kafka.admin import ConsumerGroupListing

from .constants import ListConsumerOpts
from .helpers import generate_admin_client
from .type_hints import DoneAndNotDoneFutures, ScriptContext

__all__ = ["delete_consumers", "list_consumers", "summarize_consumers"]


def _filter_telegraph_consumers(
    clist: list[ConsumerGroupListing], no_filter: bool
) -> list[ConsumerGroupListing]:
    """Potentially filter consumer list for telegraph consumers.

    Parameters
    ----------
    clist : list[ConsumerGroupListing]
        Set of consumers to potentially filter.
    no_filter : bool
        Whether or not to filter telegraph consumers.

    Returns
    -------
    list[ConsumerGroupListing]
        List of potentially filtered consumers.
    """
    if no_filter:
        return clist
    else:
        new_list = []
        for c in clist:
            if not c.group_id.startswith("telegraf"):
                new_list.append(c)
        return new_list


def delete_consumers(
    ctxobj: ScriptContext, consumers: list[str]
) -> DoneAndNotDoneFutures:
    """Delete all inactive consumers.

    Parameters
    ----------
    ctxobj : ScriptContext
        The context object from the CLI invocation.
    consumers: list[str]
        The names of the consumer groups to delete.

    Returns
    -------
    DoneAndNotDoneFutures
        The done and not done futures.
    """
    client = generate_admin_client(ctxobj["site"])
    timeout = ctxobj["timeout"] / 1000.0

    consumers_to_delete = client.delete_consumer_groups(consumers)
    results = concurrent.futures.wait(
        list(consumers_to_delete.values()), timeout=timeout
    )
    return (results.done, results.not_done)


def list_consumers(
    ctxobj: ScriptContext, opts: ListConsumerOpts
) -> tuple[list[tuple[str, str]], int]:
    """List consumers.

    Parameters
    ----------
    ctxobj : ScriptContext
        The context object from the CLI invocation.
    opts : ListConsumerOpts
        The options provided to the CLI invocation.

    Returns
    -------
    tuple[list[tuple[str, str]], int]
        The list of consumer names with state and the size of the longest name.
    """
    client = generate_admin_client(ctxobj["site"])
    timeout = ctxobj["timeout"] / 1000.0

    states = []
    if opts.consumer_state in ("All", "Stable"):
        states.append(ConsumerGroupState.STABLE)
    if opts.consumer_state in ("All", "Empty"):
        states.append(ConsumerGroupState.EMPTY)
    if opts.regex is not None:
        regex = re.compile(repr(opts.regex)[1:-1])
    consumers_task = client.list_consumer_groups(states=set(states))
    concurrent.futures.wait([consumers_task], timeout=timeout)
    consumers = _filter_telegraph_consumers(
        consumers_task.result().valid, opts.no_connector_filter
    )
    compact_list = []
    max_length = 0
    for consumer in consumers:
        name = consumer.group_id
        if len(name) > max_length:
            max_length = len(name)
        if opts.regex is not None:
            if opts.regex_mode in "Inclusive" and regex.search(name) is not None:
                compact_list.append((name, consumer.state.name))
            if opts.regex_mode in "Exclusive" and regex.search(name) is None:
                compact_list.append((name, consumer.state.name))
        else:
            compact_list.append((name, consumer.state.name))

    return compact_list, max_length


def summarize_consumers(
    ctxobj: ScriptContext, no_telegraph_filter: bool
) -> dict[str, int]:
    """Make summary of consumers.

    Parameters
    ----------
    ctxobj : ScriptContext
        The context object from the CLI invocation.
    no_telegraph_filter : bool
        Flag to determine filtering of consumers for telegraph consumers.

    Returns
    -------
    dict[str, int]
        Dict containing the number of consumers in the two states.
    """
    client = generate_admin_client(ctxobj["site"])
    timeout = ctxobj["timeout"] / 1000.0

    stable = {
        ConsumerGroupState.STABLE,
    }
    empty = {
        ConsumerGroupState.EMPTY,
    }

    stable_consumers_task = client.list_consumer_groups(states=stable)
    empty_consumers_task = client.list_consumer_groups(states=empty)
    concurrent.futures.wait(
        [stable_consumers_task, empty_consumers_task], timeout=timeout
    )
    stable_consumers = _filter_telegraph_consumers(
        stable_consumers_task.result().valid, no_telegraph_filter
    )
    empty_consumers = _filter_telegraph_consumers(
        empty_consumers_task.result().valid, no_telegraph_filter
    )

    num_stable_consumers = len(stable_consumers)
    num_empty_consumers = len(empty_consumers)

    return {"active": num_stable_consumers, "inactive": num_empty_consumers}
