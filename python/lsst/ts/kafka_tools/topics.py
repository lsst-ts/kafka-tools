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
import os
import re
from datetime import datetime, timezone
from typing import Dict, List

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import NewPartitions

from .constants import ListTopicsOpts
from .helpers import create_config, generate_admin_client
from .type_hints import DoneAndNotDoneFutures, ScriptContext

__all__ = [
    "delete_topics",
    "filter_topics",
    "get_topics",
    "set_partitions_topics",
    "query_topic_time_range",
]


def delete_topics(ctxobj: ScriptContext, topics: list[str]) -> DoneAndNotDoneFutures:
    """Delete the list of topics.

    Parameters
    ----------
    ctxobj : ScriptContext

    topics : list[str]
        List of topics to delete.

    Returns
    -------
    DoneAndNotDoneFutures
        The done and not done futures.
    """
    client = generate_admin_client(ctxobj["site"])

    topics_to_delete = client.delete_topics(topics)
    results = concurrent.futures.wait(list(topics_to_delete.values()))
    return (results.done, results.not_done)


def filter_topics(ctxobj: ScriptContext, opts: ListTopicsOpts) -> list[str]:
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
    topics: list[str] = []
    regex = None
    name_set = None
    if opts.regex is not None:
        regex = re.compile(repr(opts.regex)[1:-1])
    if opts.name_list is not None:
        name_set = opts.name_list.split(",")
    if opts.name_file is not None:
        ifile = opts.name_file.expanduser()
        name_set = ifile.read_text().split(os.linesep)
    for topic in sorted(result.topics.keys()):
        if opts.name is not None and opts.name in topic:
            topics.append(topic)
        if regex is not None and regex.search(topic) is not None:
            topics.append(topic)
        if name_set is not None:
            for name in name_set:
                if name in topic:
                    topics.append(topic)

    return topics


def get_topics(ctxobj: ScriptContext) -> list[str]:
    """Get all topics.

    Parameters
    ----------
    ctxobj : ScriptContext
        The context object from the CLI invocation.

    Returns
    -------
    list[str]
        List of all the topics.
    """
    client = generate_admin_client(ctxobj["site"])
    topics = client.list_topics()
    return topics


def set_partitions_topics(
    ctxobj: ScriptContext, topics: list[str], csc: str, partitions: int
) -> DoneAndNotDoneFutures:
    """Set partitions on CSC telemetry topics.

    Parameters
    ----------
    ctxobj : ScriptContext
        The context object from the CLI invocation.
    topics : list[str]
        The list of topics to modify. May contain similarly named CSCs.
    csc : str
        CSC name for exact checking.
    partitions : int
        The number of partitions to set on the topics.

    Returns
    -------
    DoneAndNotDoneFutures
        The done and not done futures.
    """
    telemetry_topics: list[NewPartitions] = []
    for topic in topics:
        values = topic.split(".")
        if csc != values[2]:
            continue
        if values[3].startswith(("ackcmd", "logevent", "command")):
            continue
        else:
            telemetry_topics.append(NewPartitions(topic, partitions))

    client = generate_admin_client(ctxobj["site"])
    topics_modified = client.create_partitions(telemetry_topics)
    results = concurrent.futures.wait(list(topics_modified.values()))
    return (results.done, results.not_done)


def query_topic_time_range(
    ctxobj: ScriptContext,
    topic: str,
    start_str: str,
    end_str: str,
    max_messages: int = 1000,
) -> List[Dict]:
    """Query a Kafka topic for messages within a time range.

    Parameters
    ----------
    ctxobj : ScriptContext
        CLI context.
    topic : str
        Topic name.
    start_str : str
        Start time (YYYY-MM-DD-HH:MM).
    end_str : str
        End time (YYYY-MM-DD-HH:MM).
    max_messages : int
        Safety limit.

    Returns
    -------
    list[dict]
    """

    start_dt = datetime.strptime(start_str, "%Y-%m-%d-%H:%M").replace(
        tzinfo=timezone.utc
    )
    end_dt = datetime.strptime(end_str, "%Y-%m-%d-%H:%M").replace(tzinfo=timezone.utc)

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    props = create_config(ctxobj["site"])
    conf = {
        "group.id": "kafka-tools-time-query",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    for key, prop in props.items():
        conf[str(key)] = str(prop.data)

    consumer = Consumer(conf)

    md = consumer.list_topics(topic, timeout=10)
    partitions = [
        TopicPartition(topic, p.id, start_ms)
        for p in md.topics[topic].partitions.values()
    ]

    offsets = consumer.offsets_for_times(partitions, timeout=10)
    consumer.assign(offsets)

    results: list[Dict] = []

    try:
        while len(results) < max_messages:
            msg = consumer.poll(1.0)
            if msg is None:
                break
            if msg.error():
                raise RuntimeError(msg.error())

            _, ts = msg.timestamp()
            ts_human = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
            if ts > end_ms:
                break

            results.append(
                {
                    "timestamp_ms": ts_human,
                    "key": msg.key().decode("utf-8") if msg.key() else None,
                    "value": msg.value().decode("utf-8") if msg.value() else None,
                }
            )
    finally:
        consumer.close()

    return results
