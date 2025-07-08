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
from concurrent.futures import Future

from confluent_kafka.admin import (
    ClusterMetadata,
    ConfigEntry,
    ConfigSource,
    ConsumerGroupDescription,
)

from .constants import ListTopicsOpts

__all__ = [
    "consumer_descriptions",
    "consumer_summary",
    "filtered_topics",
    "list_broker_configs",
    "summerize_deletion",
    "two_column_table",
]


def consumer_descriptions(descrs: list[ConsumerGroupDescription]) -> None:
    """Print consumer descriptions.

    Parameters
    ----------
    descrs : list[ConsumerGroupDescription]
        The list of consumer descriptions.
    """
    for descr in descrs:
        print(descr.group_id)
        print("Topics:")
        for member in descr.members:
            topics = sorted({x.topic for x in member.assignment.topic_partitions})
            for topic in topics:
                print(topic)
        print()


def consumer_summary(summary: dict[str, int]) -> None:
    """Print summary of consumer states.

    Parameters
    ----------
    summary : dict[str, int]
        Instance containing consumer information.
    """
    num_stable_consumers = summary["active"]
    num_empty_consumers = summary["inactive"]

    print(f"Found {num_stable_consumers + num_empty_consumers} consumers")
    print(f"{num_stable_consumers} active, {num_empty_consumers} inactive")


def filtered_topics(topics: ClusterMetadata, opts: ListTopicsOpts) -> None:
    """Print topic list, potentially filtered.

    Parameters
    ----------
    topics : ClusterMetadata
        Instance containing the list of topics.
    opts : ListTopicsOpts
        Options from the CLI for printing.
    """
    regex = None
    if opts.regex is not None:
        regex = re.compile(repr(opts.regex)[1:-1])
    for topic in sorted(topics.topics.keys()):
        print_topic = True
        if opts.name is not None:
            print_topic = opts.name in topic
        if regex is not None:
            print_topic = regex.search(topic) is not None
        if print_topic:
            print(topic)


def list_broker_configs(broker_id: str, configs: list[ConfigEntry]) -> None:
    """Print out the broker configuration.

    Parameters
    ----------
    broker_id : str
        The ID of the broker.
    configs : list[ConfigEntry]
        The list of configuration entries.
    """
    print(f"All configs for broker {broker_id} are:")
    for config in configs:
        if config.value is None:
            cvalue = "null"
        else:
            cvalue = config.value

        synonyms = []
        for v in config.synonyms.values():
            synonyms.append(f"{ConfigSource(v.source).name}:{v.name}={v.value}")

        is_sensitive = str(config.is_sensitive).lower()
        print(
            f"  {config.name}={cvalue} sensitive={is_sensitive} synonyms={{{', '.join(synonyms)}}}"
        )


def summerize_deletion(
    type_del: str, deletes_done: set[Future], deletes_not_done: set[Future]
) -> None:
    """Summarize the deletion action.

    Parameters
    ----------
    type_del: str
        The type of item being deleted.
    deletes_done : set[Future]
        Set of deletes completed successfully.
    deletes_not_done : set[Future]
        Set of deletes not completed successfully.
    """
    num_done = len(deletes_done)
    num_not_done = len(deletes_not_done)

    print(f"Found {num_done + num_not_done} {type_del} to delete")
    print(f"{num_done} deleted successfully, {num_not_done} not successfully deleted")


def two_column_table(values: list[tuple[str, str]], max_length: int) -> None:
    """Print a two column table of information.

    Parameters
    ----------
    values : list[tuple[str, str]]
        The instance containing the two columns of information.
    max_length : int
        The maximum length of column 1 for formatting purposes.
    """
    for c1, c2 in values:
        print(f"{c1:<{max_length}}  {c2}")
