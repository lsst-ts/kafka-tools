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

from confluent_kafka.admin import ClusterMetadata

from .constants import ListTopicsOpts

__all__ = [
    "consumer_summary",
    "filtered_topics",
    "summerize_consumer_deletion",
    "two_column_table",
]


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


def summerize_consumer_deletion(
    deletes_done: set[Future], deletes_not_done: set[Future]
) -> None:
    """Summarize the consumer deletion action.

    Parameters
    ----------
    deletes_done : set[Future]
        Set of deletes completed successfully.
    deletes_not_done : set[Future]
        Set of deletes not completed successfully.
    """
    num_done = len(deletes_done)
    num_not_done = len(deletes_not_done)

    print(f"Found {num_done + num_not_done} consumers to delete")
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
