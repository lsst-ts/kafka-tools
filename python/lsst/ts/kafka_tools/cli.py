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

import pathlib
from functools import update_wrapper
from typing import Any

import click

from .auth import create_properties_files
from .configs import show_broker_config
from .constants import SITES, ListConsumerOpts, ListTopicsOpts
from .consumers import (
    delete_consumers,
    describe_consumers,
    list_consumers,
    summarize_consumers,
)
from .helpers import acknowledge_deletion
from .print_helpers import (
    consumer_descriptions,
    consumer_summary,
    filtered_topics,
    list_broker_configs,
    summerize_deletion,
    two_column_table,
)
from .topics import delete_topics, filter_topics, get_topics, set_partitions_topics

__all__ = ["auth", "auth_create_prop_files", "main", "topics", "topics_list"]


def pass_obj(f: Any) -> Any:
    @click.pass_context
    def new_func(ctx: click.Context, *args: Any, **kwargs: Any) -> Any:
        return ctx.invoke(f, ctx.obj, *args, **kwargs)

    return update_wrapper(new_func, f)


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(message="%(version)s")
def main() -> None:
    """Command line interface for Kafka operations."""


@main.group()
def auth() -> None:
    """Authentication commands."""


@auth.command("create-prop-files")
@click.option(
    "--auth-dir",
    type=click.Path(path_type=pathlib.Path),
    help="Directory to create auth properties files in.",
)
def auth_create_prop_files(auth_dir: pathlib.Path | None) -> None:
    """Create authorization properties files with no password."""
    create_properties_files(auth_dir)


@main.group()
@click.argument("site", type=click.Choice(SITES, case_sensitive=False))
@click.pass_context
def topics(ctx: click.Context, site: str) -> None:
    """Commands for Kafka topics."""
    ctx.obj = {
        "site": site,
    }


@topics.command("list")
@click.option(
    "--regex", type=str, help="Pass a regular expression to filter the topic list."
)
@click.option("--name", type=str, help="Pass a name to filter the topic list.")
@click.pass_context
def topics_list(ctx: click.Context, regex: str | None, name: str | None) -> None:
    """List the available Kafka topics."""
    if regex is not None and name is not None:
        raise click.exceptions.UsageError(
            "Cannot use regex and name options simultaneously.", ctx
        )
    topics = get_topics(ctx.obj)
    filtered_topics(
        topics, ListTopicsOpts(regex=regex, name=name, name_list=None, name_file=None)
    )


@topics.command("delete")
@click.option(
    "--regex", type=str, help="Pass a regular expression to filter the topic list."
)
@click.option("--name", type=str, help="Pass a name to filter the topic list.")
@click.option(
    "--name-list",
    type=str,
    help="Comma-delimited list of names to filter the topic list.",
)
@click.option("--name-file", type=pathlib.Path, help="File ")
@click.pass_context
def topics_delete(
    ctx: click.Context,
    regex: str | None,
    name: str | None,
    name_list: str | None,
    name_file: pathlib.Path | None,
) -> None:
    """Delete topics."""
    check_args = [
        regex is not None,
        name is not None,
        name_list is not None,
        name_file is not None,
    ]
    if sum(check_args) > 1:
        raise click.exceptions.UsageError(
            "Cannot use regex, name, name-list and name-file options simultaneously.",
            ctx,
        )
    if not sum(check_args):
        raise click.exceptions.UsageError(
            "Must provide one of the following options: --regex, --name, --name-list or --name-file.",
            ctx,
        )

    message = f"Delete all requested topics from {ctx.obj['site']}"
    acknowledge_deletion(message)

    topics = filter_topics(
        ctx.obj,
        ListTopicsOpts(
            regex=regex, name=name, name_list=name_list, name_file=name_file
        ),
    )
    done, not_done = delete_topics(ctx.obj, topics)
    summerize_deletion("topics", done, not_done)


@topics.command("set-partitions")
@click.argument("csc")
@click.argument("number")
@click.pass_context
def topics_set_partitions(ctx: click.Context, csc: str, number: str) -> None:
    """Change the number of partitions on CSC telemetry topics."""
    topics = filter_topics(
        ctx.obj, ListTopicsOpts(regex=None, name=csc, name_list=None, name_file=None)
    )
    done, not_done = set_partitions_topics(ctx.obj, topics, csc, int(number))
    num_done = len(done)
    num_not_done = len(not_done)
    print(f"Found {num_done + num_not_done} topics to modify")
    print(f"{num_done} modified successfully, {num_not_done} not successfully modified")


@main.group()
@click.argument("site", type=click.Choice(SITES, case_sensitive=False))
@click.option(
    "--timeout",
    type=int,
    default=30000,
    help="Set the timeout for the kafka commands in milliseconds.",
)
@click.pass_context
def consumers(ctx: click.Context, site: str, timeout: int) -> None:
    """Commands for Kafka consumers and consumer groups."""
    ctx.obj = {
        "site": site,
        "timeout": timeout,
    }


@consumers.command("summary")
@click.option(
    "--no-telegraph-filter",
    is_flag=True,
    help="Don't filter telegraph consumers from list.",
)
@click.pass_context
def consumers_summary(ctx: click.Context, no_telegraph_filter: bool) -> None:
    """Summarize number of consumer groups."""
    summary = summarize_consumers(ctx.obj, no_telegraph_filter=no_telegraph_filter)
    consumer_summary(summary)


@consumers.command("list")
@click.option(
    "--no-connector-filter",
    is_flag=True,
    help="Don't filter connector consumers from list.",
)
@click.option(
    "--all",
    "consumer_state",
    flag_value="All",
    default=True,
    help="Show all consumers.",
)
@click.option(
    "--active", "consumer_state", flag_value="Stable", help="Show active consumers."
)
@click.option(
    "--inactive", "consumer_state", flag_value="Empty", help="Show inactive consumers."
)
@click.option(
    "--regex", type=str, help="Pass a regular expression to filter the consumer list."
)
@click.option(
    "--regex-inclusive",
    "regex_mode",
    type=str,
    flag_value="Inclusive",
    help="Include consumers that match regex in list.",
)
@click.option(
    "--regex-exclusive",
    "regex_mode",
    default=True,
    type=str,
    flag_value="Exclusive",
    help="Exclude consumers that match regex from list (default mode).",
)
@click.pass_context
def consumers_list(
    ctx: click.Context,
    regex: str | None,
    regex_mode: str,
    no_connector_filter: bool,
    consumer_state: str,
) -> None:
    """Filter the present consumer groups."""
    consumers, max_length = list_consumers(
        ctx.obj,
        ListConsumerOpts(
            regex=regex,
            regex_mode=regex_mode,
            no_connector_filter=no_connector_filter,
            consumer_state=consumer_state,
        ),
    )
    two_column_table(consumers, max_length)


@consumers.command("delete")
@click.option(
    "--delete-connectors",
    is_flag=True,
    help="Allow the deletion of telegraf consumers.",
)
@click.option(
    "--regex",
    type=str,
    help="Pass a regular expression to filter the consumers to be deleted.",
)
@click.option(
    "--regex-inclusive",
    "regex_mode",
    type=str,
    flag_value="Inclusive",
    help="Delete consumers that match regex.",
)
@click.option(
    "--regex-exclusive",
    "regex_mode",
    default=True,
    type=str,
    flag_value="Exclusive",
    help="Delete consumers that do not match regex (default mode).",
)
@click.pass_context
def consumers_delete(
    ctx: click.Context, regex: str | None, regex_mode: str, delete_connectors: bool
) -> None:
    """Delete all inactive consumer groups"""
    consumers, _ = list_consumers(
        ctx.obj,
        ListConsumerOpts(
            regex=regex,
            regex_mode=regex_mode,
            no_connector_filter=delete_connectors,
            consumer_state="Empty",
        ),
    )
    consumers_to_delete = [x[0] for x in consumers]
    if not len(consumers_to_delete):
        print("No consumers to delete.")
        return
    done, not_done = delete_consumers(ctx.obj, consumers_to_delete)
    summerize_deletion("consumers", done, not_done)


@consumers.command("describe")
@click.argument("consumers", type=str)
@click.pass_context
def consumers_describe(ctx: click.Context, consumers: str) -> None:
    """Describe the given set of consumer groups."""
    if "," in consumers:
        consumer_list = consumers.split(",")
    else:
        consumer_list = [consumers]
    descrs = describe_consumers(ctx.obj, consumer_list)
    consumer_descriptions(descrs)


@main.group()
@click.argument("site", type=click.Choice(SITES, case_sensitive=False))
@click.pass_context
def config(ctx: click.Context, site: str) -> None:
    """Commands for configurations."""
    ctx.obj = {
        "site": site,
    }


@config.command("brokers")
@click.argument("broker-id", type=str)
@click.pass_context
def config_brokers(ctx: click.Context, broker_id: str) -> None:
    """Show the broker configuration."""
    configs = show_broker_config(ctx.obj, broker_id)
    list_broker_configs(broker_id, configs)
