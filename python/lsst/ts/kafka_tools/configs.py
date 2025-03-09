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

from confluent_kafka.admin import ConfigEntry, ConfigResource

from .helpers import generate_admin_client
from .type_hints import ScriptContext

__all__ = ["show_broker_config"]


def show_broker_config(ctxobj: ScriptContext, broker_id: str) -> list[ConfigEntry]:
    """Retrieve the configuration of a broker.

    Parameters
    ----------
    ctxobj : ScriptContext
        The context object from the CLI invocation.
    broker_id : str
        The broker ID, usually a number given as a string.

    Returns
    -------
    list[ConfigEntry]
        The configuration parameters of the broker.
    """
    client = generate_admin_client(ctxobj["site"])
    broker = ConfigResource(ConfigResource.Type.BROKER, broker_id)
    broker_config = client.describe_configs([broker])
    result = concurrent.futures.wait(list(broker_config.values()))
    config_entry_dict = result.done.pop().result()
    items = sorted(config_entry_dict.items())
    config_entries: list[ConfigEntry] = [x[1] for x in items]
    return config_entries
