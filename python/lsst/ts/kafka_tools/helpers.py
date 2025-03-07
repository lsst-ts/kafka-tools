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

import os
import pathlib
import sys

from confluent_kafka.admin import AdminClient
from jproperties import Properties

__all__ = [
    "acknowledge_deletion",
    "check_for_exception",
    "create_config",
    "generate_admin_client",
]


def acknowledge_deletion(message: str) -> None:
    """Prompt for making sure a deletion is necessary.

    Parameters
    ----------
    message : `str`
        The prompt message for the deletion acknowledgement.
    """
    print(f"{message} Are you sure?")
    response = input("y to proceed, any other key to exit: ")
    if response != "y":
        print("Exiting")
        sys.exit(255)
    else:
        print("Proceeding with deletion.")


def check_for_exception(out: str) -> None:
    """Check for exceptions in output. Exit program if found.

    Parameters
    ----------
    out : `str`
        The output to check for exceptions.
    """
    if "Exception" in out:
        print(out)
        sys.exit(254)


def create_config(site_name: str) -> Properties:
    """Create configuration for AdminClient instance.

    Parameters
    ----------
    site_name : str
        The name of the accessed site.

    Returns
    -------
    Properties
        The site specific access configuration.
    """
    if site_name == "envvar":
        props = Properties()
        props["security.protocol"] = os.environ["LSST_KAFKA_SECURITY_PROTOCOL"]
        props["sasl.mechanism"] = os.environ["LSST_KAFKA_SECURITY_MECHANISM"]
        props["sasl.username"] = os.environ["LSST_KAFKA_SECURITY_USERNAME"]
        props["sasl.password"] = os.environ["LSST_KAFKA_SECURITY_PASSWORD"]
        props["bootstrap.servers"] = os.environ["LSST_KAFKA_BROKER_ADDR"]
    else:
        auth_config_file = (
            pathlib.Path("~/").expanduser()
            / ".auth"
            / f"kafka-aclient-{site_name}.properties"
        )
        props = Properties()
        with auth_config_file.open("rb") as acf:
            props.load(acf, encoding="utf-8")
    return props


def generate_admin_client(site_name: str) -> AdminClient:
    """Generate an AdminClient instance for a give site.

    Parameters
    ----------
    site_name : str
        The name of the accessed site.

    Returns
    -------
    AdminClient
        The site specific AdminClient instance.
    """
    client_config = create_config(site_name)
    ac = AdminClient(client_config.properties)
    return ac
