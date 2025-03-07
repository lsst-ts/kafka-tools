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

from jproperties import Properties

from .constants import SITES

__all__ = ["create_properties_files"]


def create_properties_files(auth_dir: pathlib.Path | None) -> None:

    if auth_dir is None:
        auth_dir = pathlib.Path("~/.auth")
    auth_dir = auth_dir.expanduser()

    if not auth_dir.exists():
        auth_dir.mkdir()

    for site in SITES:
        if site == "envvar":
            continue
        props = Properties()
        if site != "local":
            props["security.protocol"] = "SASL_SSL"
            props["sasl.mechanism"] = "SCRAM-SHA-512"
            props["sasl.username"] = "<replace-me>"
            props["sasl.password"] = "<replace-me>"
        props["bootstrap.servers"] = "<replace-me>"

        prop_file = auth_dir / f"kafka-aclient-{site}.properties"
        with prop_file.open("wb") as pf:
            props.store(pf, encoding="utf-8")
