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

import concurrent.futures

from confluent_kafka import ConsumerGroupState
from confluent_kafka.admin import (
    ClusterMetadata,
    ConsumerGroupListing,
    ListConsumerGroupsResult,
    PartitionMetadata,
    TopicMetadata,
)

__all__ = ["MockAdminClient"]


class MockAdminClient:

    def __init__(self) -> None:
        """Class constructor."""
        self.cluster_md = ClusterMetadata()
        self.cgl: list[ConsumerGroupListing] = []
        self.empty_consumers: list[str] = []
        self._create_topics()
        self._create_consumers()

    def _create_consumers(self) -> None:
        """Create consumers."""
        cgls = [
            ConsumerGroupListing(
                "telegraf-kafka-maintel", False, state=ConsumerGroupState.STABLE
            ),
            ConsumerGroupListing("consumer1", False, state=ConsumerGroupState.STABLE),
            ConsumerGroupListing("consumer5", False, state=ConsumerGroupState.STABLE),
            ConsumerGroupListing("consumer2", False, state=ConsumerGroupState.STABLE),
            ConsumerGroupListing("consumer6", False, state=ConsumerGroupState.STABLE),
            ConsumerGroupListing("consumer9", False, state=ConsumerGroupState.STABLE),
            ConsumerGroupListing("consumer11", False, state=ConsumerGroupState.STABLE),
            ConsumerGroupListing("consumer21", False, state=ConsumerGroupState.STABLE),
        ]
        self.cgl.extend(cgls)
        cgle = [
            ConsumerGroupListing("consumer13", False, state=ConsumerGroupState.EMPTY),
            ConsumerGroupListing("consumer10", False, state=ConsumerGroupState.EMPTY),
        ]
        self.empty_consumers = [x.group_id for x in cgle]
        self.cgl.extend(cgle)

    def _create_topics(self) -> None:
        """Create topics."""
        pm = PartitionMetadata()
        pm.id = 0
        partitions = {pm.id: pm}

        topic_names = [
            "topic1.attribute1",
            "topic1.attribute2",
            "topic1.attribute3",
            "topic2.attribute1",
            "topic2.attribute2",
            "topic2.attribute3",
            "lsst.sal.ATAOS.logevent_heartbeat",
            "lsst.sal.ATAOS.logevent_summaryState",
        ]

        topics = {}
        for topic_name in topic_names:
            tm = TopicMetadata()
            tm.topic = topic_name
            tm.partitions = partitions
            topics[topic_name] = tm

        self.cluster_md.topics = topics

    def delete_consumer_groups(
        self, consumer_groups: list[str]
    ) -> dict[str, concurrent.futures.Future]:
        """Delete consumer groups."""
        result = {}
        for consumer_group in consumer_groups:
            f: concurrent.futures.Future = concurrent.futures.Future()
            f.set_result(None)
            result[consumer_group] = f
        return result

    def list_consumer_groups(
        self, states: set[ConsumerGroupState]
    ) -> concurrent.futures.Future:
        """List consumer groups."""
        consumers = [x for x in self.cgl if x.state in states]
        lcgr = ListConsumerGroupsResult(consumers)
        f: concurrent.futures.Future = concurrent.futures.Future()
        f.set_result(lcgr)
        return f

    def list_topics(self) -> ClusterMetadata:
        """List topics creation."""
        return self.cluster_md

    def set_empty_consumers_to_stable(self) -> None:
        """Set all empty consumers to stable state."""
        for consumer in self.cgl:
            if consumer.state == ConsumerGroupState.EMPTY:
                consumer.state = ConsumerGroupState.STABLE

    def reset_empty_consumers(self) -> None:
        """Return empty consumers to original state."""
        for consumer in self.cgl:
            if consumer.group_id in self.empty_consumers:
                consumer.state = ConsumerGroupState.EMPTY
