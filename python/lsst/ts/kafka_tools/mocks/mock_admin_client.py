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
from typing import Any, Optional

from confluent_kafka import OFFSET_INVALID, ConsumerGroupState, TopicPartition
from confluent_kafka.admin import (
    ClusterMetadata,
    ConfigEntry,
    ConfigResource,
    ConfigSource,
    ConsumerGroupDescription,
    ConsumerGroupListing,
    ListConsumerGroupsResult,
    MemberAssignment,
    MemberDescription,
    NewPartitions,
    OffsetSpec,
    PartitionMetadata,
    TopicMetadata,
    _ConsumerGroupTopicPartitions,
)


class _MockListOffsetsResultInfo:
    """Simple stand-in for ListOffsetsResultInfo."""

    def __init__(self, offset: int) -> None:
        self.offset = offset


__all__ = ["MockAdminClient"]


class MockAdminClient:

    # committed offsets: group_id -> [(topic, partition, offset), ...]
    _mock_committed: dict[str, list[tuple[str, int, int]]] = {
        "consumer1": [("topic1", 0, 5), ("topic2", 0, 3)],
        "consumer2": [("topic1", 0, 8)],
    }
    # end (log) offsets: (topic, partition) -> offset
    _mock_end_offsets: dict[tuple[str, int], int] = {
        ("topic1", 0): 10,
        ("topic2", 0): 3,
    }

    def __init__(self) -> None:
        """Class constructor."""
        self.cluster_md = ClusterMetadata()
        self.cgl: list[ConsumerGroupListing] = []
        self.cgd: list[ConsumerGroupDescription] = []
        self.empty_consumers: list[str] = []
        self.broker_config: dict[str, ConfigEntry] = {}
        self._create_topics()
        self._create_consumers()
        self._create_broker_config()

    def _create_broker_config(self) -> None:
        """Create a broker configuration."""
        parameters: list[tuple[str, Any, list[tuple[int, Any]]]] = [
            (
                "log.message.timestamp.type",
                "LogAppendTime",
                [(4, None), (5, "CreateTime")],
            ),
            ("group.min.session.timeout.ms", 60000, [(5, None)]),
        ]

        for parameter in parameters:
            synonyms = {}
            for synonym in parameter[2]:
                value = parameter[1] if synonym[1] is None else synonym[1]
                synonyms[parameter[0]] = ConfigEntry(
                    parameter[0], value, ConfigSource(synonym[0])
                )

            self.broker_config[parameter[0]] = ConfigEntry(
                parameter[0],
                parameter[1],
                is_sensitive=False,
                synonyms=synonyms,
            )

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

        host = "/10.42.6.34"
        cg_md_cid1 = "consumer1"
        cg_md_mid1 = f"{cg_md_cid1}-{cg_md_cid1}"
        ma1 = [
            TopicPartition("topic1"),
            TopicPartition("topic2"),
            TopicPartition("topic3"),
        ]
        mdl1 = [
            MemberDescription(
                client_id=cg_md_cid1,
                member_id=cg_md_mid1,
                host=host,
                assignment=MemberAssignment(ma1),
            )
        ]

        cg_md_cid2 = "consumer5"
        cg_md_mid2 = f"{cg_md_cid2}-{cg_md_cid2}"
        ma2 = [
            TopicPartition("lsst.sal.ATAOS.logevent_heartbeat"),
            TopicPartition("lsst.sal.ATAOS.timestamp"),
        ]
        mdl2 = [
            MemberDescription(
                client_id=cg_md_cid2,
                member_id=cg_md_mid2,
                host=host,
                assignment=MemberAssignment(ma2),
            )
        ]

        cgds = [
            ConsumerGroupDescription(
                group_id=cg_md_cid1,
                is_simple_consumer_group=True,
                members=mdl1,
                partition_assignor=None,
                state=ConsumerGroupState.STABLE,
                coordinator=None,
            ),
            ConsumerGroupDescription(
                group_id=cg_md_cid2,
                is_simple_consumer_group=True,
                members=mdl2,
                partition_assignor=None,
                state=ConsumerGroupState.STABLE,
                coordinator=None,
            ),
        ]
        self.cgd.extend(cgds)

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
            "lsst.sal.ATAOS.command_start",
            "lsst.sal.ATAOS.logevent_heartbeat",
            "lsst.sal.ATAOS.logevent_summaryState",
            "lsst.sal.ATAOS.timestamp",
        ]

        topics = {}
        for topic_name in topic_names:
            tm = TopicMetadata()
            tm.topic = topic_name
            tm.partitions = partitions
            topics[topic_name] = tm

        self.cluster_md.topics = topics

    def create_partitions(
        self,
        partitions: list[NewPartitions],
    ) -> dict[str, concurrent.futures.Future]:
        """Expand partitions for topics."""
        result = {}
        for partition in partitions:
            f: concurrent.futures.Future = concurrent.futures.Future()
            f.set_result(None)
            result[partition.topic] = f
        return result

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

    def delete_topics(self, topics: list[str]) -> dict[str, concurrent.futures.Future]:
        """Delete topics."""
        result = {}
        for topic in topics:
            f: concurrent.futures.Future = concurrent.futures.Future()
            f.set_result(None)
            result[topic] = f
        return result

    def describe_configs(
        self,
        resources: list[ConfigResource],
    ) -> dict[ConfigResource, concurrent.futures.Future]:
        """Describe configs."""
        result = {}
        for resource in resources:
            f: concurrent.futures.Future = concurrent.futures.Future()
            f.set_result(self.broker_config)
            result[resource] = f
        return result

    def describe_consumer_groups(
        self, group_ids: list[str]
    ) -> dict[str, concurrent.futures.Future]:
        """Describe consumer groups."""
        result = {}
        for group_id in group_ids:
            f: concurrent.futures.Future = concurrent.futures.Future()
            f.set_result(next((x for x in self.cgd if x.group_id == group_id), None))
            result[group_id] = f
        return result

    def incremental_alter_configs(
        self,
        resources: list[ConfigResource],
    ) -> dict[ConfigResource, concurrent.futures.Future]:
        """Incrementally alter configuration."""
        result = {}
        for resource in resources:
            f: concurrent.futures.Future = concurrent.futures.Future()
            f.set_result(None)
            result[resource] = f
        return result

    def list_consumer_groups(
        self, states: Optional[set[ConsumerGroupState]] = None
    ) -> concurrent.futures.Future:
        """List consumer groups."""
        if states is None:
            consumers = list(self.cgl)
        else:
            consumers = [x for x in self.cgl if x.state in states]
        lcgr = ListConsumerGroupsResult(consumers)
        f: concurrent.futures.Future = concurrent.futures.Future()
        f.set_result(lcgr)
        return f

    def list_consumer_group_offsets(
        self, requests: list[_ConsumerGroupTopicPartitions]
    ) -> dict[str, concurrent.futures.Future]:
        """Return committed offsets for each requested consumer group."""
        result = {}
        for req in requests:
            gid = req.group_id
            tps = [
                TopicPartition(t, p, o) for t, p, o in self._mock_committed.get(gid, [])
            ]
            f: concurrent.futures.Future = concurrent.futures.Future()
            f.set_result(_ConsumerGroupTopicPartitions(gid, tps))
            result[gid] = f
        return result

    def list_offsets(
        self, topic_partitions: dict[TopicPartition, OffsetSpec]
    ) -> dict[TopicPartition, concurrent.futures.Future]:
        """Return the latest offset for each requested topic-partition."""
        result = {}
        for tp in topic_partitions:
            end = self._mock_end_offsets.get((tp.topic, tp.partition), OFFSET_INVALID)
            f: concurrent.futures.Future = concurrent.futures.Future()
            f.set_result(_MockListOffsetsResultInfo(end))
            result[tp] = f
        return result

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
