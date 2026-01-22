#
# Copyright (2024) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from typing import cast, Dict, List, Optional, Union

import delta.connect.proto as proto

from pyspark.sql.connect.client import SparkConnectClient
from pyspark.sql.connect.column import Column
from pyspark.sql.connect.plan import LogicalPlan
import pyspark.sql.connect.proto as spark_proto
from pyspark.sql.connect.types import pyspark_types_to_proto_types
from pyspark.sql.types import StructType


class DeltaLogicalPlan(LogicalPlan):
    def __init__(self, child: Optional[LogicalPlan]) -> None:
        super().__init__(child)

    def plan(self, session: SparkConnectClient) -> spark_proto.Relation:
        plan = self._create_proto_relation()
        plan.extension.Pack(self.to_delta_relation(session))
        return plan

    def to_delta_relation(self, session: SparkConnectClient) -> proto.DeltaRelation:
        ...

    def command(self, session: SparkConnectClient) -> spark_proto.Command:
        command = spark_proto.Command()
        command.extension.Pack(self.to_delta_command(session))
        return command

    def to_delta_command(self, session: SparkConnectClient) -> proto.DeltaCommand:
        ...


class DeltaScan(DeltaLogicalPlan):
    def __init__(self, table: proto.DeltaTable) -> None:
        super().__init__(None)
        self._table = table

    def to_delta_relation(self, client: SparkConnectClient) -> proto.DeltaRelation:
        relation = proto.DeltaRelation()
        relation.scan.table.CopyFrom(self._table)
        return relation


class Generate(DeltaLogicalPlan):
    def __init__(
        self,
        table: proto.DeltaTable,
        mode: str
    ) -> None:
        super().__init__(None)
        self._mode = mode
        self._table = table

    def to_delta_command(self, client: SparkConnectClient) -> proto.DeltaCommand:
        command = proto.DeltaCommand()
        command.generate.table.CopyFrom(self._table)
        command.generate.mode = self._mode
        return command


class DeleteFromTable(DeltaLogicalPlan):
    def __init__(self, target: Optional[LogicalPlan], condition: Optional[Column]) -> None:
        super().__init__(target)
        self._target = cast(LogicalPlan, target)
        self._condition = condition

    def to_delta_relation(self, session: SparkConnectClient) -> proto.DeltaRelation:
        relation = proto.DeltaRelation()
        relation.delete_from_table.target.CopyFrom(self._target.plan(session))
        if self._condition is not None:
            relation.delete_from_table.condition.CopyFrom(self._condition.to_plan(session))
        return relation


class Assignment:
    def __init__(self, field: Column, value: Column) -> None:
        self._field = field
        self._value = value

    def to_proto(self, session: SparkConnectClient) -> proto.Assignment:
        assignment = proto.Assignment()
        assignment.field.CopyFrom(self._field.to_plan(session))
        assignment.value.CopyFrom(self._value.to_plan(session))
        return assignment


class UpdateTable(DeltaLogicalPlan):
    def __init__(
        self,
        target: Optional[LogicalPlan],
        condition: Optional[Column],
        assignments: List[Assignment],
    ) -> None:
        super().__init__(target)
        self._target = cast(LogicalPlan, target)
        self._condition = condition
        self._assignments = assignments

    def to_delta_relation(self, session: SparkConnectClient) -> proto.DeltaRelation:
        relation = proto.DeltaRelation()
        relation.update_table.target.CopyFrom(self._target.plan(session))
        if self._condition is not None:
            relation.update_table.condition.CopyFrom(self._condition.to_plan(session))
        relation.update_table.assignments.extend(
            [assignment.to_proto(session) for assignment in self._assignments]
        )
        return relation


class MergeAction(object):
    def __init__(self, condition: Optional[Column]) -> None:
        self._condition = condition

    def to_proto(self, session: SparkConnectClient) -> proto.MergeIntoTable.Action:
        action = proto.MergeIntoTable.Action()
        if self._condition is not None:
            action.condition.CopyFrom(self._condition.to_plan(session))
        return action


class UpdateAction(MergeAction):
    def __init__(
        self,
        condition: Optional[Column],
        assignments: List[Assignment],
    ) -> None:
        super().__init__(condition)
        self._assignments = assignments

    def to_proto(self, session: SparkConnectClient) -> proto.MergeIntoTable.Action:
        action = super().to_proto(session)
        action.update_action.assignments.extend(
            [assignment.to_proto(session) for assignment in self._assignments]
        )
        return action


class UpdateStarAction(MergeAction):
    def __init__(self, condition: Optional[Column]) -> None:
        super().__init__(condition)

    def to_proto(self, session: SparkConnectClient) -> proto.MergeIntoTable.Action:
        action = super().to_proto(session)
        action.update_star_action.SetInParent()
        return action


class DeleteAction(MergeAction):
    def __init__(self, condition: Optional[Column]) -> None:
        super().__init__(condition)

    def to_proto(self, session: SparkConnectClient) -> proto.MergeIntoTable.Action:
        action = super().to_proto(session)
        action.delete_action.SetInParent()
        return action


class InsertAction(MergeAction):
    def __init__(
        self,
        condition: Optional[Column],
        assignments: List[Assignment],
    ) -> None:
        super().__init__(condition)
        self._assignments = assignments

    def to_proto(self, session: SparkConnectClient) -> proto.MergeIntoTable.Action:
        action = super().to_proto(session)
        action.insert_action.assignments.extend(
            [assignment.to_proto(session) for assignment in self._assignments]
        )
        return action


class InsertStarAction(MergeAction):
    def __init__(self, condition: Optional[Column]) -> None:
        super().__init__(condition)

    def to_proto(self, session: SparkConnectClient) -> proto.MergeIntoTable.Action:
        action = super().to_proto(session)
        action.insert_star_action.SetInParent()
        return action


class MergeIntoTable(DeltaLogicalPlan):
    def __init__(
        self,
        target: Optional[LogicalPlan],
        source: LogicalPlan,
        condition: Column,
        matched_actions: List[MergeAction],
        not_matched_actions: List[MergeAction],
        not_matched_by_source_actions: List[MergeAction],
        with_schema_evolution: Optional[bool]
    ) -> None:
        super().__init__(target)
        self._target = cast(LogicalPlan, target)
        self._source = source
        self._condition = condition
        self._matched_actions = matched_actions
        self._not_matched_actions = not_matched_actions
        self._not_matched_by_source_actions = not_matched_by_source_actions
        self._with_schema_evolution = with_schema_evolution or False

    def to_delta_relation(self, session: SparkConnectClient) -> proto.DeltaRelation:
        relation = proto.DeltaRelation()
        relation.merge_into_table.target.CopyFrom(self._target.plan(session))
        relation.merge_into_table.source.CopyFrom(self._source.plan(session))
        relation.merge_into_table.condition.CopyFrom(self._condition.to_plan(session))
        relation.merge_into_table.matched_actions.extend(
            [action.to_proto(session) for action in self._matched_actions]
        )
        relation.merge_into_table.not_matched_actions.extend(
            [action.to_proto(session) for action in self._not_matched_actions]
        )
        relation.merge_into_table.not_matched_by_source_actions.extend(
            [action.to_proto(session) for action in self._not_matched_by_source_actions]
        )
        relation.merge_into_table.with_schema_evolution = self._with_schema_evolution
        return relation


class Vacuum(DeltaLogicalPlan):
    def __init__(
        self,
        table: proto.DeltaTable,
        retentionHours: Optional[float]
    ) -> None:
        super().__init__(None)
        self._table = table
        self._retentionHours = retentionHours

    def to_delta_command(self, client: SparkConnectClient) -> proto.DeltaCommand:
        command = proto.DeltaCommand()
        command.vacuum_table.table.CopyFrom(self._table)
        if self._retentionHours is not None:
            command.vacuum_table.retention_hours = self._retentionHours
        return command


class DescribeHistory(DeltaLogicalPlan):
    def __init__(self, table: proto.DeltaTable) -> None:
        super().__init__(None)
        self._table = table

    def to_delta_relation(self, session: SparkConnectClient) -> proto.DeltaRelation:
        relation = proto.DeltaRelation()
        relation.describe_history.table.CopyFrom(self._table)
        return relation


class DescribeDetail(DeltaLogicalPlan):
    def __init__(self, table: proto.DeltaTable) -> None:
        super().__init__(None)
        self._table = table

    def to_delta_relation(self, client: SparkConnectClient) -> proto.DeltaRelation:
        relation = proto.DeltaRelation()
        relation.describe_detail.table.CopyFrom(self._table)
        return relation


class ConvertToDelta(DeltaLogicalPlan):
    def __init__(
        self,
        identifier: str,
        partitionSchema: Optional[Union[str, StructType]]
    ) -> None:
        super().__init__(None)
        self._identifier = identifier
        self._partitionSchema = partitionSchema

    def to_delta_relation(self, client: SparkConnectClient) -> proto.DeltaRelation:
        relation = proto.DeltaRelation()
        relation.convert_to_delta.identifier = self._identifier
        if self._partitionSchema is not None:
            if isinstance(self._partitionSchema, str):
                relation.convert_to_delta.partition_schema_string = self._partitionSchema
            if isinstance(self._partitionSchema, StructType):
                relation.convert_to_delta.partition_schema_struct.CopyFrom(
                    pyspark_types_to_proto_types(self._partitionSchema)
                )
        return relation


class IsDeltaTable(DeltaLogicalPlan):
    def __init__(self, path: str):
        super().__init__(None)
        self._path = path

    def to_delta_relation(self, session: SparkConnectClient) -> proto.DeltaRelation:
        relation = proto.DeltaRelation()
        relation.is_delta_table.path = self._path
        return relation


class CreateDeltaTable(DeltaLogicalPlan):
    def __init__(
        self,
        mode: proto.CreateDeltaTable.Mode,
        tableName: Optional[str],
        location: Optional[str],
        comment: Optional[str],
        columns: List[proto.CreateDeltaTable.Column],
        partitioningColumns: List[str],
        properties: Dict[str, str],
        clusteringColumns: List[str]
    ) -> None:
        super().__init__(None)
        self._mode = mode
        self._tableName = tableName
        self._location = location
        self._comment = comment
        self._columns = columns
        self._partitioningColumns = partitioningColumns
        self._clusteringColumns = clusteringColumns
        self._properties = properties

    def to_delta_command(self, client: SparkConnectClient) -> proto.DeltaCommand:
        command = proto.DeltaCommand()
        command.create_delta_table.mode = self._mode
        if self._tableName is not None:
            command.create_delta_table.table_name = self._tableName
        if self._location is not None:
            command.create_delta_table.location = self._location
        if self._comment is not None:
            command.create_delta_table.comment = self._comment
        command.create_delta_table.columns.extend(self._columns)
        command.create_delta_table.partitioning_columns.extend(self._partitioningColumns)
        command.create_delta_table.clustering_columns.extend(self._clusteringColumns)
        for k, v in self._properties.items():
            command.create_delta_table.properties[k] = v
        return command


class UpgradeTableProtocol(DeltaLogicalPlan):
    def __init__(
        self,
        table: proto.DeltaTable,
        readerVersion: int,
        writerVersion: int
    ) -> None:
        super().__init__(None)
        self._table = table
        self._readerVersion = readerVersion
        self._writerVersion = writerVersion

    def to_delta_command(self, client: SparkConnectClient) -> proto.DeltaCommand:
        command = proto.DeltaCommand()
        command.upgrade_table_protocol.table.CopyFrom(self._table)
        command.upgrade_table_protocol.reader_version = self._readerVersion
        command.upgrade_table_protocol.writer_version = self._writerVersion
        return command


class AddFeatureSupport(DeltaLogicalPlan):
    def __init__(
        self,
        table: proto.DeltaTable,
        featureName: str
    ) -> None:
        super().__init__(None)
        self._table = table
        self._featureName = featureName

    def to_delta_command(self, client: SparkConnectClient) -> proto.DeltaCommand:
        command = proto.DeltaCommand()
        command.add_feature_support.table.CopyFrom(self._table)
        command.add_feature_support.feature_name = self._featureName
        return command


class DropFeatureSupport(DeltaLogicalPlan):
    def __init__(
        self,
        table: proto.DeltaTable,
        featureName: str,
        truncateHistory: Optional[bool]
    ) -> None:
        super().__init__(None)
        self._table = table
        self._featureName = featureName
        self._truncateHistory = truncateHistory

    def to_delta_command(self, client: SparkConnectClient) -> proto.DeltaCommand:
        command = proto.DeltaCommand()
        command.drop_feature_support.table.CopyFrom(self._table)
        command.drop_feature_support.feature_name = self._featureName
        if self._truncateHistory is not None:
            command.drop_feature_support.truncate_history = self._truncateHistory
        return command


class RestoreTable(DeltaLogicalPlan):
    def __init__(
        self,
        table: proto.DeltaTable,
        version: Optional[int] = None,
        timestamp: Optional[str] = None
    ) -> None:
        super().__init__(None)
        self._table = table
        self._version = version
        self._timestamp = timestamp

    def to_delta_relation(self, client: SparkConnectClient) -> proto.DeltaRelation:
        relation = proto.DeltaRelation()
        relation.restore_table.table.CopyFrom(self._table)
        if self._version is not None:
            relation.restore_table.version = self._version
        if self._timestamp is not None:
            relation.restore_table.timestamp = self._timestamp
        return relation


class OptimizeTable(DeltaLogicalPlan):
    def __init__(
        self,
        table: proto.DeltaTable,
        partitionFilters: List[str],
        zOrderCols: List[str]
    ) -> None:
        super().__init__(None)
        self._table = table
        self._partitionFilters = partitionFilters
        self._zOrderCols = zOrderCols

    def to_delta_relation(self, client: SparkConnectClient) -> proto.DeltaRelation:
        relation = proto.DeltaRelation()
        relation.optimize_table.table.CopyFrom(self._table)
        relation.optimize_table.partition_filters.extend(self._partitionFilters)
        relation.optimize_table.zorder_columns.extend(self._zOrderCols)
        return relation


class CloneTable(DeltaLogicalPlan):
    def __init__(
        self,
        table: proto.DeltaTable,
        target: str,
        isShallow: bool,
        replace: bool,
        properties: Optional[Dict[str, str]],
        version: Optional[int] = None,
        timestamp: Optional[str] = None,
    ) -> None:
        super().__init__(None)
        self._table = table
        self._target = target
        self._isShallow = isShallow
        self._replace = replace
        self._properties = properties or {}
        self._version = version
        self._timestamp = timestamp

    def to_delta_command(self, client: SparkConnectClient) -> proto.DeltaCommand:
        command = proto.DeltaCommand()
        command.clone_table.table.CopyFrom(self._table)
        command.clone_table.target = self._target
        command.clone_table.is_shallow = self._isShallow
        command.clone_table.replace = self._replace
        for k, v in self._properties.items():
            command.clone_table.properties[k] = v
        if self._version is not None:
            command.clone_table.version = self._version
        if self._timestamp is not None:
            command.clone_table.timestamp = self._timestamp
        return command
