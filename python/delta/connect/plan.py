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

from typing import Optional

import delta.connect.proto as proto

from pyspark.sql.connect.client import SparkConnectClient
from pyspark.sql.connect.plan import LogicalPlan
import pyspark.sql.connect.proto as spark_proto


class DeltaLogicalPlan(LogicalPlan):
    def __init__(self, child: Optional[LogicalPlan]) -> None:
        super().__init__(child)

    def plan(self, session: SparkConnectClient) -> spark_proto.Relation:
        plan = spark_proto.Relation()
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
