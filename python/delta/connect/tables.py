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

from typing import Dict, Optional

from delta.connect.plan import DeltaScan
import delta.connect.proto as proto
from delta.tables import DeltaTable as LocalDeltaTable

from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import LogicalPlan, SubqueryAlias
from pyspark.sql.connect.session import SparkSession


class DeltaTable(object):
    def __init__(
        self,
        spark: SparkSession,
        path: Optional[str] = None,
        tableOrViewName: Optional[str] = None,
        hadoopConf: Dict[str, str] = dict(),
        plan: Optional[LogicalPlan] = None
    ) -> None:
        self._spark = spark
        self._path = path
        self._tableOrViewName = tableOrViewName
        self._hadoopConf = hadoopConf
        if plan is not None:
            self._plan = plan
        else:
            self._plan = DeltaScan(self._to_proto())

    def toDF(self) -> DataFrame:
        return DataFrame(self._plan, session=self._spark)

    def alias(self, aliasName: str) -> "DeltaTable":
        return DeltaTable(
            self._spark,
            self._path,
            self._tableOrViewName,
            self._hadoopConf,
            SubqueryAlias(self._plan, aliasName)
        )

    @classmethod
    def forPath(
        cls,
        sparkSession: SparkSession,
        path: str,
        hadoopConf: Dict[str, str] = dict()
    ) -> "DeltaTable":
        assert sparkSession is not None
        return DeltaTable(sparkSession, path=path, hadoopConf=hadoopConf)

    @classmethod
    def forName(
        cls, sparkSession: SparkSession, tableOrViewName: str
    ) -> "DeltaTable":
        assert sparkSession is not None
        return DeltaTable(sparkSession, tableOrViewName=tableOrViewName)

    def _to_proto(self) -> proto.DeltaTable:
        result = proto.DeltaTable()
        if self._path is not None:
            result.path.path = self._path
        if self._tableOrViewName is not None:
            result.table_or_view_name = self._tableOrViewName
        return result


DeltaTable.__doc__ = LocalDeltaTable.__doc__
DeltaTable.toDF.__doc__ = LocalDeltaTable.toDF.__doc__
DeltaTable.alias.__doc__ = LocalDeltaTable.alias.__doc__
DeltaTable.forPath.__func__.__doc__ = LocalDeltaTable.forPath.__doc__
DeltaTable.forName.__func__.__doc__ = LocalDeltaTable.forName.__doc__
