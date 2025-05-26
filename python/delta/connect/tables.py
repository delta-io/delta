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

from typing import (
    Any,
    Dict,
    Iterable,
    List,
    NoReturn,
    Optional,
    Tuple,
    Union,
    overload
)

from delta.connect._typing import (
    ColumnMapping,
    OptionalColumnMapping,
    ExpressionOrColumn,
    OptionalExpressionOrColumn
)
from delta.connect.plan import (
    AddFeatureSupport,
    Assignment,
    CloneTable,
    ConvertToDelta,
    CreateDeltaTable,
    DeleteAction,
    DeleteFromTable,
    DeltaScan,
    DescribeHistory,
    DescribeDetail,
    DropFeatureSupport,
    Generate,
    InsertAction,
    InsertStarAction,
    IsDeltaTable,
    MergeIntoTable,
    OptimizeTable,
    RestoreTable,
    UpdateAction,
    UpdateStarAction,
    UpdateTable,
    UpgradeTableProtocol,
    Vacuum,
)
import delta.connect.proto as proto
from delta.tables import (
    DeltaTable as LocalDeltaTable,
    DeltaTableBuilder as LocalDeltaTableBuilder,
    DeltaMergeBuilder as LocalDeltaMergeBuilder,
    DeltaOptimizeBuilder as LocalDeltaOptimizeBuilder,
    IdentityGenerator,
)

from pyspark.sql.connect import functions
from pyspark.sql.connect.column import Column
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import LogicalPlan, SubqueryAlias
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.connect.types import pyspark_types_to_proto_types
from pyspark.sql.types import DataType, StructField, StructType


class DeltaTable(object):
    __doc__ = LocalDeltaTable.__doc__

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

    toDF.__doc__ = LocalDeltaTable.toDF.__doc__

    def alias(self, aliasName: str) -> "DeltaTable":
        return DeltaTable(
            self._spark,
            self._path,
            self._tableOrViewName,
            self._hadoopConf,
            SubqueryAlias(self._plan, aliasName)
        )

    alias.__doc__ = LocalDeltaTable.alias.__doc__

    def generate(self, mode: str) -> None:
        command = Generate(self._to_proto(), mode).command(session=self._spark.client)
        self._spark.client.execute_command(command)

    generate.__doc__ = LocalDeltaTable.generate.__doc__

    def delete(self, condition: OptionalExpressionOrColumn = None) -> DataFrame:
        plan = DeleteFromTable(
            self._plan,
            DeltaTable._condition_to_column(condition)
        )
        df = DataFrame(plan, session=self._spark)
        return self._spark.createDataFrame(df.toPandas())

    delete.__doc__ = LocalDeltaTable.delete.__doc__

    @overload
    def update(
        self, condition: ExpressionOrColumn, set: ColumnMapping
    ) -> None:
        ...

    @overload
    def update(self, *, set: ColumnMapping) -> None:
        ...

    def update(
        self,
        condition: OptionalExpressionOrColumn = None,
        set: OptionalColumnMapping = None
    ) -> DataFrame:
        assignments = DeltaTable._dict_to_assignments(set, "'set'")
        condition = DeltaTable._condition_to_column(condition)
        plan = UpdateTable(
            self._plan,
            condition,
            assignments
        )
        df = DataFrame(plan, session=self._spark)
        return self._spark.createDataFrame(df.toPandas())

    update.__doc__ = LocalDeltaTable.update.__doc__

    def merge(
        self, source: DataFrame, condition: ExpressionOrColumn
    ) -> "DeltaMergeBuilder":
        if source is None:
            raise ValueError("'source' in merge cannot be None")
        elif not isinstance(source, DataFrame):
            raise TypeError("Type of 'source' in merge must be DataFrame. {}".format(type(source)))
        if condition is None:
            raise ValueError("'condition' in merge cannot be None")

        return DeltaMergeBuilder(
            self._spark,
            self._plan,
            source._plan,
            DeltaTable._condition_to_column(condition))

    merge.__doc__ = LocalDeltaTable.merge.__doc__

    def vacuum(self, retentionHours: Optional[float] = None) -> DataFrame:
        command = Vacuum(self._to_proto(), retentionHours).command(session=self._spark.client)
        self._spark.client.execute_command(command)
        return None  # TODO: Return empty DataFrame

    vacuum.__doc__ = LocalDeltaTable.vacuum.__doc__

    def history(self, limit: Optional[int] = None) -> DataFrame:
        df = DataFrame(DescribeHistory(self._to_proto()), session=self._spark)
        if limit is not None:
            df = df.limit(limit)
        return df

    history.__doc__ = LocalDeltaTable.history.__doc__

    def detail(self) -> DataFrame:
        return DataFrame(DescribeDetail(self._to_proto()), session=self._spark)

    detail.__doc__ = LocalDeltaTable.detail.__doc__

    @classmethod
    def convertToDelta(
        cls,
        sparkSession: SparkSession,
        identifier: str,
        partitionSchema: Optional[Union[str, StructType]] = None,
    ) -> "DeltaTable":
        assert sparkSession is not None

        pdf = DataFrame(
            ConvertToDelta(identifier, partitionSchema),
            session=sparkSession
        ).toPandas()
        identifier = pdf.iloc[0].iloc[0]

        return DeltaTable.forName(sparkSession, identifier)

    convertToDelta.__func__.__doc__ = LocalDeltaTable.convertToDelta.__doc__

    @classmethod
    def forPath(
        cls,
        sparkSession: SparkSession,
        path: str,
        hadoopConf: Dict[str, str] = dict()
    ) -> "DeltaTable":
        assert sparkSession is not None
        return DeltaTable(sparkSession, path=path, hadoopConf=hadoopConf)

    forPath.__func__.__doc__ = LocalDeltaTable.forPath.__doc__

    @classmethod
    def forName(
        cls, sparkSession: SparkSession, tableOrViewName: str
    ) -> "DeltaTable":
        assert sparkSession is not None
        return DeltaTable(sparkSession, tableOrViewName=tableOrViewName)

    forName.__func__.__doc__ = LocalDeltaTable.forName.__doc__

    @classmethod
    def create(
        cls, sparkSession: Optional[SparkSession] = None
    ) -> "DeltaTableBuilder":
        return DeltaTableBuilder(
            sparkSession,
            proto.CreateDeltaTable.Mode.MODE_CREATE)

    create.__func__.__doc__ = LocalDeltaTable.create.__doc__

    @classmethod
    def createIfNotExists(
        cls, sparkSession: Optional[SparkSession] = None
    ) -> "DeltaTableBuilder":
        return DeltaTableBuilder(
            sparkSession,
            proto.CreateDeltaTable.Mode.MODE_CREATE_IF_NOT_EXISTS)

    createIfNotExists.__func__.__doc__ = LocalDeltaTable.createIfNotExists.__doc__

    @classmethod
    def replace(
        cls, sparkSession: Optional[SparkSession] = None
    ) -> "DeltaTableBuilder":
        return DeltaTableBuilder(
            sparkSession,
            proto.CreateDeltaTable.Mode.MODE_REPLACE)

    replace.__func__.__doc__ = LocalDeltaTable.replace.__doc__

    @classmethod
    def createOrReplace(
        cls, sparkSession: Optional[SparkSession] = None
    ) -> "DeltaTableBuilder":
        return DeltaTableBuilder(
            sparkSession,
            proto.CreateDeltaTable.Mode.MODE_CREATE_OR_REPLACE)

    createOrReplace.__func__.__doc__ = LocalDeltaTable.createOrReplace.__doc__

    @classmethod
    def isDeltaTable(cls, sparkSession: SparkSession, identifier: str) -> bool:
        assert sparkSession is not None

        pdf = DataFrame(
            IsDeltaTable(identifier),
            session=sparkSession
        ).toPandas()
        return pdf.iloc[0].iloc[0]

    isDeltaTable.__func__.__doc__ = LocalDeltaTable.isDeltaTable.__doc__

    def upgradeTableProtocol(self, readerVersion: int, writerVersion: int) -> None:
        if not isinstance(readerVersion, int):
            raise ValueError("The readerVersion needs to be an integer but got '%s'." %
                             type(readerVersion))
        if not isinstance(writerVersion, int):
            raise ValueError("The writerVersion needs to be an integer but got '%s'." %
                             type(writerVersion))
        command = UpgradeTableProtocol(
            self._to_proto(),
            readerVersion,
            writerVersion
        ).command(session=self._spark.client)
        self._spark.client.execute_command(command)

    upgradeTableProtocol.__doc__ = LocalDeltaTable.upgradeTableProtocol.__doc__

    def addFeatureSupport(self, featureName: str) -> None:
        LocalDeltaTable._verify_type_str(featureName, "featureName")
        command = AddFeatureSupport(
            self._to_proto(),
            featureName
        ).command(session=self._spark.client)
        self._spark.client.execute_command(command)

    addFeatureSupport.__doc__ = LocalDeltaTable.addFeatureSupport.__doc__

    def dropFeatureSupport(self, featureName: str, truncateHistory: Optional[bool] = None) -> None:
        LocalDeltaTable._verify_type_str(featureName, "featureName")
        if truncateHistory is not None:
            LocalDeltaTable._verify_type_bool(truncateHistory, "truncateHistory")
        command = DropFeatureSupport(
            self._to_proto(),
            featureName,
            truncateHistory
        ).command(session=self._spark.client)
        self._spark.client.execute_command(command)

    dropFeatureSupport.__doc__ = LocalDeltaTable.dropFeatureSupport.__doc__

    def restoreToVersion(self, version: int) -> DataFrame:
        LocalDeltaTable._verify_type_int(version, "version")
        plan = RestoreTable(self._to_proto(), version=version)
        df = DataFrame(plan, session=self._spark)
        return self._spark.createDataFrame(df.toPandas())

    restoreToVersion.__doc__ = LocalDeltaTable.restoreToVersion.__doc__

    def restoreToTimestamp(self, timestamp: str) -> DataFrame:
        LocalDeltaTable._verify_type_str(timestamp, "timestamp")
        plan = RestoreTable(self._to_proto(), timestamp=timestamp)
        df = DataFrame(plan, session=self._spark)
        return self._spark.createDataFrame(df.toPandas())

    restoreToTimestamp.__doc__ = LocalDeltaTable.restoreToTimestamp.__doc__

    def optimize(self) -> "DeltaOptimizeBuilder":
        return DeltaOptimizeBuilder(self._spark, self)

    optimize.__doc__ = LocalDeltaTable.optimize.__doc__

    def clone(
        self,
        target: str,
        isShallow: bool = False,
        replace: bool = False,
        properties: Optional[Dict[str, str]] = None
    ) -> "DeltaTable":
        LocalDeltaTable._verify_clone_types(target, isShallow, replace, properties)
        command = CloneTable(
            self._to_proto(),
            target,
            isShallow,
            replace,
            properties
        ).command(session=self._spark.client)
        self._spark.client.execute_command(command)
        return DeltaTable.forName(self._spark, target)

    clone.__doc__ = LocalDeltaTable.clone.__doc__

    def cloneAtVersion(
        self,
        version: int,
        target: str,
        isShallow: bool = False,
        replace: bool = False,
        properties: Optional[Dict[str, str]] = None
    ) -> "DeltaTable":
        LocalDeltaTable._verify_clone_types(target, isShallow, replace, properties, version=version)
        command = CloneTable(
            self._to_proto(),
            target,
            isShallow,
            replace,
            properties,
            version=version
        ).command(session=self._spark.client)
        self._spark.client.execute_command(command)
        return DeltaTable.forName(self._spark, target)

    cloneAtVersion.__doc__ = LocalDeltaTable.cloneAtVersion.__doc__

    def cloneAtTimestamp(
        self,
        timestamp: str,
        target: str,
        isShallow: bool = False,
        replace: bool = False,
        properties: Optional[Dict[str, str]] = None
    ) -> "DeltaTable":
        LocalDeltaTable._verify_clone_types(target, isShallow, replace, properties, timestamp)
        command = CloneTable(
            self._to_proto(),
            target,
            isShallow,
            replace,
            properties,
            timestamp=timestamp
        ).command(session=self._spark.client)
        self._spark.client.execute_command(command)
        return DeltaTable.forName(self._spark, target)

    cloneAtTimestamp.__doc__ = LocalDeltaTable.cloneAtTimestamp.__doc__

    def _to_proto(self) -> proto.DeltaTable:
        result = proto.DeltaTable()
        if self._path is not None:
            result.path.path = self._path
        if self._tableOrViewName is not None:
            result.table_or_view_name = self._tableOrViewName
        return result

    @staticmethod
    def _dict_to_assignments(
        mapping: OptionalColumnMapping,
        argname: str,
    ) -> Optional[List[Assignment]]:
        if mapping is None:
            raise ValueError("%s cannot be None" % argname)
        elif type(mapping) is not dict:
            e = "%s must be a dict, found to be %s" % (argname, str(type(dict)))
            raise TypeError(e)

        result = []
        for col, expr in mapping.items():
            if type(col) is not str:
                e = ("Keys of dict in %s must contain only strings with column names" % argname) + \
                    (", found '%s' of type '%s" % (str(col), str(type(col))))
                raise TypeError(e)
            field = functions.col(col)

            if isinstance(expr, Column):
                value = expr
            elif isinstance(expr, str):
                value = functions.expr(expr)
            else:
                e = ("Values of dict in %s must contain only Spark SQL Columns " % argname) + \
                    "or strings (expressions in SQL syntax) as values, " + \
                    ("found '%s' of type '%s'" % (str(expr), str(type(expr))))
                raise TypeError(e)
            result.append(Assignment(field, value))

        return result

    @staticmethod
    def _condition_to_column(
        condition: OptionalExpressionOrColumn, argname: str = "'condition'"
    ) -> Column:
        if condition is None:
            result = None
        elif isinstance(condition, Column):
            result = condition
        elif isinstance(condition, str):
            result = functions.expr(condition)
        else:
            e = ("%s must be a Spark SQL Column or a string (expression in SQL syntax)" % argname) \
                + ", found to be of type %s" % str(type(condition))
            raise TypeError(e)
        return result


class DeltaMergeBuilder(object):
    __doc__ = LocalDeltaMergeBuilder.__doc__

    def __init__(
        self,
        spark: SparkSession,
        target: LogicalPlan,
        source: LogicalPlan,
        condition: ExpressionOrColumn
    ) -> None:
        self._spark = spark
        self._target = target
        self._source = source
        self._condition = condition
        self._matchedActions = []
        self._notMatchedActions = []
        self._notMatchedBySourceActions = []
        self._with_schema_evolution = False

    @overload
    def whenMatchedUpdate(
        self, condition: OptionalExpressionOrColumn, set: ColumnMapping
    ) -> "DeltaMergeBuilder":
        ...

    @overload
    def whenMatchedUpdate(
        self, *, set: ColumnMapping
    ) -> "DeltaMergeBuilder":
        ...

    def whenMatchedUpdate(
        self,
        condition: OptionalExpressionOrColumn = None,
        set: OptionalColumnMapping = None
    ) -> "DeltaMergeBuilder":
        assignments = DeltaTable._dict_to_assignments(set, "'set' in whenMatchedUpdate")
        condition = DeltaTable._condition_to_column(condition)
        self._matchedActions.append(UpdateAction(condition, assignments))
        return self

    whenMatchedUpdate.__doc__ = LocalDeltaMergeBuilder.whenMatchedUpdate.__doc__

    def whenMatchedUpdateAll(
        self, condition: OptionalExpressionOrColumn = None
    ) -> "DeltaMergeBuilder":
        self._matchedActions.append(UpdateStarAction(DeltaTable._condition_to_column(condition)))
        return self

    whenMatchedUpdateAll.__doc__ = LocalDeltaMergeBuilder.whenMatchedUpdateAll.__doc__

    def whenMatchedDelete(
        self, condition: OptionalExpressionOrColumn = None
    ) -> "DeltaMergeBuilder":
        self._matchedActions.append(DeleteAction(DeltaTable._condition_to_column(condition)))
        return self

    whenMatchedDelete.__doc__ = LocalDeltaMergeBuilder.whenMatchedDelete.__doc__

    @overload
    def whenNotMatchedInsert(
        self, condition: ExpressionOrColumn, values: ColumnMapping
    ) -> "DeltaMergeBuilder":
        ...

    @overload
    def whenNotMatchedInsert(
        self, *, values: ColumnMapping = ...
    ) -> "DeltaMergeBuilder":
        ...

    def whenNotMatchedInsert(
        self,
        condition: OptionalExpressionOrColumn = None,
        values: OptionalColumnMapping = None
    ) -> "DeltaMergeBuilder":
        assignments = DeltaTable._dict_to_assignments(values, "'values' in whenNotMatchedInsert")
        condition = DeltaTable._condition_to_column(condition)
        self._notMatchedActions.append(InsertAction(condition, assignments))
        return self

    whenNotMatchedInsert.__doc__ = LocalDeltaMergeBuilder.whenNotMatchedInsert.__doc__

    def whenNotMatchedInsertAll(
        self, condition: OptionalExpressionOrColumn = None
    ) -> "DeltaMergeBuilder":
        self._notMatchedActions.append(
            InsertStarAction(DeltaTable._condition_to_column(condition))
        )
        return self

    whenNotMatchedInsertAll.__doc__ = LocalDeltaMergeBuilder.whenNotMatchedInsertAll.__doc__

    @overload
    def whenNotMatchedBySourceUpdate(
        self, condition: OptionalExpressionOrColumn, set: ColumnMapping
    ) -> "DeltaMergeBuilder":
        ...

    @overload
    def whenNotMatchedBySourceUpdate(
        self, *, set: ColumnMapping
    ) -> "DeltaMergeBuilder":
        ...

    def whenNotMatchedBySourceUpdate(
        self,
        condition: OptionalExpressionOrColumn = None,
        set: OptionalColumnMapping = None
    ) -> "DeltaMergeBuilder":
        assignments = DeltaTable._dict_to_assignments(set, "'set' in whenNotMatchedBySourceUpdate")
        condition = DeltaTable._condition_to_column(condition)
        self._notMatchedBySourceActions.append(UpdateAction(condition, assignments))
        return self

    whenNotMatchedBySourceUpdate.__doc__ = LocalDeltaMergeBuilder.whenNotMatchedBySourceUpdate.__doc__

    def whenNotMatchedBySourceDelete(
        self, condition: OptionalExpressionOrColumn = None
    ) -> "DeltaMergeBuilder":
        action = DeleteAction(DeltaTable._condition_to_column(condition))
        self._notMatchedBySourceActions.append(action)
        return self

    whenNotMatchedBySourceDelete.__doc__ = LocalDeltaMergeBuilder.whenNotMatchedBySourceDelete.__doc__

    def withSchemaEvolution(self) -> "DeltaMergeBuilder":
        self._with_schema_evolution = True
        return self

    def execute(self) -> DataFrame:
        plan = MergeIntoTable(
            self._target,
            self._source,
            self._condition,
            self._matchedActions,
            self._notMatchedActions,
            self._notMatchedBySourceActions,
            self._with_schema_evolution
        )
        df = DataFrame(plan, session=self._spark)
        return self._spark.createDataFrame(df.toPandas())

    execute.__doc__ = LocalDeltaMergeBuilder.execute.__doc__


class DeltaTableBuilder(object):
    __doc__ = LocalDeltaTableBuilder.__doc__

    def __init__(
        self,
        spark: SparkSession,
        mode: proto.CreateDeltaTable.Mode
    ) -> None:
        self._spark = spark
        self._mode = mode
        self._tableName = None
        self._location = None
        self._comment = None
        self._columns = []
        self._properties = {}
        self._partitioningColumns = []
        self._clusteringColumns = []

    def _raise_type_error(self, msg: str, objs: Iterable[Any]) -> NoReturn:
        errorMsg = msg
        for obj in objs:
            errorMsg += " Found %s with type %s" % ((str(obj)), str(type(obj)))
        raise TypeError(errorMsg)

    def _check_identity_column_spec(self, identityGenerator: IdentityGenerator) -> None:
        if identityGenerator.step == 0:
            raise ValueError("Column identity generation requires step to be non-zero.")

    def tableName(self, identifier: str) -> "DeltaTableBuilder":
        if type(identifier) is not str:
            self._raise_type_error("Identifier must be str.", [identifier])
        self._tableName = identifier
        return self

    tableName.__doc__ = LocalDeltaTableBuilder.tableName.__doc__

    def location(self, location: str) -> "DeltaTableBuilder":
        if type(location) is not str:
            self._raise_type_error("Location must be str.", [location])
        self._location = location
        return self

    location.__doc__ = LocalDeltaTableBuilder.location.__doc__

    def comment(self, comment: str) -> "DeltaTableBuilder":
        if type(comment) is not str:
            self._raise_type_error("Table comment must be str.", [comment])
        self._comment = comment
        return self

    comment.__doc__ = LocalDeltaTableBuilder.comment.__doc__

    def addColumn(
        self,
        colName: str,
        dataType: Union[str, DataType],
        nullable: bool = True,
        generatedAlwaysAs: Optional[Union[str, IdentityGenerator]] = None,
        generatedByDefaultAs: Optional[IdentityGenerator] = None,
        comment: Optional[str] = None,
    ) -> "DeltaTableBuilder":
        if type(colName) is not str:
            self._raise_type_error("Column name must be str.", [colName])
        if type(dataType) is not str and not isinstance(dataType, DataType):
            self._raise_type_error(
                "Column data type must be str or DataType.", [dataType])
        if type(nullable) is not bool:
            self._raise_type_error("Column nullable must be bool.", [nullable])
        if generatedAlwaysAs is not None and generatedByDefaultAs is not None:
            raise ValueError(
                "generatedByDefaultAs and generatedAlwaysAs cannot both be set.",
                [generatedByDefaultAs, generatedAlwaysAs])
        if generatedAlwaysAs is not None:
            if isinstance(generatedAlwaysAs, IdentityGenerator):
                self._check_identity_column_spec(generatedAlwaysAs)
            elif type(generatedAlwaysAs) is not str:
                self._raise_type_error(
                    "Generated always as expression must be str or IdentityGenerator.",
                    [generatedAlwaysAs])
        elif generatedByDefaultAs is not None:
            if not isinstance(generatedByDefaultAs, IdentityGenerator):
                self._raise_type_error(
                    "Generated by default expression must be IdentityGenerator.",
                    [generatedByDefaultAs])
            self._check_identity_column_spec(generatedByDefaultAs)

        if comment is not None and type(comment) is not str:
            self._raise_type_error("Comment must be str or None.", [colName])

        column = proto.CreateDeltaTable.Column()
        column.name = colName
        if type(dataType) is str:
            column.data_type.unparsed.data_type_string = dataType
        elif isinstance(dataType, DataType):
            column.data_type.CopyFrom(pyspark_types_to_proto_types(dataType))
        column.nullable = nullable
        if generatedAlwaysAs is not None:
            if type(generatedAlwaysAs) is str:
                column.generated_always_as = generatedAlwaysAs
            else:
                identity_info = proto.CreateDeltaTable.Column.IdentityInfo(
                    start=generatedAlwaysAs.start,
                    step=generatedAlwaysAs.step,
                    allow_explicit_insert=False)
                column.identity_info.CopyFrom(identity_info)
        if generatedByDefaultAs is not None:
            identity_info = proto.CreateDeltaTable.Column.IdentityInfo(
                start=generatedByDefaultAs.start,
                step=generatedByDefaultAs.step,
                allow_explicit_insert=True)
            column.identity_info.CopyFrom(identity_info)
        if comment is not None:
            column.comment = comment
        self._columns.append(column)
        return self

    addColumn.__doc__ = LocalDeltaTableBuilder.addColumn.__doc__

    def addColumns(
        self, cols: Union[StructType, List[StructField]]
    ) -> "DeltaTableBuilder":
        if isinstance(cols, list):
            for col in cols:
                if type(col) is not StructField:
                    self._raise_type_error(
                        "Column in existing schema must be StructField.", [col])
            cols = StructType(cols)
        if type(cols) is not StructType:
            self._raise_type_error(
                "Schema must be StructType or a list of StructField.", [cols])

        for col in cols:
            self.addColumn(col.name, col.dataType, col.nullable)
        return self

    addColumns.__doc__ = LocalDeltaTableBuilder.addColumns.__doc__

    @overload
    def partitionedBy(
        self, *cols: str
    ) -> "DeltaTableBuilder":
        ...

    @overload
    def partitionedBy(
        self, __cols: Union[List[str], Tuple[str, ...]]
    ) -> "DeltaTableBuilder":
        ...

    def partitionedBy(
        self, *cols: Union[str, List[str], Tuple[str, ...]]
    ) -> "DeltaTableBuilder":
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]  # type: ignore[assignment]
        for c in cols:
            if type(c) is not str:
                self._raise_type_error("Partitioning column must be str.", [c])

        self._partitioningColumns.extend(cols)
        return self

    partitionedBy.__doc__ = LocalDeltaTableBuilder.partitionedBy.__doc__

    @overload
    def clusterBy(
        self, *cols: str
    ) -> "DeltaTableBuilder":
        ...

    @overload
    def clusterBy(
        self, __cols: Union[List[str], Tuple[str, ...]]
    ) -> "DeltaTableBuilder":
        ...

    def clusterBy(
        self, *cols: Union[str, List[str], Tuple[str, ...]]
    ) -> "DeltaTableBuilder":
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]  # type: ignore[assignment]
        for c in cols:
            if type(c) is not str:
                self._raise_type_error("Clustering column must be str.", [c])

        self._clusteringColumns.extend(cols)
        return self

    clusterBy.__doc__ = LocalDeltaTableBuilder.clusterBy.__doc__

    def property(self, key: str, value: str) -> "DeltaTableBuilder":
        if type(key) is not str or type(value) is not str:
            self._raise_type_error(
                "Key and value of property must be string.", [key, value])

        self._properties[key] = value
        return self

    property.__doc__ = LocalDeltaTableBuilder.property.__doc__

    def execute(self) -> DeltaTable:
        command = CreateDeltaTable(
            self._mode,
            self._tableName,
            self._location,
            self._comment,
            self._columns,
            self._partitioningColumns,
            self._properties,
            self._clusteringColumns
        ).command(session=self._spark.client)
        self._spark.client.execute_command(command)
        if self._tableName is not None:
            return DeltaTable.forName(self._spark, self._tableName)
        else:
            return DeltaTable.forPath(self._spark, self._location)

    execute.__doc__ = LocalDeltaTableBuilder.execute.__doc__


class DeltaOptimizeBuilder(object):
    __doc__ = LocalDeltaOptimizeBuilder.__doc__

    def __init__(self, spark: SparkSession, table: "DeltaTable"):
        self._spark = spark
        self._table = table
        self._partitionFilters = []

    def where(self, partitionFilter: str) -> "DeltaOptimizeBuilder":
        self._partitionFilters.append(partitionFilter)
        return self

    where.__doc__ = LocalDeltaOptimizeBuilder.where.__doc__

    def executeCompaction(self) -> DataFrame:
        plan = OptimizeTable(self._table._to_proto(), self._partitionFilters, [])
        df = DataFrame(plan, session=self._spark)
        return self._spark.createDataFrame(df.toPandas())

    executeCompaction.__doc__ = LocalDeltaOptimizeBuilder.executeCompaction.__doc__

    def executeZOrderBy(self, *cols: Union[str, List[str], Tuple[str, ...]]) -> DataFrame:
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]  # type: ignore[assignment]
        for c in cols:
            if type(c) is not str:
                errorMsg = "Z-order column must be str. "
                errorMsg += "Found %s with type %s" % ((str(c)), str(type(c)))
                raise TypeError(errorMsg)

        plan = OptimizeTable(self._table._to_proto(), self._partitionFilters, cols)
        df = DataFrame(plan, session=self._spark)
        return self._spark.createDataFrame(df.toPandas())

    executeZOrderBy.__doc__ = LocalDeltaOptimizeBuilder.executeZOrderBy.__doc__
