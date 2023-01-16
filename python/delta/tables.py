#
# Copyright (2021) The Delta Lake Project Authors.
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
    TYPE_CHECKING, cast, overload, Any, Dict, Iterable, Optional, Union, NoReturn, List, Tuple
)

import delta.exceptions  # noqa: F401; pylint: disable=unused-variable
from delta._typing import (
    ColumnMapping, OptionalColumnMapping, ExpressionOrColumn, OptionalExpressionOrColumn
)

from pyspark import since
from pyspark.sql import Column, DataFrame, functions, SparkSession
from pyspark.sql.column import _to_seq  # type: ignore[attr-defined]
from pyspark.sql.types import DataType, StructType, StructField


if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject, JVMView  # type: ignore[import]
    from py4j.java_collections import JavaMap  # type: ignore[import]


class DeltaTable(object):
    """
        Main class for programmatically interacting with Delta tables.
        You can create DeltaTable instances using the path of the Delta table.::

            deltaTable = DeltaTable.forPath(spark, "/path/to/table")

        In addition, you can convert an existing Parquet table in place into a Delta table.::

            deltaTable = DeltaTable.convertToDelta(spark, "parquet.`/path/to/table`")

        .. versionadded:: 0.4
    """
    def __init__(self, spark: SparkSession, jdt: "JavaObject"):
        self._spark = spark
        self._jdt = jdt

    @since(0.4)  # type: ignore[arg-type]
    def toDF(self) -> DataFrame:
        """
        Get a DataFrame representation of this Delta table.
        """
        return DataFrame(
            self._jdt.toDF(),
            # Simple trick to avoid warnings from Spark 3.3.0. `_wrapped`
            # in SparkSession is removed in Spark 3.3.0, see also SPARK-38121.
            getattr(self._spark, "_wrapped", self._spark)  # type: ignore[attr-defined]
        )

    @since(0.4)  # type: ignore[arg-type]
    def alias(self, aliasName: str) -> "DeltaTable":
        """
        Apply an alias to the Delta table.
        """
        jdt = self._jdt.alias(aliasName)
        return DeltaTable(self._spark, jdt)

    @since(0.5)  # type: ignore[arg-type]
    def generate(self, mode: str) -> None:
        """
        Generate manifest files for the given delta table.

        :param mode: mode for the type of manifest file to be generated
                     The valid modes are as follows (not case sensitive):

                     - "symlink_format_manifest": This will generate manifests in symlink format
                                                  for Presto and Athena read support.

                     See the online documentation for more information.
        """
        self._jdt.generate(mode)

    @since(0.4)  # type: ignore[arg-type]
    def delete(self, condition: OptionalExpressionOrColumn = None) -> None:
        """
        Delete data from the table that match the given ``condition``.

        Example::

            deltaTable.delete("date < '2017-01-01'")        # predicate using SQL formatted string

            deltaTable.delete(col("date") < "2017-01-01")   # predicate using Spark SQL functions

        :param condition: condition of the update
        :type condition: str or pyspark.sql.Column
        """
        if condition is None:
            self._jdt.delete()
        else:
            self._jdt.delete(DeltaTable._condition_to_jcolumn(condition))

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
    ) -> None:
        """
        Update data from the table on the rows that match the given ``condition``,
        which performs the rules defined by ``set``.

        Example::

            # condition using SQL formatted string
            deltaTable.update(
                condition = "eventType = 'clck'",
                set = { "eventType": "'click'" } )

            # condition using Spark SQL functions
            deltaTable.update(
                condition = col("eventType") == "clck",
                set = { "eventType": lit("click") } )

        :param condition: Optional condition of the update
        :type condition: str or pyspark.sql.Column
        :param set: Defines the rules of setting the values of columns that need to be updated.
                    *Note: This param is required.* Default value None is present to allow
                    positional args in same order across languages.
        :type set: dict with str as keys and str or pyspark.sql.Column as values

        .. versionadded:: 0.4
        """
        jmap = DeltaTable._dict_to_jmap(self._spark, set, "'set'")
        jcolumn = DeltaTable._condition_to_jcolumn(condition)
        if condition is None:
            self._jdt.update(jmap)
        else:
            self._jdt.update(jcolumn, jmap)

    @since(0.4)  # type: ignore[arg-type]
    def merge(
        self, source: DataFrame, condition: ExpressionOrColumn
    ) -> "DeltaMergeBuilder":
        """
        Merge data from the `source` DataFrame based on the given merge `condition`. This returns
        a :class:`DeltaMergeBuilder` object that can be used to specify the update, delete, or
        insert actions to be performed on rows based on whether the rows matched the condition or
        not. See :class:`DeltaMergeBuilder` for a full description of this operation and what
        combinations of update, delete and insert operations are allowed.

        Example 1 with conditions and update expressions as SQL formatted string::

            deltaTable.alias("events").merge(
                source = updatesDF.alias("updates"),
                condition = "events.eventId = updates.eventId"
              ).whenMatchedUpdate(set =
                {
                  "data": "updates.data",
                  "count": "events.count + 1"
                }
              ).whenNotMatchedInsert(values =
                {
                  "date": "updates.date",
                  "eventId": "updates.eventId",
                  "data": "updates.data",
                  "count": "1"
                }
              ).execute()

        Example 2 with conditions and update expressions as Spark SQL functions::

            from pyspark.sql.functions import *

            deltaTable.alias("events").merge(
                source = updatesDF.alias("updates"),
                condition = expr("events.eventId = updates.eventId")
              ).whenMatchedUpdate(set =
                {
                  "data" : col("updates.data"),
                  "count": col("events.count") + 1
                }
              ).whenNotMatchedInsert(values =
                {
                  "date": col("updates.date"),
                  "eventId": col("updates.eventId"),
                  "data": col("updates.data"),
                  "count": lit("1")
                }
              ).execute()

        :param source: Source DataFrame
        :type source: pyspark.sql.DataFrame
        :param condition: Condition to match sources rows with the Delta table rows.
        :type condition: str or pyspark.sql.Column

        :return: builder object to specify whether to update, delete or insert rows based on
                 whether the condition matched or not
        :rtype: :py:class:`delta.tables.DeltaMergeBuilder`
        """
        if source is None:
            raise ValueError("'source' in merge cannot be None")
        elif type(source) is not DataFrame:
            raise TypeError("Type of 'source' in merge must be DataFrame.")
        if condition is None:
            raise ValueError("'condition' in merge cannot be None")

        jbuilder = self._jdt.merge(source._jdf, DeltaTable._condition_to_jcolumn(condition))
        return DeltaMergeBuilder(self._spark, jbuilder)

    @since(0.4)  # type: ignore[arg-type]
    def vacuum(self, retentionHours: Optional[float] = None) -> DataFrame:
        """
        Recursively delete files and directories in the table that are not needed by the table for
        maintaining older versions up to the given retention threshold. This method will return an
        empty DataFrame on successful completion.

        Example::

            deltaTable.vacuum()     # vacuum files not required by versions more than 7 days old

            deltaTable.vacuum(100)  # vacuum files not required by versions more than 100 hours old

        :param retentionHours: Optional number of hours retain history. If not specified, then the
                               default retention period of 168 hours (7 days) will be used.
        """
        jdt = self._jdt
        if retentionHours is None:
            return DataFrame(
                jdt.vacuum(),
                getattr(self._spark, "_wrapped", self._spark)  # type: ignore[attr-defined]
            )
        else:
            return DataFrame(
                jdt.vacuum(float(retentionHours)),
                getattr(self._spark, "_wrapped", self._spark)  # type: ignore[attr-defined]
            )

    @since(0.4)  # type: ignore[arg-type]
    def history(self, limit: Optional[int] = None) -> DataFrame:
        """
        Get the information of the latest `limit` commits on this table as a Spark DataFrame.
        The information is in reverse chronological order.

        Example::

            fullHistoryDF = deltaTable.history()    # get the full history of the table

            lastOperationDF = deltaTable.history(1) # get the last operation

        :param limit: Optional, number of latest commits to returns in the history.
        :return: Table's commit history. See the online Delta Lake documentation for more details.
        :rtype: pyspark.sql.DataFrame
        """
        jdt = self._jdt
        if limit is None:
            return DataFrame(
                jdt.history(),
                getattr(self._spark, "_wrapped", self._spark)  # type: ignore[attr-defined]
            )
        else:
            return DataFrame(
                jdt.history(limit),
                getattr(self._spark, "_wrapped", self._spark)  # type: ignore[attr-defined]
            )

    @since(2.1)  # type: ignore[arg-type]
    def detail(self) -> DataFrame:
        """
        Get the details of a Delta table such as the format, name, and size.

        Example::

            detailDF = deltaTable.detail() # get the full details of the table

        :return Information of the table (format, name, size, etc.)
        :rtype: pyspark.sql.DataFrame

        .. note:: Evolving
        """
        return DataFrame(
            self._jdt.detail(),
            getattr(self._spark, "_wrapped", self._spark)  # type: ignore[attr-defined]
        )

    @classmethod
    @since(0.4)  # type: ignore[arg-type]
    def convertToDelta(
        cls,
        sparkSession: SparkSession,
        identifier: str,
        partitionSchema: Optional[Union[str, StructType]] = None
    ) -> "DeltaTable":
        """
        Create a DeltaTable from the given parquet table. Takes an existing parquet table and
        constructs a delta transaction log in the base path of the table.
        Note: Any changes to the table during the conversion process may not result in a consistent
        state at the end of the conversion. Users should stop any changes to the table before the
        conversion is started.

        Example::

            # Convert unpartitioned parquet table at path 'path/to/table'
            deltaTable = DeltaTable.convertToDelta(
                spark, "parquet.`path/to/table`")

            # Convert partitioned parquet table at path 'path/to/table' and partitioned by
            # integer column named 'part'
            partitionedDeltaTable = DeltaTable.convertToDelta(
                spark, "parquet.`path/to/table`", "part int")

        :param sparkSession: SparkSession to use for the conversion
        :type sparkSession: pyspark.sql.SparkSession
        :param identifier: Parquet table identifier formatted as "parquet.`path`"
        :type identifier: str
        :param partitionSchema: Hive DDL formatted string, or pyspark.sql.types.StructType
        :return: DeltaTable representing the converted Delta table
        :rtype: :py:class:`~delta.tables.DeltaTable`
        """
        assert sparkSession is not None

        jvm: "JVMView" = sparkSession._sc._jvm  # type: ignore[attr-defined]
        jsparkSession: "JavaObject" = sparkSession._jsparkSession  # type: ignore[attr-defined]

        if partitionSchema is None:
            jdt = jvm.io.delta.tables.DeltaTable.convertToDelta(
                jsparkSession, identifier
            )
        else:
            if not isinstance(partitionSchema, str):
                partitionSchema = jsparkSession.parseDataType(partitionSchema.json())
            jdt = jvm.io.delta.tables.DeltaTable.convertToDelta(
                jsparkSession, identifier,
                partitionSchema)
        return DeltaTable(sparkSession, jdt)

    @classmethod
    @since(0.4)  # type: ignore[arg-type]
    def forPath(
        cls,
        sparkSession: SparkSession,
        path: str,
        hadoopConf: Dict[str, str] = dict()
    ) -> "DeltaTable":
        """
        Instantiate a :class:`DeltaTable` object representing the data at the given path,
        If the given path is invalid (i.e. either no table exists or an existing table is
        not a Delta table), it throws a `not a Delta table` error.

        :param sparkSession: SparkSession to use for loading the table
        :type sparkSession: pyspark.sql.SparkSession
        :param hadoopConf: Hadoop configuration starting with "fs." or "dfs." will be picked
                           up by `DeltaTable` to access the file system when executing queries.
                           Other configurations will not be allowed.
        :type hadoopConf: optional dict with str as key and str as value.
        :return: loaded Delta table
        :rtype: :py:class:`~delta.tables.DeltaTable`

        Example::

            hadoopConf = {"fs.s3a.access.key" : "<access-key>",
                       "fs.s3a.secret.key": "secret-key"}
            deltaTable = DeltaTable.forPath(
                           spark,
                           "/path/to/table",
                           hadoopConf)
        """
        assert sparkSession is not None

        jvm: "JVMView" = sparkSession._sc._jvm  # type: ignore[attr-defined]
        jsparkSession: "JavaObject" = sparkSession._jsparkSession  # type: ignore[attr-defined]

        jdt = jvm.io.delta.tables.DeltaTable.forPath(jsparkSession, path, hadoopConf)
        return DeltaTable(sparkSession, jdt)

    @classmethod
    @since(0.7)  # type: ignore[arg-type]
    def forName(
        cls, sparkSession: SparkSession, tableOrViewName: str
    ) -> "DeltaTable":
        """
        Instantiate a :class:`DeltaTable` object using the given table or view name. If the given
        tableOrViewName is invalid (i.e. either no table exists or an existing table is not a
        Delta table), it throws a `not a Delta table` error.

        The given tableOrViewName can also be the absolute path of a delta datasource (i.e.
        delta.`path`), If so, instantiate a :class:`DeltaTable` object representing the data at
        the given path (consistent with the `forPath`).

        :param sparkSession: SparkSession to use for loading the table
        :param tableOrViewName: name of the table or view
        :return: loaded Delta table
        :rtype: :py:class:`~delta.tables.DeltaTable`

        Example::

            deltaTable = DeltaTable.forName(spark, "tblName")
        """
        assert sparkSession is not None

        jvm: "JVMView" = sparkSession._sc._jvm  # type: ignore[attr-defined]
        jsparkSession: "JavaObject" = sparkSession._jsparkSession  # type: ignore[attr-defined]

        jdt = jvm.io.delta.tables.DeltaTable.forName(jsparkSession, tableOrViewName)
        return DeltaTable(sparkSession, jdt)

    @classmethod
    @since(1.0)  # type: ignore[arg-type]
    def create(
        cls, sparkSession: Optional[SparkSession] = None
    ) -> "DeltaTableBuilder":
        """
        Return :class:`DeltaTableBuilder` object that can be used to specify
        the table name, location, columns, partitioning columns, table comment,
        and table properties to create a Delta table, error if the table exists
        (the same as SQL `CREATE TABLE`).

        See :class:`DeltaTableBuilder` for a full description and examples
        of this operation.

        :param sparkSession: SparkSession to use for creating the table
        :return: an instance of DeltaTableBuilder
        :rtype: :py:class:`~delta.tables.DeltaTableBuilder`

        .. note:: Evolving
        """
        if sparkSession is None:
            sparkSession = SparkSession.getActiveSession()
        assert sparkSession is not None

        jvm: "JVMView" = sparkSession._sc._jvm  # type: ignore[attr-defined]
        jsparkSession: "JavaObject" = sparkSession._jsparkSession  # type: ignore[attr-defined]

        jdt = jvm.io.delta.tables.DeltaTable.create(jsparkSession)
        return DeltaTableBuilder(sparkSession, jdt)

    @classmethod
    @since(1.0)  # type: ignore[arg-type]
    def createIfNotExists(
        cls, sparkSession: Optional[SparkSession] = None
    ) -> "DeltaTableBuilder":
        """
        Return :class:`DeltaTableBuilder` object that can be used to specify
        the table name, location, columns, partitioning columns, table comment,
        and table properties to create a Delta table,
        if it does not exists (the same as SQL `CREATE TABLE IF NOT EXISTS`).

        See :class:`DeltaTableBuilder` for a full description and examples
        of this operation.

        :param sparkSession: SparkSession to use for creating the table
        :return: an instance of DeltaTableBuilder
        :rtype: :py:class:`~delta.tables.DeltaTableBuilder`

        .. note:: Evolving
        """
        if sparkSession is None:
            sparkSession = SparkSession.getActiveSession()
        assert sparkSession is not None

        jvm: "JVMView" = sparkSession._sc._jvm  # type: ignore[attr-defined]
        jsparkSession: "JavaObject" = sparkSession._jsparkSession  # type: ignore[attr-defined]

        jdt = jvm.io.delta.tables.DeltaTable.createIfNotExists(jsparkSession)
        return DeltaTableBuilder(sparkSession, jdt)

    @classmethod
    @since(1.0)  # type: ignore[arg-type]
    def replace(
        cls, sparkSession: Optional[SparkSession] = None
    ) -> "DeltaTableBuilder":
        """
        Return :class:`DeltaTableBuilder` object that can be used to specify
        the table name, location, columns, partitioning columns, table comment,
        and table properties to replace a Delta table,
        error if the table doesn't exist (the same as SQL `REPLACE TABLE`).

        See :class:`DeltaTableBuilder` for a full description and examples
        of this operation.

        :param sparkSession: SparkSession to use for creating the table
        :return: an instance of DeltaTableBuilder
        :rtype: :py:class:`~delta.tables.DeltaTableBuilder`

        .. note:: Evolving
        """
        if sparkSession is None:
            sparkSession = SparkSession.getActiveSession()
        assert sparkSession is not None

        jvm: "JVMView" = sparkSession._sc._jvm  # type: ignore[attr-defined]
        jsparkSession: "JavaObject" = sparkSession._jsparkSession  # type: ignore[attr-defined]

        jdt = jvm.io.delta.tables.DeltaTable.replace(jsparkSession)
        return DeltaTableBuilder(sparkSession, jdt)

    @classmethod
    @since(1.0)  # type: ignore[arg-type]
    def createOrReplace(
        cls, sparkSession: Optional[SparkSession] = None
    ) -> "DeltaTableBuilder":
        """
        Return :class:`DeltaTableBuilder` object that can be used to specify
        the table name, location, columns, partitioning columns, table comment,
        and table properties replace a Delta table,
        error if the table doesn't exist (the same as SQL `REPLACE TABLE`).

        See :class:`DeltaTableBuilder` for a full description and examples
        of this operation.

        :param sparkSession: SparkSession to use for creating the table
        :return: an instance of DeltaTableBuilder
        :rtype: :py:class:`~delta.tables.DeltaTableBuilder`

        .. note:: Evolving
        """
        if sparkSession is None:
            sparkSession = SparkSession.getActiveSession()
        assert sparkSession is not None

        jvm: "JVMView" = sparkSession._sc._jvm  # type: ignore[attr-defined]
        jsparkSession: "JavaObject" = sparkSession._jsparkSession  # type: ignore[attr-defined]

        jdt = jvm.io.delta.tables.DeltaTable.createOrReplace(jsparkSession)
        return DeltaTableBuilder(sparkSession, jdt)

    @classmethod
    @since(0.4)  # type: ignore[arg-type]
    def isDeltaTable(cls, sparkSession: SparkSession, identifier: str) -> bool:
        """
        Check if the provided `identifier` string, in this case a file path,
        is the root of a Delta table using the given SparkSession.

        :param sparkSession: SparkSession to use to perform the check
        :param path: location of the table
        :return: If the table is a delta table or not
        :rtype: bool

        Example::

            DeltaTable.isDeltaTable(spark, "/path/to/table")
        """
        assert sparkSession is not None

        jvm: "JVMView" = sparkSession._sc._jvm  # type: ignore[attr-defined]
        jsparkSession: "JavaObject" = sparkSession._jsparkSession  # type: ignore[attr-defined]

        return jvm.io.delta.tables.DeltaTable.isDeltaTable(jsparkSession, identifier)

    @since(0.8)  # type: ignore[arg-type]
    def upgradeTableProtocol(self, readerVersion: int, writerVersion: int) -> None:
        """
        Updates the protocol version of the table to leverage new features. Upgrading the reader
        version will prevent all clients that have an older version of Delta Lake from accessing
        this table. Upgrading the writer version will prevent older versions of Delta Lake to write
        to this table. The reader or writer version cannot be downgraded.

        See online documentation and Delta's protocol specification at PROTOCOL.md for more details.
        """
        jdt = self._jdt
        if not isinstance(readerVersion, int):
            raise ValueError("The readerVersion needs to be an integer but got '%s'." %
                             type(readerVersion))
        if not isinstance(writerVersion, int):
            raise ValueError("The writerVersion needs to be an integer but got '%s'." %
                             type(writerVersion))
        jdt.upgradeTableProtocol(readerVersion, writerVersion)

    @since(1.2)  # type: ignore[arg-type]
    def restoreToVersion(self, version: int) -> DataFrame:
        """
        Restore the DeltaTable to an older version of the table specified by version number.

        Example::

            io.delta.tables.DeltaTable.restoreToVersion(1)

        :param version: target version of restored table
        :return: Dataframe with metrics of restore operation.
        :rtype: pyspark.sql.DataFrame
        """

        DeltaTable._verify_type_int(version, "version")
        return DataFrame(
            self._jdt.restoreToVersion(version),
            getattr(self._spark, "_wrapped", self._spark)  # type: ignore[attr-defined]
        )

    @since(1.2)  # type: ignore[arg-type]
    def restoreToTimestamp(self, timestamp: str) -> DataFrame:
        """
        Restore the DeltaTable to an older version of the table specified by a timestamp.
        Timestamp can be of the format yyyy-MM-dd or yyyy-MM-dd HH:mm:ss

        Example::

            io.delta.tables.DeltaTable.restoreToTimestamp('2021-01-01')
            io.delta.tables.DeltaTable.restoreToTimestamp('2021-01-01 01:01:01')

        :param timestamp: target timestamp of restored table
        :return: Dataframe with metrics of restore operation.
        :rtype: pyspark.sql.DataFrame
        """

        DeltaTable._verify_type_str(timestamp, "timestamp")
        return DataFrame(
            self._jdt.restoreToTimestamp(timestamp),
            getattr(self._spark, "_wrapped", self._spark)  # type: ignore[attr-defined]
        )

    @since(2.0)  # type: ignore[arg-type]
    def optimize(self) -> "DeltaOptimizeBuilder":
        """
        Optimize the data layout of the table. This returns
        a :py:class:`~delta.tables.DeltaOptimizeBuilder` object that can
        be used to specify the partition filter to limit the scope of
        optimize and also execute different optimization techniques
        such as file compaction or order data using Z-Order curves.

        See the :py:class:`~delta.tables.DeltaOptimizeBuilder` for a
        full description of this operation.

        Example::

            deltaTable.optimize().where("date='2021-11-18'").executeCompaction()

        :return: an instance of DeltaOptimizeBuilder.
        :rtype: :py:class:`~delta.tables.DeltaOptimizeBuilder`
        """
        jbuilder = self._jdt.optimize()
        return DeltaOptimizeBuilder(self._spark, jbuilder)

    @staticmethod  # type: ignore[arg-type]
    def _verify_type_str(variable: str, name: str) -> None:
        if not isinstance(variable, str) or variable is None:
            raise ValueError("%s needs to be a string but got '%s'." % (name, type(variable)))

    @staticmethod  # type: ignore[arg-type]
    def _verify_type_int(variable: int, name: str) -> None:
        if not isinstance(variable, int) or variable is None:
            raise ValueError("%s needs to be an int but got '%s'." % (name, type(variable)))

    @staticmethod
    def _dict_to_jmap(
        sparkSession: SparkSession,
        pydict: OptionalColumnMapping,
        argname: str,
    ) -> "JavaObject":
        """
        convert dict<str, pColumn/str> to Map<str, jColumn>
        """
        # Get the Java map for pydict
        if pydict is None:
            raise ValueError("%s cannot be None" % argname)
        elif type(pydict) is not dict:
            e = "%s must be a dict, found to be %s" % (argname, str(type(pydict)))
            raise TypeError(e)

        jvm: "JVMView" = sparkSession._sc._jvm  # type: ignore[attr-defined]

        jmap: "JavaMap" = jvm.java.util.HashMap()
        for col, expr in pydict.items():
            if type(col) is not str:
                e = ("Keys of dict in %s must contain only strings with column names" % argname) + \
                    (", found '%s' of type '%s" % (str(col), str(type(col))))
                raise TypeError(e)
            if type(expr) is Column:
                jmap.put(col, expr._jc)
            elif type(expr) is str:
                jmap.put(col, functions.expr(expr)._jc)
            else:
                e = ("Values of dict in %s must contain only Spark SQL Columns " % argname) + \
                    "or strings (expressions in SQL syntax) as values, " + \
                    ("found '%s' of type '%s'" % (str(expr), str(type(expr))))
                raise TypeError(e)
        return jmap

    @staticmethod
    def _condition_to_jcolumn(
        condition: OptionalExpressionOrColumn, argname: str = "'condition'"
    ) -> "JavaObject":
        if condition is None:
            jcondition = None
        elif type(condition) is Column:
            jcondition = condition._jc
        elif type(condition) is str:
            jcondition = functions.expr(condition)._jc
        else:
            e = ("%s must be a Spark SQL Column or a string (expression in SQL syntax)" % argname) \
                + ", found to be of type %s" % str(type(condition))
            raise TypeError(e)
        return jcondition


class DeltaMergeBuilder(object):
    """
    Builder to specify how to merge data from source DataFrame into the target Delta table.
    Use :py:meth:`delta.tables.DeltaTable.merge` to create an object of this class.
    Using this builder, you can specify any number of ``whenMatched``, ``whenNotMatched`` and
    ``whenNotMatchedBySource`` clauses. Here are the constraints on these clauses.

    - Constraints in the ``whenMatched`` clauses:

      - The condition in a ``whenMatched`` clause is optional. However, if there are multiple
        ``whenMatched`` clauses, then only the last one may omit the condition.

      - When there are more than one ``whenMatched`` clauses and there are conditions (or the lack
        of) such that a row satisfies multiple clauses, then the action for the first clause
        satisfied is executed. In other words, the order of the ``whenMatched`` clauses matters.

      - If none of the ``whenMatched`` clauses match a source-target row pair that satisfy
        the merge condition, then the target rows will not be updated or deleted.

      - If you want to update all the columns of the target Delta table with the
        corresponding column of the source DataFrame, then you can use the
        ``whenMatchedUpdateAll()``. This is equivalent to::

            whenMatchedUpdate(set = {
              "col1": "source.col1",
              "col2": "source.col2",
              ...    # for all columns in the delta table
            })

    - Constraints in the ``whenNotMatched`` clauses:

      - The condition in a ``whenNotMatched`` clause is optional. However, if there are
        multiple ``whenNotMatched`` clauses, then only the last one may omit the condition.

      - When there are more than one ``whenNotMatched`` clauses and there are conditions (or the
        lack of) such that a row satisfies multiple clauses, then the action for the first clause
        satisfied is executed. In other words, the order of the ``whenNotMatched`` clauses matters.

      - If no ``whenNotMatched`` clause is present or if it is present but the non-matching source
        row does not satisfy the condition, then the source row is not inserted.

      - If you want to insert all the columns of the target Delta table with the
        corresponding column of the source DataFrame, then you can use
        ``whenNotMatchedInsertAll()``. This is equivalent to::

            whenNotMatchedInsert(values = {
              "col1": "source.col1",
              "col2": "source.col2",
              ...    # for all columns in the delta table
            })

    - Constraints in the ``whenNotMatchedBySource`` clauses:

      - The condition in a ``whenNotMatchedBySource`` clause is optional. However, if there are
        multiple ``whenNotMatchedBySource`` clauses, then only the last ``whenNotMatchedBySource``
        clause may omit the condition.

      - Conditions and update expressions  in ``whenNotMatchedBySource`` clauses may only refer to
        columns from the target Delta table.

      - When there are more than one ``whenNotMatchedBySource`` clauses and there are conditions (or
        the lack of) such that a row satisfies multiple clauses, then the action for the first
        clause satisfied is executed. In other words, the order of the ``whenNotMatchedBySource``
        clauses matters.

      - If no ``whenNotMatchedBySource`` clause is present or if it is present but the
        non-matching target row does not satisfy any of the ``whenNotMatchedBySource`` clause
        condition, then the target row will not be updated or deleted.

    Example 1 with conditions and update expressions as SQL formatted string::

        deltaTable.alias("events").merge(
            source = updatesDF.alias("updates"),
            condition = "events.eventId = updates.eventId"
          ).whenMatchedUpdate(set =
            {
              "data": "updates.data",
              "count": "events.count + 1"
            }
          ).whenNotMatchedInsert(values =
            {
              "date": "updates.date",
              "eventId": "updates.eventId",
              "data": "updates.data",
              "count": "1",
              "missed_count": "0"
            }
          ).whenNotMatchedBySourceUpdate(set =
            {
              "missed_count": "events.missed_count + 1"
            }
          ).execute()

    Example 2 with conditions and update expressions as Spark SQL functions::

        from pyspark.sql.functions import *

        deltaTable.alias("events").merge(
            source = updatesDF.alias("updates"),
            condition = expr("events.eventId = updates.eventId")
          ).whenMatchedUpdate(set =
            {
              "data" : col("updates.data"),
              "count": col("events.count") + 1
            }
          ).whenNotMatchedInsert(values =
            {
              "date": col("updates.date"),
              "eventId": col("updates.eventId"),
              "data": col("updates.data"),
              "count": lit("1"),
              "missed_count": lit("0")
            }
          ).whenNotMatchedBySourceUpdate(set =
            {
              "missed_count": col("events.missed_count") + 1
            }
          ).execute()

    .. versionadded:: 0.4
    """
    def __init__(self, spark: SparkSession, jbuilder: "JavaObject"):
        self._spark = spark
        self._jbuilder = jbuilder

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
        """
        Update a matched table row based on the rules defined by ``set``.
        If a ``condition`` is specified, then it must evaluate to true for the row to be updated.

        See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.

        :param condition: Optional condition of the update
        :type condition: str or pyspark.sql.Column
        :param set: Defines the rules of setting the values of columns that need to be updated.
                    *Note: This param is required.* Default value None is present to allow
                    positional args in same order across languages.
        :type set: dict with str as keys and str or pyspark.sql.Column as values
        :return: this builder

        .. versionadded:: 0.4
        """
        jset = DeltaTable._dict_to_jmap(self._spark, set, "'set' in whenMatchedUpdate")
        new_jbuilder = self.__getMatchedBuilder(condition).update(jset)
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    @since(0.4)  # type: ignore[arg-type]
    def whenMatchedUpdateAll(
        self, condition: OptionalExpressionOrColumn = None
    ) -> "DeltaMergeBuilder":
        """
        Update all the columns of the matched table row with the values of the  corresponding
        columns in the source row. If a ``condition`` is specified, then it must be
        true for the new row to be updated.

        See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.

        :param condition: Optional condition of the insert
        :type condition: str or pyspark.sql.Column
        :return: this builder
        """
        new_jbuilder = self.__getMatchedBuilder(condition).updateAll()
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    @since(0.4)  # type: ignore[arg-type]
    def whenMatchedDelete(
        self, condition: OptionalExpressionOrColumn = None
    ) -> "DeltaMergeBuilder":
        """
        Delete a matched row from the table only if the given ``condition`` (if specified) is
        true for the matched row.

        See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.

        :param condition: Optional condition of the delete
        :type condition: str or pyspark.sql.Column
        :return: this builder
        """
        new_jbuilder = self.__getMatchedBuilder(condition).delete()
        return DeltaMergeBuilder(self._spark, new_jbuilder)

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
        """
        Insert a new row to the target table based on the rules defined by ``values``. If a
        ``condition`` is specified, then it must evaluate to true for the new row to be inserted.

        See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.

        :param condition: Optional condition of the insert
        :type condition: str or pyspark.sql.Column
        :param values: Defines the rules of setting the values of columns that need to be updated.
                       *Note: This param is required.* Default value None is present to allow
                       positional args in same order across languages.
        :type values: dict with str as keys and str or pyspark.sql.Column as values
        :return: this builder

        .. versionadded:: 0.4
        """
        jvalues = DeltaTable._dict_to_jmap(self._spark, values, "'values' in whenNotMatchedInsert")
        new_jbuilder = self.__getNotMatchedBuilder(condition).insert(jvalues)
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    @since(0.4)  # type: ignore[arg-type]
    def whenNotMatchedInsertAll(
        self, condition: OptionalExpressionOrColumn = None
    ) -> "DeltaMergeBuilder":
        """
        Insert a new target Delta table row by assigning the target columns to the values of the
        corresponding columns in the source row. If a ``condition`` is specified, then it must
        evaluate to true for the new row to be inserted.

        See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.

        :param condition: Optional condition of the insert
        :type condition: str or pyspark.sql.Column
        :return: this builder
        """
        new_jbuilder = self.__getNotMatchedBuilder(condition).insertAll()
        return DeltaMergeBuilder(self._spark, new_jbuilder)

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
        """
        Update a target row that has no matches in the source based on the rules defined by ``set``.
        If a ``condition`` is specified, then it must evaluate to true for the row to be updated.

        See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.

        :param condition: Optional condition of the update
        :type condition: str or pyspark.sql.Column
        :param set: Defines the rules of setting the values of columns that need to be updated.
                    *Note: This param is required.* Default value None is present to allow
                    positional args in same order across languages.
        :type set: dict with str as keys and str or pyspark.sql.Column as values
        :return: this builder

        .. versionadded:: 2.3
        """
        jset = DeltaTable._dict_to_jmap(self._spark, set, "'set' in whenNotMatchedBySourceUpdate")
        new_jbuilder = self.__getNotMatchedBySourceBuilder(condition).update(jset)
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    @since(2.3)  # type: ignore[arg-type]
    def whenNotMatchedBySourceDelete(
        self, condition: OptionalExpressionOrColumn = None
    ) -> "DeltaMergeBuilder":
        """
        Delete a target row that has no matches in the source from the table only if the given
        ``condition`` (if specified) is true for the target row.

        See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.

        :param condition: Optional condition of the delete
        :type condition: str or pyspark.sql.Column
        :return: this builder
        """
        new_jbuilder = self.__getNotMatchedBySourceBuilder(condition).delete()
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    @since(0.4)  # type: ignore[arg-type]
    def execute(self) -> None:
        """
        Execute the merge operation based on the built matched and not matched actions.

        See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.
        """
        self._jbuilder.execute()

    def __getMatchedBuilder(
        self, condition: OptionalExpressionOrColumn = None
    ) -> "JavaObject":
        if condition is None:
            return self._jbuilder.whenMatched()
        else:
            return self._jbuilder.whenMatched(DeltaTable._condition_to_jcolumn(condition))

    def __getNotMatchedBuilder(
        self, condition: OptionalExpressionOrColumn = None
    ) -> "JavaObject":
        if condition is None:
            return self._jbuilder.whenNotMatched()
        else:
            return self._jbuilder.whenNotMatched(DeltaTable._condition_to_jcolumn(condition))

    def __getNotMatchedBySourceBuilder(
        self, condition: OptionalExpressionOrColumn = None
    ) -> "JavaObject":
        if condition is None:
            return self._jbuilder.whenNotMatchedBySource()
        else:
            return self._jbuilder.whenNotMatchedBySource(
                DeltaTable._condition_to_jcolumn(condition))


class DeltaTableBuilder(object):
    """
    Builder to specify how to create / replace a Delta table.
    You must specify the table name or the path before executing the builder.
    You can specify the table columns, the partitioning columns,
    the location of the data, the table comment and the property,
    and how you want to create / replace the Delta table.

    After executing the builder, a :py:class:`~delta.tables.DeltaTable`
    object is returned.

    Use :py:meth:`delta.tables.DeltaTable.create`,
    :py:meth:`delta.tables.DeltaTable.createIfNotExists`,
    :py:meth:`delta.tables.DeltaTable.replace`,
    :py:meth:`delta.tables.DeltaTable.createOrReplace` to create an object of this class.

    Example 1 to create a Delta table with separate columns, using the table name::

        deltaTable = DeltaTable.create(sparkSession)
            .tableName("testTable")
            .addColumn("c1", dataType = "INT", nullable = False)
            .addColumn("c2", dataType = IntegerType(), generatedAlwaysAs = "c1 + 1")
            .partitionedBy("c1")
            .execute()

    Example 2 to replace a Delta table with existing columns, using the location::

        df = spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])

        deltaTable = DeltaTable.replace(sparkSession)
            .tableName("testTable")
            .addColumns(df.schema)
            .execute()

    .. versionadded:: 1.0

    .. note:: Evolving
    """
    def __init__(self, spark: SparkSession, jbuilder: "JavaObject"):
        self._spark = spark
        self._jbuilder = jbuilder

    def _raise_type_error(self, msg: str, objs: Iterable[Any]) -> NoReturn:
        errorMsg = msg
        for obj in objs:
            errorMsg += " Found %s with type %s" % ((str(obj)), str(type(obj)))
        raise TypeError(errorMsg)

    @since(1.0)  # type: ignore[arg-type]
    def tableName(self, identifier: str) -> "DeltaTableBuilder":
        """
        Specify the table name.
        Optionally qualified with a database name [database_name.] table_name.

        :param identifier: the table name
        :type identifier: str
        :return: this builder

        .. note:: Evolving
        """
        if type(identifier) is not str:
            self._raise_type_error("Identifier must be str.", [identifier])
        self._jbuilder = self._jbuilder.tableName(identifier)
        return self

    @since(1.0)  # type: ignore[arg-type]
    def location(self, location: str) -> "DeltaTableBuilder":
        """
        Specify the path to the directory where table data is stored,
        which could be a path on distributed storage.

        :param location: the data stored location
        :type location: str
        :return: this builder

        .. note:: Evolving
        """
        if type(location) is not str:
            self._raise_type_error("Location must be str.", [location])
        self._jbuilder = self._jbuilder.location(location)
        return self

    @since(1.0)  # type: ignore[arg-type]
    def comment(self, comment: str) -> "DeltaTableBuilder":
        """
        Comment to describe the table.

        :param comment: the table comment
        :type comment: str
        :return: this builder

        .. note:: Evolving
        """
        if type(comment) is not str:
            self._raise_type_error("Table comment must be str.", [comment])
        self._jbuilder = self._jbuilder.comment(comment)
        return self

    @since(1.0)  # type: ignore[arg-type]
    def addColumn(
        self,
        colName: str,
        dataType: Union[str, DataType],
        nullable: bool = True,
        generatedAlwaysAs: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> "DeltaTableBuilder":
        """
        Specify a column in the table

        :param colName: the column name
        :type colName: str
        :param dataType: the column data type
        :type dataType: str or pyspark.sql.types.DataType
        :param nullable: whether column is nullable
        :type nullable: bool
        :param generatedAlwaysAs: a SQL expression if the column is always generated
                                  as a function of other columns.
                                  See online documentation for details on Generated Columns.
        :type generatedAlwaysAs: str
        :param comment: the column comment
        :type comment: str

        :return: this builder

        .. note:: Evolving
        """
        if type(colName) is not str:
            self._raise_type_error("Column name must be str.", [colName])
        if type(dataType) is not str and not isinstance(dataType, DataType):
            self._raise_type_error("Column data type must be str or DataType.",
                                   [dataType])

        jvm: "JVMView" = self._spark._sc._jvm  # type: ignore[attr-defined]
        jsparkSession: "JavaObject" = self._spark._jsparkSession  # type: ignore[attr-defined]

        _col_jbuilder = jvm.io.delta.tables.DeltaTable.columnBuilder(jsparkSession, colName)
        if isinstance(dataType, DataType):
            dataType = jsparkSession.parseDataType(dataType.json())
        _col_jbuilder = _col_jbuilder.dataType(dataType)
        if type(nullable) is not bool:
            self._raise_type_error("Column nullable must be bool.", [nullable])
        _col_jbuilder = _col_jbuilder.nullable(nullable)
        if generatedAlwaysAs is not None:
            if type(generatedAlwaysAs) is not str:
                self._raise_type_error("Column generation expression must be str.",
                                       [generatedAlwaysAs])
            _col_jbuilder = _col_jbuilder.generatedAlwaysAs(generatedAlwaysAs)
        if comment is not None:
            if type(comment) is not str:
                self._raise_type_error("Column comment must be str.", [comment])
            _col_jbuilder = _col_jbuilder.comment(comment)
        self._jbuilder = self._jbuilder.addColumn(_col_jbuilder.build())
        return self

    @since(1.0)  # type: ignore[arg-type]
    def addColumns(
        self, cols: Union[StructType, List[StructField]]
    ) -> "DeltaTableBuilder":
        """
        Specify columns in the table using an existing schema

        :param cols: the columns in the existing schema
        :type cols: pyspark.sql.types.StructType
                    or a list of pyspark.sql.types.StructType.

        :return: this builder

        .. note:: Evolving
        """
        if isinstance(cols, list):
            for col in cols:
                if type(col) is not StructField:
                    self._raise_type_error(
                        "Column in existing schema must be StructField.", [col])
            cols = StructType(cols)
        if type(cols) is not StructType:
            self._raise_type_error("Schema must be StructType " +
                                   "or a list of StructField.",
                                   [cols])

        jsparkSession: "JavaObject" = self._spark._jsparkSession  # type: ignore[attr-defined]

        scalaSchema = jsparkSession.parseDataType(cols.json())
        self._jbuilder = self._jbuilder.addColumns(scalaSchema)
        return self

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

    @since(1.0)  # type: ignore[arg-type]
    def partitionedBy(
        self, *cols: Union[str, List[str], Tuple[str, ...]]
    ) -> "DeltaTableBuilder":
        """
        Specify columns for partitioning

        :param cols: the partitioning cols
        :type cols: str or list name of columns

        :return: this builder

        .. note:: Evolving
        """
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]  # type: ignore[assignment]
        for c in cols:
            if type(c) is not str:
                self._raise_type_error("Partitioning column must be str.", [c])
        self._jbuilder = self._jbuilder.partitionedBy(_to_seq(
            self._spark._sc,  # type: ignore[attr-defined]
            cast(Iterable[Union[Column, str]], cols)
        ))
        return self

    @since(1.0)  # type: ignore[arg-type]
    def property(self, key: str, value: str) -> "DeltaTableBuilder":
        """
        Specify a table property

        :param key: the table property key
        :type value: the table property value

        :return: this builder

        .. note:: Evolving
        """
        if type(key) is not str or type(value) is not str:
            self._raise_type_error("Key and value of property must be string.",
                                   [key, value])
        self._jbuilder = self._jbuilder.property(key, value)
        return self

    @since(1.0)  # type: ignore[arg-type]
    def execute(self) -> DeltaTable:
        """
        Execute Table Creation.

        :rtype: :py:class:`~delta.tables.DeltaTable`

        .. note:: Evolving
        """
        jdt = self._jbuilder.execute()
        return DeltaTable(self._spark, jdt)


class DeltaOptimizeBuilder(object):
    """
    Builder class for constructing OPTIMIZE command and executing.

    Use :py:meth:`delta.tables.DeltaTable.optimize` to create an instance of this class.

    .. versionadded:: 2.0.0
    """
    def __init__(self, spark: SparkSession, jbuilder: "JavaObject"):
        self._spark = spark
        self._jbuilder = jbuilder

    @since(2.0)  # type: ignore[arg-type]
    def where(self, partitionFilter: str) -> "DeltaOptimizeBuilder":
        """
        Apply partition filter on this optimize command builder to limit
        the operation on selected partitions.

        :param partitionFilter: The partition filter to apply
        :type partitionFilter: str
        :return: DeltaOptimizeBuilder with partition filter applied
        :rtype: :py:class:`~delta.tables.DeltaOptimizeBuilder`
        """
        self._jbuilder = self._jbuilder.where(partitionFilter)
        return self

    @since(2.0)  # type: ignore[arg-type]
    def executeCompaction(self) -> DataFrame:
        """
        Compact the small files in selected partitions.

        :return: DataFrame containing the OPTIMIZE execution metrics
        :rtype: pyspark.sql.DataFrame
        """
        return DataFrame(
            self._jbuilder.executeCompaction(),
            getattr(self._spark, "_wrapped", self._spark)  # type: ignore[attr-defined]
        )

    @since(2.0)  # type: ignore[arg-type]
    def executeZOrderBy(self, *cols: Union[str, List[str], Tuple[str, ...]]) -> DataFrame:
        """
        Z-Order the data in selected partitions using the given columns.

        :param cols: the Z-Order cols
        :type cols: str or list name of columns

        :return: DataFrame containing the OPTIMIZE execution metrics
        :rtype: pyspark.sql.DataFrame
        """
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]  # type: ignore[assignment]
        for c in cols:
            if type(c) is not str:
                errorMsg = "Z-order column must be str. "
                errorMsg += "Found %s with type %s" % ((str(c)), str(type(c)))
                raise TypeError(errorMsg)

        return DataFrame(
            self._jbuilder.executeZOrderBy(_to_seq(
                self._spark._sc,  # type: ignore[attr-defined]
                cast(Iterable[Union[Column, str]], cols)
            )),
            getattr(self._spark, "_wrapped", self._spark)  # type: ignore[attr-defined]
        )
