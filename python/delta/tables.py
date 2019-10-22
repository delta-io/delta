#
# Copyright 2019 Databricks, Inc.
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

import sys
import tempfile
from pyspark import SparkContext
from pyspark import since
from pyspark.sql import Column, DataFrame, SparkSession, SQLContext, functions
from py4j.java_collections import MapConverter


class DeltaTable(object):
    """
        Main class for programmatically interacting with Delta tables.
        You can create DeltaTable instances using the path of the Delta table.::

            deltaTable = DeltaTable.forPath(spark, "/path/to/table")

        In addition, you can convert an existing Parquet table in place into a Delta table.::

            deltaTable = DeltaTable.convertToDelta(spark, "parquet.`/path/to/table`")

        .. versionadded:: 0.4

        .. note:: Evolving
    """
    def __init__(self, spark, jdt):
        self._spark = spark
        self._jdt = jdt

    @since(0.4)
    def toDF(self):
        """
        Get a DataFrame representation of this Delta table.

        .. note:: Evolving
        """
        return DataFrame(self._jdt.toDF(), self._spark._wrapped)

    @since(0.4)
    def alias(self, aliasName):
        """
        Apply an alias to the Delta table.

        .. note:: Evolving
        """
        jdt = self._jdt.alias(aliasName)
        return DeltaTable(self._spark, jdt)

    @since(0.4)
    def delete(self, condition=None):
        """
        Delete data from the table that match the given ``condition``.

        Example::

            deltaTable.delete("date < '2017-01-01'")        # predicate using SQL formatted string

            deltaTable.delete(col("date") < "2017-01-01")   # predicate using Spark SQL functions

        :param condition: condition of the update
        :type condition: str or pyspark.sql.Column

        .. note:: Evolving
        """
        if condition is None:
            self._jdt.delete()
        else:
            self._jdt.delete(self._condition_to_jcolumn(condition))

    @since(0.4)
    def update(self, condition=None, set=None):
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

        .. note:: Evolving
        """
        jmap = self._dict_to_jmap(self._spark, set, "'set'")
        jcolumn = self._condition_to_jcolumn(condition)
        if condition is None:
            self._jdt.update(jmap)
        else:
            self._jdt.update(jcolumn, jmap)

    @since(0.4)
    def merge(self, source, condition):
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

        .. note:: Evolving
        """
        if source is None:
            raise ValueError("'source' in merge cannot be None")
        elif type(source) is not DataFrame:
            raise TypeError("Type of 'source' in merge must be DataFrame.")
        if condition is None:
            raise ValueError("'condition' in merge cannot be None")

        jbuilder = self._jdt.merge(source._jdf, self._condition_to_jcolumn(condition))
        return DeltaMergeBuilder(self._spark, jbuilder)

    @since(0.4)
    def vacuum(self, retentionHours=None):
        """
        Recursively delete files and directories in the table that are not needed by the table for
        maintaining older versions up to the given retention threshold. This method will return an
        empty DataFrame on successful completion.

        Example::

            deltaTable.vacuum()     # vacuum files not required by versions more than 7 days old

            deltaTable.vacuum(100)  # vacuum files not required by versions more than 100 hours old

        :param retentionHours: Optional number of hours retain history. If not specified, then the
                               default retention period of 168 hours (7 days) will be used.

        .. note:: Evolving
        """
        jdt = self._jdt
        if retentionHours is None:
            return DataFrame(jdt.vacuum(), self._spark._wrapped)
        else:
            return DataFrame(jdt.vacuum(float(retentionHours)), self._spark._wrapped)

    @since(0.4)
    def history(self, limit=None):
        """
        Get the information of the latest `limit` commits on this table as a Spark DataFrame.
        The information is in reverse chronological order.

        Example::

            fullHistoryDF = deltaTable.history()    # get the full history of the table

            lastOperationDF = deltaTable.history(1) # get the last operation

        :param limit: Optional, number of latest commits to returns in the history.
        :return: Table's commit history. See the online Delta Lake documentation for more details.
        :rtype: pyspark.sql.DataFrame

        .. note:: Evolving
        """
        jdt = self._jdt
        if limit is None:
            return DataFrame(jdt.history(), self._spark._wrapped)
        else:
            return DataFrame(jdt.history(limit), self._spark._wrapped)

    @classmethod
    @since(0.4)
    def convertToDelta(cls, sparkSession, identifier, partitionSchema=None):
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
        :param partitionSchema:
        :param partitionSchema: Hive DDL formatted string, or pyspark.sql.types.StructType
        :return: DeltaTable representing the converted Delta table
        :rtype: :py:class:`~delta.tables.DeltaTable`

        .. note:: Evolving
        """
        assert sparkSession is not None
        if partitionSchema is None:
            jdt = sparkSession._sc._jvm.io.delta.tables.DeltaTable.convertToDelta(
                sparkSession._jsparkSession, identifier)
        else:
            if not isinstance(partitionSchema, str):
                partitionSchema = sparkSession._jsparkSession.parseDataType(partitionSchema.json())
            jdt = sparkSession._sc._jvm.io.delta.tables.DeltaTable.convertToDelta(
                sparkSession._jsparkSession, identifier,
                partitionSchema)
        return jdt

    @classmethod
    @since(0.4)
    def forPath(cls, sparkSession, path):
        """
        Create a DeltaTable for the data at the given `path` using the given SparkSession.

        :param sparkSession: SparkSession to use for loading the table
        :type sparkSession: pyspark.sql.SparkSession
        :return: loaded Delta table
        :rtype: :py:class:`~delta.tables.DeltaTable`

        Example::

            deltaTable = DeltaTable.forPath(spark, "/path/to/table")

        .. note:: Evolving
        """
        assert sparkSession is not None
        jdt = sparkSession._sc._jvm.io.delta.tables.DeltaTable.forPath(
            sparkSession._jsparkSession, path)
        return DeltaTable(sparkSession, jdt)

    @classmethod
    @since(0.4)
    def isDeltaTable(cls, sparkSession, identifier):
        """
        Check if the provided `identifier` string, in this case a file path,
        is the root of a Delta table using the given SparkSession.

        :param sparkSession: SparkSession to use to perform the check
        :param path: location of the table
        :return: If the table is a delta table or not
        :rtype: bool

        Example::

            DeltaTable.isDeltaTable(spark, "/path/to/table")

        .. note:: Evolving
        """
        assert sparkSession is not None
        return sparkSession._sc._jvm.io.delta.tables.DeltaTable.isDeltaTable(
            sparkSession._jsparkSession, identifier)

    @classmethod
    def _dict_to_jmap(cls, sparkSession, pydict, argname):
        """
        convert dict<str, pColumn/str> to Map<str, jColumn>
        """
        # Get the Java map for pydict
        if pydict is None:
            raise ValueError("%s cannot be None" % argname)
        elif type(pydict) is not dict:
            e = "%s must be a dict, found to be %s" % (argname, str(type(pydict)))
            raise TypeError(e)

        jmap = sparkSession._sc._jvm.java.util.HashMap()
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

    @classmethod
    def _condition_to_jcolumn(cls, condition, argname="'condition'"):
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
    Using this builder, you can specify 1, 2 or 3 ``when`` clauses of which there can be at most
    2 ``whenMatched`` clauses and at most 1 ``whenNotMatched`` clause.
    Here are the constraints on these clauses.

    - Constraints in the ``whenMatched`` clauses:

      - There can be at most one ``update`` action and one ``delete`` action in `whenMatched`
        clauses.

      - Each ``whenMatched`` clause can have an optional condition. However, if there are two
        ``whenMatched`` clauses, then the first one must have a condition.

      - When there are two ``whenMatched`` clauses and there are conditions (or the lack of)
        such that a row matches both clauses, then the first clause/action is executed.
        In other words, the order of the ``whenMatched`` clauses matter.

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

      - This clause can have only an ``insert`` action, which can have an optional condition.

      - If ``whenNotMatchedInsert`` is not present or if it is present but the non-matching
        source row does not satisfy the condition, then the source row is not inserted.

      - If you want to insert all the columns of the target Delta table with the
        corresponding column of the source DataFrame, then you can use
        ``whenNotMatchedInsertAll()``. This is equivalent to::

            whenMatchedInsert(values = {
              "col1": "source.col1",
              "col2": "source.col2",
              ...    # for all columns in the delta table
            })

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

    .. versionadded:: 0.4

    .. note:: Evolving
    """
    def __init__(self, spark, jbuilder):
        self._spark = spark
        self._jbuilder = jbuilder

    @since(0.4)
    def whenMatchedUpdate(self, condition=None, set=None):
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

        .. note:: Evolving
        """
        jset = DeltaTable._dict_to_jmap(self._spark, set, "'set' in whenMatchedUpdate")
        new_jbuilder = self.__getMatchedBuilder(condition).update(jset)
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    @since(0.4)
    def whenMatchedUpdateAll(self, condition=None):
        """
        Update all the columns of the matched table row with the values of the  corresponding
        columns in the source row. If a ``condition`` is specified, then it must be
        true for the new row to be updated.

        See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.

        :param condition: Optional condition of the insert
        :type condition: str or pyspark.sql.Column
        :return: this builder

        .. note:: Evolving
        """
        new_jbuilder = self.__getMatchedBuilder(condition).updateAll()
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    @since(0.4)
    def whenMatchedDelete(self, condition=None):
        """
        Delete a matched row from the table only if the given ``condition`` (if specified) is
        true for the matched row.

        See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.

        :param condition: Optional condition of the delete
        :type condition: str or pyspark.sql.Column
        :return: this builder

        .. note:: Evolving
        """
        new_jbuilder = self.__getMatchedBuilder(condition).delete()
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    @since(0.4)
    def whenNotMatchedInsert(self, condition=None, values=None):
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

        .. note:: Evolving
        """
        jvalues = DeltaTable._dict_to_jmap(self._spark, values, "'values' in whenNotMatchedInsert")
        new_jbuilder = self.__getNotMatchedBuilder(condition).insert(jvalues)
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    @since(0.4)
    def whenNotMatchedInsertAll(self, condition=None):
        """
        Insert a new target Delta table row by assigning the target columns to the values of the
        corresponding columns in the source row. If a ``condition`` is specified, then it must
        evaluate to true for the new row to be inserted.

        See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.

        :param condition: Optional condition of the insert
        :type condition: str or pyspark.sql.Column
        :return: this builder

        .. note:: Evolving
        """
        new_jbuilder = self.__getNotMatchedBuilder(condition).insertAll()
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    @since(0.4)
    def execute(self):
        """
        Execute the merge operation based on the built matched and not matched actions.

        See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.

        .. note:: Evolving
        """
        self._jbuilder.execute()

    def __getMatchedBuilder(self, condition=None):
        if condition is None:
            return self._jbuilder.whenMatched()
        else:
            return self._jbuilder.whenMatched(DeltaTable._condition_to_jcolumn(condition))

    def __getNotMatchedBuilder(self, condition=None):
        if condition is None:
            return self._jbuilder.whenNotMatched()
        else:
            return self._jbuilder.whenNotMatched(DeltaTable._condition_to_jcolumn(condition))
