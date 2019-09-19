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
        You can create DeltaTable instances using the class methods.

        e.g DeltaTable.forPath(spark, path)

        .. note:: Evolving
    """

    def __init__(self, spark, jdt):
        self._spark = spark
        self._jdt = jdt

    def toDF(self):
        """
        Get a DataFrame representation of this Delta table.

        .. note:: Evolving
        """
        return DataFrame(self._jdt.toDF(), self._spark._wrapped)

    def alias(self, aliasName):
        """
        Apply an alias to the Delta table.

        .. note:: Evolving
        """
        jdt = self._jdt.alias(aliasName)
        return DeltaTable(self._spark, jdt)

    def delete(self, condition=None):
        """
        Delete data from the table that match the given ``condition``.

        :param condition: condition of the update
        :type condition: str or pyspark.sql.Column

        .. note:: Evolving
        """
        if condition is None:
            self._jdt.delete()
        else:
            self._jdt.delete(self._condition_to_jcolumn(condition))

    def update(self, condition=None, set=None):
        """
        Update data from the table on the rows that match the given ``condition``,
        which performs the rules defined by ``set``.

        :param condition: condition of the update
        :type condition: str or pyspark.sql.Column
        :param set: defines the rules of setting the values of columns that need to be updated
        :type set: dict between str as keys and str or pyspark.sql.Column as values

        .. note:: Evolving
        """
        # Handle the case where this func was called with positional args and only one arg
        if condition is not None and set is None and type(condition) is dict:
            set = condition
            condition = None

        jmap = self._dict_to_jmap(self._spark, set, "set")
        if condition is None:
            self._jdt.update(jmap)
        else:
            self._jdt.update(self._condition_to_jcolumn(condition), jmap)

    def merge(self, source, condition):
        """
        Merge data from the `source` DataFrame based on the given merge `condition`. This returns
        a :class:`DeltaMergeBuilder` object that can be used to specify the update, delete, or
        insert actions to be performed on rows based on whether the rows matched the condition or
        not. See :class:`DeltaMergeBuilder` for a full description of this operation and what
        combinations of update, delete and insert operations are allowed.

        :return: a DeltaMergeBuilder object

        .. note:: Evolving
        """
        if source is None:
            raise ValueError("'condition' argument cannot be None")
        elif type(source) is not DataFrame:
            raise TypeError("Type of 'source' argument can only be DataFrame.")
        if condition is None:
            raise ValueError("'condition' argument cannot be None")

        jbuilder = self._jdt.merge(source._jdf, self._condition_to_jcolumn(condition))
        return DeltaMergeBuilder(self._spark, jbuilder)

    def vacuum(self, retentionHours=None):
        """
        Recursively delete files and directories in the table that are not needed by the table for
        maintaining older versions up to the given retention threshold. This method will return an
        empty DataFrame on successful completion. This uses the default retention period of 7
        days.
        :param retentionHours:
        :return: DataFrame of the history

        .. note:: Evolving
        """
        jdt = self._jdt
        if retentionHours is None:
            return DataFrame(jdt.vacuum(), self._spark._wrapped)
        else:
            return DataFrame(jdt.vacuum(retentionHours), self._spark._wrapped)

    def history(self, limit=None):
        """
        Get the information of the latest `limit` commits on this table as a Spark DataFrame.
        The information is in reverse chronological order.
        :param limit:
        :return: DataFrame of the history

        .. note:: Evolving
        """
        jdt = self._jdt
        if limit is None:
            return DataFrame(jdt.history(), self._spark._wrapped)
        else:
            return DataFrame(jdt.history(limit), self._spark._wrapped)

    @classmethod
    def convertToDelta(cls, sparkSession, identifier, partitionSchema=None):
        """
        Create a DeltaTable from the given parquet table. Takes an existing parquet table and
        constructs a delta transaction log in the base path of the table.
        Note: Any changes to the table during the conversion process may not result in a consistent
        state at the end of the conversion. Users should stop any changes to the table before the
        conversion is started.

        :param sparkSession:
        :param identifier:
        :param partitionSchema:
        :return: DeltaTable
        """
        assert sparkSession is not None
        if partitionSchema is None:
            jdt = sparkSession._sc._jvm.io.delta.tables.DeltaTable.convertToDelta(
                sparkSession._jsparkSession, identifier)
        else:
            jdt = sparkSession._sc._jvm.io.delta.tables.DeltaTable.convertToDelta(
                sparkSession._jsparkSession, identifier,
                sparkSession._jsparkSession.parseDataType(partitionSchema.json()))
        return jdt

    @classmethod
    def forPath(cls, sparkSession, path):
        """
        Create a DeltaTable for the data at the given `path` using the given SparkSession.

        .. note:: Evolving
        """
        assert sparkSession is not None
        jdt = sparkSession._sc._jvm.io.delta.tables.DeltaTable.forPath(
            sparkSession._jsparkSession, path)
        return DeltaTable(sparkSession, jdt)

    @classmethod
    def _dict_to_jmap(cls, sparkSession, pydict, argname):
        """
        convert dict<str, pColumn/str> to Map<str, jColumn>
        """
        # Get the Java map for pydict
        if pydict is None:
            raise ValueError("'%s' argument cannot be None" % argname)
        elif type(pydict) is not dict:
            e = "argument '%s'  must be a dict, found to be %s." % (argname, str(type(pydict)))
            raise TypeError(e)

        jmap = sparkSession._sc._jvm.java.util.HashMap()
        for col, expr in pydict.items():
            if type(expr) is Column:
                jmap.put(col, expr._jc)
            elif type(expr) is str:
                jmap.put(col, functions.expr(expr)._jc)
            else:
                e = "dict in argument '%s' must contain only Columns or strs as values" % expr
                raise TypeError(e)
        return jmap

    @classmethod
    def _condition_to_jcolumn(cls, condition):
        if condition is None:
            jcondition = None
        elif type(condition) is Column:
            jcondition = condition._jc
        elif type(condition) is str:
            jcondition = functions.expr(condition)._jc
        else:
            e = "argument 'condition'  must be a dict, found to be %s." % str(type(condition))
            raise TypeError(e)
        return jcondition


class DeltaMergeBuilder(object):
    """
    Builder to specify how to merge data from source DataFrame into the target Delta table. You can
    specify 1, 2 or 3 ``when`` clauses of which there can be at most 2 ``whenMatched`` clauses
    and at most 1 ```whenNotMatched``` clause. Here are the constraints on these clauses.

    - ``whenMatched`` clauses:

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
          ``whenMatchedUpdateAll()``. This is equivalent to
            <pre>
                whenMatched(...).updateExpr(Map(
                  ("col1", "source.col1"),
                  ("col2", "source.col2"),
                  ...))
            </pre>

    - ``whenNotMatched`` clauses:

        - This clause can have only an ``insert`` action, which can have an optional condition.

        - If the ``whenNotMatched`` clause is not present or if it is present but the non-matching
          source row does not satisfy the condition, then the source row is not inserted.

        - If you want to insert all the columns of the target Delta table with the
          corresponding column of the source DataFrame, then you can use
          ``whenMatchedInsertAll()``. This is equivalent to
          <pre>
            whenMatched(...).insertExpr(Map(
              ("col1", "source.col1"),
              ("col2", "source.col2"),
              ...))
          </pre>

    .. note:: Evolving
    """

    def __init__(self, spark, jbuilder):
        self._spark = spark
        self._jbuilder = jbuilder

    def whenMatchedUpdate(self, condition=None, set=None):
        """
        Update a matched table row based on the rules defined by ``set`` only if the given
        ``condition`` (if specified) is true for the matched row.

        .. note:: Evolving
        """
        # Handle the case where this func was called with positional args and only one arg
        if condition is not None and set is None and type(condition) is dict:
            set = condition
            condition = None

        jset = DeltaTable._dict_to_jmap(self._spark, set, "set")
        new_jbuilder = self.__getMatchedBuilder(condition).update(jset)
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    def whenMatchedUpdateAll(self, condition=None):
        """
        Update all the columns of the matched table row with the values of the
        corresponding columns in the source row only if the given
        ``condition`` (if specified) is true for the matched row.

        .. note:: Evolving
        """
        new_jbuilder = self.__getMatchedBuilder(condition).updateAll()
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    def whenMatchedDelete(self, condition=None):
        """
        Delete a matched row from the table only if the given ``condition`` (if specified) is
        true for the matched row.

        .. note:: Evolving
        """
        new_jbuilder = self.__getMatchedBuilder(condition).delete()
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    def whenNotMatchedInsert(self, condition=None, values=None):
        """
        Insert a new row to the target table based on the rules defined by ``values``
        only if the given ``condition`` (if specified) is true for the new row.

        .. note:: Evolving
        """
        # Handle the case where this func was called with positional args and only one arg
        if condition is not None and values is None and type(condition) is dict:
            values = condition
            condition = None

        jvalues = DeltaTable._dict_to_jmap(self._spark, values, "values")
        new_jbuilder = self.__getNotMatchedBuilder(condition).insert(jvalues)
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    def whenNotMatchedInsertAll(self, condition=None):
        """
        Insert a new target Delta table row by assigning the target columns to the values of the
        corresponding columns in the source row only if the given ``condition`` (if specified)
        is true for the new row.

        .. note:: Evolving
        """
        new_jbuilder = self.__getNotMatchedBuilder(condition).insertAll()
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    def execute(self):
        """
        Execute the merge operation based on the built matched and not matched actions.

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
