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

    def delete(self, where=None):
        """
        Delete data that match the given `where`.

        .. note:: Evolving
        """
        if where is None:
            self._jdt.delete()
        elif type(where) is Column:
            self._jdt.delete(where._jc)
        elif type(where) is str:
            self._jdt.delete(where)
        else:
            raise TypeError("Type of 'where' argument can only be str or Column.")

    def update(self, where=None, set=None):
        """
        Update data that match the given `where` based on the rules defined by `set`.
        Based on the features of Python, `set` can be a dict with type {str: str/Column}, and
        type of `where` can be either str or Column or None.

        .. note:: Evolving
        """

        # Handle the case where this func was called with positional args and only one arg
        if where is not None and set is None and type(where) is dict:
            set = where
            where = None

        if set is None:
            raise ValueError("'set' argument cannot be None")
        elif type(set) is not dict:
            raise TypeError("Type of 'set' argument must be dict")

        jSetMap = self.__convert_dict_to_map(self._spark, set)

        if where is None:
            self._jdt.update(jSetMap)
        elif type(where) is Column:
            self._jdt.update(where._jc, jSetMap)
        elif type(where) is str:
            self._jdt.update(functions.expr(where)._jc, jSetMap)
        else:
            raise TypeError("Type of 'where' argument can only be str or Column.")

    def merge(self, sourceDF, condition):
        """
        Merge data from the `source` DataFrame based on the given merge `condition`.

        .. note:: Evolving
        """
        j_dmb = self._jdt.merge(sourceDF._jdf, condition._jc) \
            if type(condition) is Column else self._jdt.merge(sourceDF._jdf, condition)
        return DeltaMergeBuilder(self._spark, j_dmb)

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
    def __convert_dict_to_map(cls, sparkSession, dict):
        """
        convert dict<str, pColumn/str> to Map<str, jColumn>
        """
        m = sparkSession._sc._jvm.java.util.HashMap()
        for col, expr in dict.items():
            if type(expr) is Column:
                m.put(col, expr._jc)
            elif type(expr) is str:
                m.put(col, functions.expr(expr)._jc)
            else:
                raise TypeError("Dict can contain only Columns or strs as values")
        return m


class DeltaMergeBuilder:

    def __init__(self, spark, j_mergebuilder):
        self._spark = spark
        self._j_mergebuilder = j_mergebuilder

    def whenMatchedUpdate(self, set, condition=None):
        j_matchedbuilder = self.__getMatchedBuilder(condition)
        m = self.__convert_dict_to_map(set)
        return DeltaMergeBuilder(self._spark, j_matchedbuilder.update(m))

    def whenMatchedUpdateAll(self, condition=None):
        j_matchedbuilder = self.__getMatchedBuilder(condition)
        return DeltaMergeBuilder(self._spark, j_matchedbuilder.updateAll())

    def whenMatchedThenDelete(self, condition=None):
        j_matchedbuilder = self.__getMatchedBuilder(condition)
        return DeltaMergeBuilder(self._spark, j_matchedbuilder.delete())

    def whenNotMatchedThenInsert(self, actions, condition=None):
        j_not_matchedbuilder = self.__getNotMatchedBuilder(condition)
        m = self.__convert_dict_to_map(actions)
        return DeltaMergeBuilder(self._spark, j_not_matchedbuilder.insert(m))

    def whenNotMatchedThenInsertAll(self, condition=None):
        j_not_matchedbuilder = self.__getNotMatchedBuilder(condition)
        return DeltaMergeBuilder(self._spark, j_not_matchedbuilder.insertAll())

    def execute(self):
        self._j_mergebuilder.execute()

    def __getMatchedBuilder(self, condition=None):
        return self._j_mergebuilder.whenMatched() if condition is None \
            else self._j_mergebuilder.whenMatched(condition._jc) if type(condition) is Column \
            else self._j_mergebuilder.whenMatched(condition)

    def __getNotMatchedBuilder(self, condition=None):
        return self._j_mergebuilder.whenNotMatched() if condition is None \
            else self._j_mergebuilder.whenNotMatched(condition._jc) if type(condition) is Column \
            else self._j_mergebuilder.whenNotMatched(condition)

    def __convert_dict_to_map(self, d):
        m = self._spark._sc._jvm.java.util.HashMap()
        for col, expr in d.items():
            if type(expr) is Column:
                m.put(col, expr._jc)
            else:
                m.put(col, functions.expr(expr)._jc)
        return m
