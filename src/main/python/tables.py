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
from pyspark.sql import Column, DataFrame, SparkSession, SQLContext, functions
from py4j.java_collections import MapConverter


class DeltaTable(object):

    """
    Construct a DeltaTable using java_SparkSession and java_DeltaTable object.
    """
    def __init__(self, spark, j_deltatable):
        self._spark = spark
        self._j_deltatable = j_deltatable

    """
    Get the underneath DataFrame of a DeltaTable.
    """
    def toDF(self):
        assert self._spark is not None and self._j_deltatable is not None
        return DataFrame(self._j_deltatable.toDF(), self._spark._wrapped)

    """
    Create a DeltaTable based on the delta path and spark session.
    """
    @classmethod
    def forPath(cls, path, sparkSession=None):
        if sparkSession is None:
            # pyspark don't have getActiveSession method.
            sparkSession = SparkSession.builder.getOrCreate()
        assert sparkSession is not None
        return DeltaTable(
            sparkSession, sparkSession._sc._jvm.io.delta.tables.DeltaTable.forPath(path))


    """
    Delete data that match the given `where`.
    """
    def delete(self, where=None):
        if where is None:
            self._j_deltatable.delete()
        elif type(where) is Column:
            self._j_deltatable.delete(where._jc)
        elif type(where) is str:
            self._j_deltatable.delete(where)
        else:
            raise Exception("type of 'where' can only be None, str, Column.")

    """
    Update data that match the given `where` based on the rules defined by `set`.
    
    Based on the features of Python, `set` can be a dict with type {str: str/Column}, and
    type of `where` can be either str or Column or None.
    """
    def update(self, set, where=None):
        m = self.__convert_dict_to_map(set)
        if where is None:
            self._j_deltatable.update(m)
        elif type(where) is Column:
            self._j_deltatable.update(where._jc, m)
        else:
            self._j_deltatable.update(functions.expr(where)._jc, m)

    """
    Merge data from the `source` DataFrame based on the given merge `condition`.
    """
    def merge(self, sourceDF, condition):
        j_dmb = self._j_deltatable.merge(sourceDF._jdf, condition._jc)\
            if type(condition) is Column else self._j_deltatable.merge(sourceDF._jdf, condition)
        return DeltaMergeBuilder(self._spark, j_dmb)

    """
    convert dict<str, pColumn/str> to Map<str, jColumn>
    """
    def __convert_dict_to_map(self, d):
        m = self._spark._sc._jvm.java.util.HashMap()
        for col, expr in d.items():
            if type(expr) is Column:
                m.put(col, expr._jc)
            else:
                m.put(col, functions.expr(expr)._jc)
        return m


class DeltaMergeBuilder:
    def __init__(self, spark, j_mergebuilder):
        self._spark = spark
        self._j_mergebuilder = j_mergebuilder

    def whenMatched(self, condition=None):
        if condition is None:
            return DeltaMergeMatchedActionBuilder(
                self._spark, self._j_mergebuilder.whenMatched())
        elif type(condition) is str:
            return DeltaMergeMatchedActionBuilder(
                self._spark, self._j_mergebuilder.whenMatched(condition))
        elif type(condition) is Column:
            return DeltaMergeMatchedActionBuilder(
                self._spark, self._j_mergebuilder.whenMatched(condition._jc))
        else:
            raise Exception("type of condition can only be None, str, Column.")

    def whenNotMatched(self, condition=None):
        if condition is None:
            return DeltaMergeNotMatchedActionBuilder(
                self._spark, self._j_mergebuilder.whenNotMatched())
        elif type(condition) is str:
            return DeltaMergeNotMatchedActionBuilder(
                self._spark, self._j_mergebuilder.whenNotMatched(condition))
        elif type(condition) is Column:
            return DeltaMergeNotMatchedActionBuilder(
                self._spark, self._j_mergebuilder.whenNotMatched(condition._jc))
        else:
            raise Exception("type of condition can only be None, str, Column.")

    def execute(self):
        self._j_mergebuilder.execute()


class DeltaMergeMatchedActionBuilder:
    def __init__(self, spark, j_matched_builder):
        self._spark = spark
        self._j_matched_builder = j_matched_builder

    def update(self, setCol):
        m = self.__convert_dict_to_map(setCol)
        return DeltaMergeBuilder(self._spark, self._j_matched_builder.update(m))

    def updateExpr(self, setStr):
        m = MapConverter().convert(setStr, self._spark._sc._jvm._gateway_client)
        return DeltaMergeBuilder(self._spark, self._j_matched_builder.updateExpr(m))

    def updateAll(self):
        return DeltaMergeBuilder(self._spark, self._j_matched_builder.updateAll())

    def delete(self):
        return DeltaMergeBuilder(self._spark, self._j_matched_builder.delete())

    """
    convert dict<str, pColumn/str> to Map<str, jColumn>
    """
    def __convert_dict_to_map(self, d):
        m = self._spark._sc._jvm.java.util.HashMap()
        for col, expr in d.items():
            if type(expr) is Column:
                m.put(col, expr._jc)
            else:
                m.put(col, functions.expr(expr)._jc)
        return m


class DeltaMergeNotMatchedActionBuilder:
    def __init__(self, spark, j_notmatched_builder):
        self._spark = spark
        self._j_notmatched_builder = j_notmatched_builder

    def insert(self, values):
        m = self.__convert_dict_to_map(values)
        return DeltaMergeBuilder(self._spark, self._j_notmatched_builder.insert(m))

    def insertExpr(self, values):
        m = MapConverter().convert(values, self._spark._sc._jvm._gateway_client)
        return DeltaMergeBuilder(self._spark, self._j_notmatched_builder.insertExpr(m))

    def insertAll(self):
        return DeltaMergeBuilder(self._spark, self._j_notmatched_builder.insertAll())

    """
    convert dict<str, pColumn/str> to Map<str, jColumn>
    """
    def __convert_dict_to_map(self, d):
        m = self._spark._sc._jvm.java.util.HashMap()
        for col, expr in d.items():
            if type(expr) is Column:
                m.put(col, expr._jc)
            else:
                m.put(col, functions.expr(expr)._jc)
        return m
