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
    def forPath(cls, path, sparkSession):
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
