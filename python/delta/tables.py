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
