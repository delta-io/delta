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
from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
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
    def forPath(cls, path, spark=None):
        if spark is None:
            spark = SparkSession.getActiveSession()
        assert spark is not None
        return DeltaTable(spark, spark._sc._jvm.io.delta.tables.DeltaTable.forPath(path))
