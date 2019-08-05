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


import glob
import os
import struct
import sys
import unittest

from pyspark import SparkContext, SparkConf


class PySparkTestCase(unittest.TestCase):

    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        conf = SparkConf().set("spark.databricks.pyspark.enablePy4JSecurity", "true")
        self.sc = SparkContext('local[4]', class_name, conf=conf)

    def tearDown(self):
        self.sc.stop()
        sys.path = self._old_sys_path


class DeltaTableTests(PySparkTestCase):
    def helloPySpark(self):
        print("hello pyspark!")


if __name__ == "__main__":
    t = DeltaTableTests()
