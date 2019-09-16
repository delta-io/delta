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

import unittest
import tempfile
import shutil
import os

from pyspark.sql import SQLContext, functions, Row, SparkSession

from delta.testing.utils import PySparkTestCase


class DeltaSqlTests(PySparkTestCase):

    def setUp(self):
        super(DeltaTableTests, self).setUp()
        spark = SparkSession(self.sc)
        if self.sc.version < "3.":
            # Manually activate "DeltaSparkSessionExtension" in PySpark 2.4 in a cloned session
            # because "spark.sql.extensions" is not picked up. (See SPARK-25003).
            self.sc._jvm.io.delta.sql.DeltaSparkSessionExtension() \
                .apply(spark._jsparkSession.extensions())
            self.spark = SparkSession(self.sc, spark._jsparkSession.cloneSession())
        else:
            self.spark = spark
        self.tempPath = tempfile.mkdtemp()
        self.tempFile = os.path.join(self.tempPath, "tempFile")

    def tearDown(self):
        self.spark.stop()
        shutil.rmtree(self.tempPath)
        super(DeltaTableTests, self).tearDown()

    def test_vacuum(self):
        df = self.spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
        df.write.format("delta").save(self.tempFile)
        deleted_files = self.spark.sql("VACUUM '%s' RETAIN 0 HOURS" % self.tempFile).collect()
        self.assertTrue(self.tempFile in deleted_files[0][0])

if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)
