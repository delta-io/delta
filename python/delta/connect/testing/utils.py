#
# Copyright (2024) The Delta Lake Project Authors.
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

import tempfile
import shutil
import os
import uuid

from contextlib import contextmanager
from pyspark import SparkConf
from pyspark.testing.connectutils import ReusedConnectTestCase
from typing import Generator


class DeltaTestCase(ReusedConnectTestCase):
    """
    Test suite base for setting up a properly configured SparkSession for using Delta Connect.
    """

    @classmethod
    def setUpClass(cls) -> None:
        # Spark Connect will set SPARK_CONNECT_TESTING_REMOTE, and it does not allow MASTER
        # to be set simultaneously, so we need to clear it.
        # TODO(long.vu): Find a cleaner way to clear "MASTER".
        if "MASTER" in os.environ:
            del os.environ["MASTER"]
        super(DeltaTestCase, cls).setUpClass()

    @classmethod
    def conf(cls) -> SparkConf:
        _conf = super(DeltaTestCase, cls).conf()
        _conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        _conf.set("spark.sql.catalog.spark_catalog",
                  "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        _conf.set("spark.connect.extensions.relation.classes",
                  "org.apache.spark.sql.connect.delta.DeltaRelationPlugin")
        _conf.set("spark.connect.extensions.command.classes",
                  "org.apache.spark.sql.connect.delta.DeltaCommandPlugin")
        return _conf

    def setUp(self) -> None:
        super(DeltaTestCase, self).setUp()
        self.tempPath = tempfile.mkdtemp()
        self.tempFile = os.path.join(self.tempPath, "tempFile")

    def tearDown(self) -> None:
        super(DeltaTestCase, self).tearDown()
        shutil.rmtree(self.tempPath)

    @contextmanager
    def tempTable(self) -> Generator[str, None, None]:
        table_name = "table_" + str(uuid.uuid4()).replace("-", "_")

        with super(DeltaTestCase, self).table(table_name):
            yield table_name
