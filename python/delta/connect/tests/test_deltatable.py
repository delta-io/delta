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

import os
import unittest
import sys

from delta.connect.testing.utils import DeltaTestCase

path_to_delta_connect_tests_folder = os.path.dirname(os.path.abspath(__file__))
path_to_delta_folder = os.path.dirname(os.path.dirname(path_to_delta_connect_tests_folder))
sys.path.append(path_to_delta_folder)

from tests.test_deltatable import DeltaTableTestsMixin


class DeltaTableTests(DeltaTableTestsMixin, DeltaTestCase):
    @unittest.skip("create has not been implemented yet")
    def test_create_table_with_existing_schema(self):
        pass

    @unittest.skip("createOrReplace has not been implemented yet")
    def test_create_replace_table_with_cluster_by(self):
        pass

    @unittest.skip("createOrReplace has not been implemented yet")
    def test_create_replace_table_with_no_spark_session_passed(self):
        pass

    @unittest.skip("create has not been implemented yet")
    def test_create_table_with_name_only(self):
        pass

    @unittest.skip("create has not been implemented yet")
    def test_create_table_with_location_only(self):
        pass

    @unittest.skip("create has not been implemented yet")
    def test_create_table_with_name_and_location(self):
        pass

    @unittest.skip("create has not been implemented yet")
    def test_create_table_behavior(self):
        pass

    @unittest.skip("replace has not been implemented yet")
    def test_replace_table_with_name_only(self):
        pass

    @unittest.skip("replace has not been implemented yet")
    def test_replace_table_with_location_only(self):
        pass

    @unittest.skip("replace has not been implemented yet")
    def test_replace_table_with_name_and_location(self):
        pass

    @unittest.skip("replace has not been implemented yet")
    def test_replace_table_behavior(self):
        pass

    @unittest.skip("create has not been implemented yet")
    def test_verify_paritionedBy_compatibility(self):
        pass

    @unittest.skip("create has not been implemented yet")
    def test_create_table_with_identity_column(self):
        pass

    @unittest.skip("create has not been implemented yet")
    def test_delta_table_builder_with_bad_args(self):
        pass

    @unittest.skip("addFeatureSupport has not been implemented yet")
    def test_addFeatureSupport(self):
        pass

    @unittest.skip("dropFeatureSupport has not been implemented yet")
    def test_dropFeatureSupport(self):
        pass

    @unittest.skip("clusterBy has not been implemented yet")
    def test_create_table_with_cluster_by(self):
        pass

    @unittest.skip("clusterBy has not been implemented yet")
    def test_replace_table_with_cluster_by(self):
        pass

    @unittest.skip("clusterBy has not been implemented yet")
    def __test_table_with_cluster_by(self) -> None:
        pass

    @unittest.skip("clusterBy has not been implemented yet")
    def test_cluster_by_bad_args(self) -> None:
        pass

if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)
