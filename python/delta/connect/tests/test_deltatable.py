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
    @unittest.skip("delete has not been implemented yet")
    def test_delete(self):
        pass

    @unittest.skip("generate has not been implemented yet")
    def test_generate(self):
        pass

    @unittest.skip("update has not been implemented yet")
    def test_update(self):
        pass

    @unittest.skip("merge has not been implemented yet")
    def test_merge(self):
        pass

    @unittest.skip("merge has not been implemented yet")
    def test_merge_with_inconsistent_sessions(self):
        pass

    @unittest.skip("history has not been implemented yet")
    def test_history(self):
        pass

    @unittest.skip("detail has not been implemented yet")
    def test_detail(self):
        pass

    @unittest.skip("vacuum has not been implemented yet")
    def test_vacuum(self):
        pass

    @unittest.skip("convertToDelta has not been implemented yet")
    def test_convertToDelta(self):
        pass

    @unittest.skip("isDeltaTable has not been implemented yet")
    def test_isDeltaTable(self):
        pass

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

    @unittest.skip("upgradeToProtocol has not been implemented yet")
    def test_protocolUpgrade(self):
        pass

    @unittest.skip("restoreToVersion has not been implemented yet")
    def test_restore_to_version(self):
        pass

    @unittest.skip("restoreToTimestamp has not been implemented yet")
    def test_restore_to_timestamp(self):
        pass

    @unittest.skip("restore has not been implemented yet")
    def test_restore_invalid_inputs(self):
        pass

    @unittest.skip("optimize has not been implemented yet")
    def test_optimize(self):
        pass

    @unittest.skip("optimize has not been implemented yet")
    def test_optimize_w_partition_filter(self):
        pass

    @unittest.skip("optimize has not been implemented yet")
    def test_optimize_zorder_by(self):
        pass

    @unittest.skip("optimize has not been implemented yet")
    def test_optimize_zorder_by_w_partition_filter(self):
        pass


if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)
