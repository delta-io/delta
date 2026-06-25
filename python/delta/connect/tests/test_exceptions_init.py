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

import sys
import unittest

# Import the package, not delta.connect.exceptions - run-tests.py runs this file in its own
# process, and delta.connect.exceptions is imported only by delta/connect/__init__.py, so this
# verifies that wiring installs the registration.
import delta.connect  # noqa: F401


class DeltaConnectExceptionConversionInstalledTests(unittest.TestCase):
    def test_importing_delta_connect_installs_conversion(self):
        # Inspect sys.modules rather than importing delta.connect.exceptions ourselves (which would
        # set the flag regardless), so a missing import in delta/connect/__init__.py is caught.
        module = sys.modules.get("delta.connect.exceptions")
        self.assertIsNotNone(
            module, "import delta.connect did not import delta.connect.exceptions")
        self.assertTrue(module._delta_exception_registered)

    def test_delta_exceptions_registered_in_mapping(self):
        # Each Delta concurrency exception must be registered in PySpark's conversion mapping,
        # keyed by its server-side class name and mapped to the matching delta.connect class.
        from pyspark.errors.exceptions.connect import EXCEPTION_CLASS_MAPPING
        module = sys.modules.get("delta.connect.exceptions")
        self.assertIsNotNone(
            module, "import delta.connect did not import delta.connect.exceptions")
        names = [
            "DeltaConcurrentModificationException",
            "ConcurrentWriteException",
            "MetadataChangedException",
            "ProtocolChangedException",
            "ConcurrentAppendException",
            "ConcurrentDeleteReadException",
            "ConcurrentDeleteDeleteException",
            "ConcurrentTransactionException",
        ]
        for name in names:
            class_name = "io.delta.exceptions." + name
            self.assertIs(
                EXCEPTION_CLASS_MAPPING.get(class_name), getattr(module, name),
                "%s is not registered to the Delta %s" % (class_name, name))


if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)
