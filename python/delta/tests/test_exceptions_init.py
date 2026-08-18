#
# Copyright (2021) The Delta Lake Project Authors.
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

from pyspark.util import is_remote_only

# Import delta, not delta.exceptions / delta.exceptions.captured - run-tests.py runs this file in
# its own process, and captured is imported only via the import delta -> tables -> delta.exceptions
# chain, so this verifies that wiring installs the conversion.
import delta  # noqa: F401


class DeltaClassicExceptionConversionInstalledTests(unittest.TestCase):
    @unittest.skipIf(is_remote_only(), "classic conversion needs py4j/SparkContext")
    def test_importing_delta_installs_conversion(self) -> None:
        # Inspect sys.modules rather than importing delta.exceptions.captured ourselves (which would
        # set the flag regardless), so a missing import in the chain is caught.
        module = sys.modules.get("delta.exceptions.captured")
        assert module is not None, "import delta did not import delta.exceptions.captured"
        self.assertTrue(module._delta_exception_patched)

    @unittest.skipIf(is_remote_only(), "classic conversion needs py4j/SparkContext")
    def test_convert_exception_is_patched(self) -> None:
        # delta replaces pyspark's classic convert_exception with its own wrapper; assert the
        # installed function comes from delta (see delta/exceptions/captured.py).
        import pyspark.errors.exceptions.captured as pyspark_captured
        self.assertEqual(
            pyspark_captured.convert_exception.__module__, "delta.exceptions.captured")


if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)
