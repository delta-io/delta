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

import os
import unittest
from packaging.version import Version


class VersionAPITests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.project_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', '..', '..')
        )
        cls.version_sbt_path = os.path.join(cls.project_root, 'version.sbt')

    def verify_version(self, version):
        self.assertIsNotNone(version)
        self.assertIsInstance(version, str)
        self.assertEqual(version.count("."), 2, "Version should have major.minor.patch format")
        # version should be parseable by packaging.version.Version
        Version(version)

    def test_version_import_from_module(self):
        """Test that __version__ can be imported from delta.version"""
        from delta.version import __version__
        self.verify_version(__version__)

    def test_version_import_from_package(self):
        """Test that __version__ can be imported from delta package"""
        from delta import __version__
        self.verify_version(__version__)

    def test_version_consistency_across_imports(self):
        """Test that version is consistent across import methods"""
        from delta.version import __version__ as version_from_module
        from delta import __version__ as version_from_package

        self.assertEqual(version_from_module, version_from_package)

    def test_version_sbt_exists(self):
        """Verify version.sbt exists"""
        self.assertTrue(
            os.path.exists(self.version_sbt_path),
            f"version.sbt not found at {self.version_sbt_path}"
        )

    def test_version_sbt_and_version_py_consistency(self):
        with open(self.version_sbt_path) as f:
            sbt_content = f.read()
            # Extract version from: ThisBuild / version := "x.y.z-SNAPSHOT" -> "x.y.z"
            sbt_version = sbt_content.split('"')[1].removesuffix("-SNAPSHOT")

        from delta import __version__

        self.assertEqual(
            __version__,
            sbt_version,
            f"version.py ({__version__}) does not match version.sbt ({sbt_version}). "
            f"Run: build/sbt sparkV1/generatePythonVersion"
        )


if __name__ == '__main__':
    unittest.main()
