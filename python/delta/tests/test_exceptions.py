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

from typing import Any, Callable, TYPE_CHECKING
import unittest

import delta.exceptions as exceptions

from delta.testing.utils import DeltaTestCase
from pyspark.sql.utils import AnalysisException, IllegalArgumentException

if TYPE_CHECKING:
    from py4j.java_gateway import JVMView  # type: ignore[import]


class DeltaExceptionTests(DeltaTestCase):

    def setUp(self) -> None:
        super(DeltaExceptionTests, self).setUp()
        self.jvm: "JVMView" = self.spark.sparkContext._jvm  # type: ignore[attr-defined]

    def _raise_concurrent_exception(self, exception_type: Callable[[Any], Any]) -> None:
        e = exception_type("")
        self.jvm.scala.util.Failure(e).get()

    def test_capture_concurrent_write_exception(self) -> None:
        e = self.jvm.io.delta.exceptions.ConcurrentWriteException
        self.assertRaises(exceptions.ConcurrentWriteException,
                          lambda: self._raise_concurrent_exception(e))

    def test_capture_metadata_changed_exception(self) -> None:
        e = self.jvm.io.delta.exceptions.MetadataChangedException
        self.assertRaises(exceptions.MetadataChangedException,
                          lambda: self._raise_concurrent_exception(e))

    def test_capture_protocol_changed_exception(self) -> None:
        e = self.jvm.io.delta.exceptions.ProtocolChangedException
        self.assertRaises(exceptions.ProtocolChangedException,
                          lambda: self._raise_concurrent_exception(e))

    def test_capture_concurrent_append_exception(self) -> None:
        e = self.jvm.io.delta.exceptions.ConcurrentAppendException
        self.assertRaises(exceptions.ConcurrentAppendException,
                          lambda: self._raise_concurrent_exception(e))

    def test_capture_concurrent_delete_read_exception(self) -> None:
        e = self.jvm.io.delta.exceptions.ConcurrentDeleteReadException
        self.assertRaises(exceptions.ConcurrentDeleteReadException,
                          lambda: self._raise_concurrent_exception(e))

    def test_capture_concurrent_delete_delete_exception(self) -> None:
        e = self.jvm.io.delta.exceptions.ConcurrentDeleteDeleteException
        self.assertRaises(exceptions.ConcurrentDeleteDeleteException,
                          lambda: self._raise_concurrent_exception(e))

    def test_capture_concurrent_transaction_exception(self) -> None:
        e = self.jvm.io.delta.exceptions.ConcurrentTransactionException
        self.assertRaises(exceptions.ConcurrentTransactionException,
                          lambda: self._raise_concurrent_exception(e))

    def test_capture_delta_analysis_exception(self) -> None:
        e = self.jvm.org.apache.spark.sql.delta.DeltaErrors.invalidColumnName
        self.assertRaises(AnalysisException,
                          lambda: self.jvm.scala.util.Failure(e("invalid")).get())

    def test_capture_delta_illegal_argument_exception(self) -> None:
        e = self.jvm.org.apache.spark.sql.delta.DeltaErrors
        method = e.throwDeltaIllegalArgumentException
        self.assertRaises(IllegalArgumentException,
                          lambda: self.jvm.scala.util.Failure(method()).get())


if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)
