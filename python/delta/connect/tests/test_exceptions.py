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

import json
import unittest

from google.rpc.error_details_pb2 import ErrorInfo

# Imported for its side effect: registers the exception class mappings under test.
import delta.connect.exceptions  # noqa: F401
import delta.exceptions
from delta.connect.exceptions import ConcurrentAppendException

import pyspark.sql.connect.client.core
import pyspark.sql.connect.proto as pb2
from pyspark.errors.exceptions.connect import AnalysisException


class DeltaConnectExceptionConversionTests(unittest.TestCase):
    """
    Tests for the exception class mappings registered by delta.connect.exceptions in PySpark's
    Spark Connect error conversion. These tests exercise the conversion directly from
    constructed gRPC error payloads and do not need a Spark Connect server.
    """

    def _convert(self, info, truncated_message, resp=None):
        # Conversion goes through the binding used by SparkConnectClient.
        return pyspark.sql.connect.client.core.convert_exception(info, truncated_message, resp)

    def test_delta_exception_from_error_info(self):
        info = ErrorInfo()
        info.reason = "io.delta.exceptions.ConcurrentAppendException"
        info.metadata["classes"] = json.dumps([
            "io.delta.exceptions.ConcurrentAppendException",
            "io.delta.exceptions.DeltaConcurrentModificationException",
        ])
        info.metadata["errorClass"] = "DELTA_CONCURRENT_APPEND"
        info.metadata["sqlState"] = "2D521"
        info.metadata["messageParameters"] = json.dumps({"partition": "the root of the table"})

        exception = self._convert(info, "Files were added by a concurrent update.")

        self.assertIsInstance(exception, ConcurrentAppendException)
        # User code catching the documented delta.exceptions types must keep working.
        self.assertIsInstance(exception, delta.exceptions.ConcurrentAppendException)
        self.assertEqual(exception.getCondition(), "DELTA_CONCURRENT_APPEND")
        self.assertEqual(exception.getSqlState(), "2D521")
        self.assertEqual(
            exception.getMessageParameters(), {"partition": "the root of the table"})
        # The "(<reason>) " prefix added by the generic conversion must not leak in.
        self.assertEqual(exception.getMessage(), "Files were added by a concurrent update.")

    def test_delta_exception_with_fetch_error_details(self):
        # With an enriched error (FetchErrorDetailsResponse), the full message, the server-side
        # stacktrace and the message parameters come from the response and must be preserved on
        # the Delta-specific exception.
        resp = pb2.FetchErrorDetailsResponse()
        resp.root_error_idx = 0
        error = resp.errors.add()
        error.message = "Files were added by a concurrent update."
        error.error_type_hierarchy.append("io.delta.exceptions.ConcurrentAppendException")
        stack_frame = error.stack_trace.add()
        stack_frame.declaring_class = "org.apache.spark.sql.delta.ConflictChecker"
        stack_frame.method_name = "checkConflicts"
        stack_frame.file_name = "ConflictChecker.scala"
        stack_frame.line_number = 42
        error.spark_throwable.error_class = "DELTA_CONCURRENT_APPEND"
        error.spark_throwable.message_parameters["partition"] = "the root of the table"

        info = ErrorInfo()
        info.reason = "io.delta.exceptions.ConcurrentAppendException"
        info.metadata["classes"] = json.dumps(["io.delta.exceptions.ConcurrentAppendException"])
        info.metadata["errorClass"] = "DELTA_CONCURRENT_APPEND"

        exception = self._convert(info, "Truncated message", resp)

        self.assertIsInstance(exception, ConcurrentAppendException)
        self.assertEqual(exception.getCondition(), "DELTA_CONCURRENT_APPEND")
        self.assertEqual(
            exception.getMessageParameters(), {"partition": "the root of the table"})
        self.assertIn("ConflictChecker", exception.getStackTrace())
        self.assertEqual(exception.getMessage(), "Files were added by a concurrent update.")

    def test_non_delta_exception_is_unaffected(self):
        info = ErrorInfo()
        info.reason = "org.apache.spark.sql.AnalysisException"
        info.metadata["classes"] = json.dumps(["org.apache.spark.sql.AnalysisException"])
        info.metadata["errorClass"] = "TABLE_OR_VIEW_NOT_FOUND"
        info.metadata["sqlState"] = "42P01"
        info.metadata["messageParameters"] = json.dumps({"relationName": "`t`"})

        exception = self._convert(
            info, "[TABLE_OR_VIEW_NOT_FOUND] The table or view `t` cannot be found.")

        self.assertIsInstance(exception, AnalysisException)
        self.assertNotIsInstance(
            exception, delta.exceptions.DeltaConcurrentModificationException)
        self.assertEqual(exception.getCondition(), "TABLE_OR_VIEW_NOT_FOUND")


if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)
