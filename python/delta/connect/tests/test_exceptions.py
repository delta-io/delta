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

from pyspark.errors.exceptions.connect import SparkConnectGrpcException

from delta.connect.exceptions import (
    _convert_delta_exception,
    ConcurrentWriteException,
    MetadataChangedException,
    ProtocolChangedException,
    ConcurrentAppendException,
    ConcurrentDeleteReadException,
    ConcurrentDeleteDeleteException,
    ConcurrentTransactionException,
    DeltaConcurrentModificationException,
)
from delta.exceptions.base import (
    ConcurrentWriteException as BaseConcurrentWriteException,
    MetadataChangedException as BaseMetadataChangedException,
    ProtocolChangedException as BaseProtocolChangedException,
    ConcurrentAppendException as BaseConcurrentAppendException,
    ConcurrentDeleteReadException as BaseConcurrentDeleteReadException,
    ConcurrentDeleteDeleteException as BaseConcurrentDeleteDeleteException,
    ConcurrentTransactionException as BaseConcurrentTransactionException,
    DeltaConcurrentModificationException as BaseDeltaConcurrentModificationException,
)


class _FakeErrorInfo:
    """Minimal stub for google.rpc.error_details_pb2.ErrorInfo."""

    def __init__(self, classes=None, sql_state=None):
        self.metadata = {}
        if classes is not None:
            self.metadata["classes"] = json.dumps(classes)
        if sql_state is not None:
            self.metadata["sqlState"] = sql_state


_MESSAGE = "test error message"
_SQL_STATE = "2D521"

_CLASS_TO_EXCEPTION = [
    ("io.delta.exceptions.ConcurrentWriteException", ConcurrentWriteException, BaseConcurrentWriteException),
    ("io.delta.exceptions.MetadataChangedException", MetadataChangedException, BaseMetadataChangedException),
    ("io.delta.exceptions.ProtocolChangedException", ProtocolChangedException, BaseProtocolChangedException),
    ("io.delta.exceptions.ConcurrentAppendException", ConcurrentAppendException, BaseConcurrentAppendException),
    ("io.delta.exceptions.ConcurrentDeleteReadException", ConcurrentDeleteReadException, BaseConcurrentDeleteReadException),
    ("io.delta.exceptions.ConcurrentDeleteDeleteException", ConcurrentDeleteDeleteException, BaseConcurrentDeleteDeleteException),
    ("io.delta.exceptions.ConcurrentTransactionException", ConcurrentTransactionException, BaseConcurrentTransactionException),
    ("io.delta.exceptions.DeltaConcurrentModificationException", DeltaConcurrentModificationException, BaseDeltaConcurrentModificationException),
]


class DeltaConnectExceptionConversionTests(unittest.TestCase):

    def test_converts_to_correct_exception_type(self) -> None:
        for java_class, expected_connect_class, _ in _CLASS_TO_EXCEPTION:
            with self.subTest(java_class=java_class):
                info = _FakeErrorInfo(classes=[java_class])
                exc = _convert_delta_exception(info, _MESSAGE)
                self.assertIsNotNone(exc)
                self.assertIsInstance(exc, expected_connect_class)

    def test_sql_state_propagated_from_info(self) -> None:
        for java_class, _, _ in _CLASS_TO_EXCEPTION:
            with self.subTest(java_class=java_class):
                info = _FakeErrorInfo(classes=[java_class], sql_state=_SQL_STATE)
                exc = _convert_delta_exception(info, _MESSAGE)
                self.assertIsNotNone(exc)
                self.assertEqual(exc.getSqlState(), _SQL_STATE)  # type: ignore[union-attr]

    def test_no_sql_state_in_info_returns_none(self) -> None:
        for java_class, _, _ in _CLASS_TO_EXCEPTION:
            with self.subTest(java_class=java_class):
                info = _FakeErrorInfo(classes=[java_class])
                exc = _convert_delta_exception(info, _MESSAGE)
                self.assertIsNotNone(exc)
                self.assertIsNone(exc.getSqlState())  # type: ignore[union-attr]

    def test_unknown_class_returns_none(self) -> None:
        info = _FakeErrorInfo(classes=["io.delta.exceptions.UnknownException"])
        result = _convert_delta_exception(info, _MESSAGE)
        self.assertIsNone(result)

    def test_empty_classes_returns_none(self) -> None:
        info = _FakeErrorInfo(classes=[])
        result = _convert_delta_exception(info, _MESSAGE)
        self.assertIsNone(result)

    def test_no_classes_key_returns_none(self) -> None:
        info = _FakeErrorInfo()
        result = _convert_delta_exception(info, _MESSAGE)
        self.assertIsNone(result)

    def test_inherits_from_spark_connect_grpc_exception(self) -> None:
        for java_class, _, _ in _CLASS_TO_EXCEPTION:
            with self.subTest(java_class=java_class):
                info = _FakeErrorInfo(classes=[java_class])
                exc = _convert_delta_exception(info, _MESSAGE)
                self.assertIsInstance(exc, SparkConnectGrpcException)

    def test_inherits_from_base_delta_exception(self) -> None:
        for java_class, _, expected_base_class in _CLASS_TO_EXCEPTION:
            with self.subTest(java_class=java_class):
                info = _FakeErrorInfo(classes=[java_class])
                exc = _convert_delta_exception(info, _MESSAGE)
                self.assertIsInstance(exc, expected_base_class)


if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)
