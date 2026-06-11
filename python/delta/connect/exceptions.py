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

import pyspark.errors.exceptions.connect
from pyspark.errors.exceptions.connect import SparkConnectGrpcException

from delta.exceptions.base import (
    DeltaConcurrentModificationException as BaseDeltaConcurrentModificationException,
    ConcurrentWriteException as BaseConcurrentWriteException,
    MetadataChangedException as BaseMetadataChangedException,
    ProtocolChangedException as BaseProtocolChangedException,
    ConcurrentAppendException as BaseConcurrentAppendException,
    ConcurrentDeleteReadException as BaseConcurrentDeleteReadException,
    ConcurrentDeleteDeleteException as BaseConcurrentDeleteDeleteException,
    ConcurrentTransactionException as BaseConcurrentTransactionException,
)


class DeltaConcurrentModificationException(
    SparkConnectGrpcException, BaseDeltaConcurrentModificationException
):
    """
    The basic class for all Delta commit conflict exceptions.

    .. versionadded:: 4.0

    .. note:: Evolving
    """


class ConcurrentWriteException(SparkConnectGrpcException, BaseConcurrentWriteException):
    """
    Thrown when a concurrent transaction has written data after the current transaction read the
    table.

    .. versionadded:: 4.0

    .. note:: Evolving
    """


class MetadataChangedException(SparkConnectGrpcException, BaseMetadataChangedException):
    """
    Thrown when the metadata of the Delta table has changed between the time of read
    and the time of commit.

    .. versionadded:: 4.0

    .. note:: Evolving
    """


class ProtocolChangedException(SparkConnectGrpcException, BaseProtocolChangedException):
    """
    Thrown when the protocol version has changed between the time of read
    and the time of commit.

    .. versionadded:: 4.0

    .. note:: Evolving
    """


class ConcurrentAppendException(SparkConnectGrpcException, BaseConcurrentAppendException):
    """
    Thrown when files are added that would have been read by the current transaction.

    .. versionadded:: 4.0

    .. note:: Evolving
    """


class ConcurrentDeleteReadException(SparkConnectGrpcException, BaseConcurrentDeleteReadException):
    """
    Thrown when the current transaction reads data that was deleted by a concurrent transaction.

    .. versionadded:: 4.0

    .. note:: Evolving
    """


class ConcurrentDeleteDeleteException(
    SparkConnectGrpcException, BaseConcurrentDeleteDeleteException
):
    """
    Thrown when the current transaction deletes data that was deleted by a concurrent transaction.

    .. versionadded:: 4.0

    .. note:: Evolving
    """


class ConcurrentTransactionException(SparkConnectGrpcException, BaseConcurrentTransactionException):
    """
    Thrown when concurrent transaction both attempt to update the same idempotent transaction.

    .. versionadded:: 4.0

    .. note:: Evolving
    """


_delta_exceptions_registered = False


def _register_exception_class_mappings() -> None:
    """
    Register the Delta-specific exception classes in PySpark's Spark Connect error conversion
    (EXCEPTION_CLASS_MAPPING maps server-side exception class names to Python exception
    classes), so that Delta concurrent-modification exceptions raised by the server surface as
    these classes instead of a generic SparkConnectGrpcException. The generic conversion
    attaches the structured error metadata (error class, SQL state, server-side stacktrace,
    message parameters, query contexts) to the registered classes the same way as to PySpark's
    own exceptions. The conversion walks the server-sent class hierarchy from the most derived
    class, so the most specific registered class wins.

    This mirrors, for Spark Connect, the conversion patch that delta.exceptions.captured
    installs for the classic (py4j) client.
    """
    pyspark.errors.exceptions.connect.EXCEPTION_CLASS_MAPPING.update(
        {
            "io.delta.exceptions.DeltaConcurrentModificationException":
                DeltaConcurrentModificationException,
            "io.delta.exceptions.ConcurrentWriteException": ConcurrentWriteException,
            "io.delta.exceptions.MetadataChangedException": MetadataChangedException,
            "io.delta.exceptions.ProtocolChangedException": ProtocolChangedException,
            "io.delta.exceptions.ConcurrentAppendException": ConcurrentAppendException,
            "io.delta.exceptions.ConcurrentDeleteReadException":
                ConcurrentDeleteReadException,
            "io.delta.exceptions.ConcurrentDeleteDeleteException":
                ConcurrentDeleteDeleteException,
            "io.delta.exceptions.ConcurrentTransactionException":
                ConcurrentTransactionException,
        }
    )


if not _delta_exceptions_registered:
    _register_exception_class_mappings()
    _delta_exceptions_registered = True
