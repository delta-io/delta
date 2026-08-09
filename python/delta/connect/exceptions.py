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


_delta_exception_registered = False


# Server-side Delta exception class name -> Python Delta exception class. We register only the
# io.delta.exceptions.* names because those are the public API; org.apache.spark.sql.delta.* are
# their internal Scala superclasses (returned in the class hierarchy at runtime) and are not part
# of the contract user code catches. See _register_exception_class_mappings for how this is used.
_DELTA_EXCEPTION_CLASS_MAPPING = {
    "io.delta.exceptions.DeltaConcurrentModificationException":
        DeltaConcurrentModificationException,
    "io.delta.exceptions.ConcurrentWriteException": ConcurrentWriteException,
    "io.delta.exceptions.MetadataChangedException": MetadataChangedException,
    "io.delta.exceptions.ProtocolChangedException": ProtocolChangedException,
    "io.delta.exceptions.ConcurrentAppendException": ConcurrentAppendException,
    "io.delta.exceptions.ConcurrentDeleteReadException": ConcurrentDeleteReadException,
    "io.delta.exceptions.ConcurrentDeleteDeleteException": ConcurrentDeleteDeleteException,
    "io.delta.exceptions.ConcurrentTransactionException": ConcurrentTransactionException,
}


def _register_exception_class_mappings() -> None:
    """
    Register the Delta-specific exception classes in PySpark's Spark Connect error conversion.

    PySpark converts a server error by looking up the server-side exception class names in
    EXCEPTION_CLASS_MAPPING. Registering the Delta classes there makes Delta commit-conflict
    exceptions surface as these classes instead of a generic SparkConnectGrpcException, and lets
    PySpark's generic conversion attach the structured error metadata (error class, SQL state,
    server-side stacktrace, message parameters, query contexts) the same way it does for its own
    exceptions.

    The lookup iterates the server-sent class hierarchy (ordered most-derived first) and returns
    on the first name found in EXCEPTION_CLASS_MAPPING. We register only io.delta.exceptions.*
    names - both the concrete exceptions and their base DeltaConcurrentModificationException - so
    the most-derived registered class wins over the base. For example a ConcurrentAppendException
    arrives as

        ["io.delta.exceptions.ConcurrentAppendException",              # <- picked (registered)
         "org.apache.spark.sql.delta.ConcurrentAppendException",       # <- skipped (legacy alias,
                                                                       #    not registered)
         "io.delta.exceptions.DeltaConcurrentModificationException",   # <- would match, but the
                                                                       #    earlier hit wins
         ...]

    and maps to the concrete ConcurrentAppendException, not the base.

    The org.apache.spark.sql.delta.* entries are internal Scala superclasses of the
    io.delta.exceptions.* classes; they appear in the hierarchy at runtime but are not part of the
    user-facing catch contract, so we deliberately don't register them.

    This mirrors, for Spark Connect, the conversion that delta.exceptions.captured installs for
    the classic (py4j) client.
    """
    pyspark.errors.exceptions.connect.EXCEPTION_CLASS_MAPPING.update(
        _DELTA_EXCEPTION_CLASS_MAPPING)


if not _delta_exception_registered:
    _register_exception_class_mappings()
    _delta_exception_registered = True
