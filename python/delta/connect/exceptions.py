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

import json
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import pyspark.errors.exceptions.connect
import pyspark.sql.connect.client.core
from pyspark.errors.exceptions.connect import SparkConnectException, SparkConnectGrpcException

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

if TYPE_CHECKING:
    from google.rpc.error_details_pb2 import ErrorInfo
    from pyspark.sql.connect.proto import FetchErrorDetailsResponse


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


def _convert_delta_exception(classes: List[str], message: str, **kwargs: Any):
    """
    Maps a server-side Delta concurrency exception to its Delta-specific Python class.

    kwargs carry the structured error metadata (errorClass, sql_state, server_stacktrace, ...)
    accepted by SparkConnectGrpcException, so that the converted exception keeps the same
    metadata as exceptions built through the generic conversion in
    pyspark.errors.exceptions.connect.
    """
    if "io.delta.exceptions.ConcurrentWriteException" in classes:
        return ConcurrentWriteException(message, **kwargs)
    if "io.delta.exceptions.MetadataChangedException" in classes:
        return MetadataChangedException(message, **kwargs)
    if "io.delta.exceptions.ProtocolChangedException" in classes:
        return ProtocolChangedException(message, **kwargs)
    if "io.delta.exceptions.ConcurrentAppendException" in classes:
        return ConcurrentAppendException(message, **kwargs)
    if "io.delta.exceptions.ConcurrentDeleteReadException" in classes:
        return ConcurrentDeleteReadException(message, **kwargs)
    if "io.delta.exceptions.ConcurrentDeleteDeleteException" in classes:
        return ConcurrentDeleteDeleteException(message, **kwargs)
    if "io.delta.exceptions.ConcurrentTransactionException" in classes:
        return ConcurrentTransactionException(message, **kwargs)
    if "io.delta.exceptions.DeltaConcurrentModificationException" in classes:
        return DeltaConcurrentModificationException(message, **kwargs)
    return None


_delta_exception_patched = False


def _patch_convert_exception() -> None:
    """
    Patch PySpark's Spark Connect error conversion so that Delta concurrent-modification
    exceptions raised by the server surface as their Delta-specific Python classes instead of
    a generic SparkConnectGrpcException, keeping the structured error metadata intact.

    This mirrors the patching delta.exceptions.captured does for the classic (py4j) client.
    """
    original_convert_exception = pyspark.errors.exceptions.connect.convert_exception

    def convert_delta_exception(
        info: "ErrorInfo",
        truncated_message: str,
        resp: Optional["FetchErrorDetailsResponse"] = None,
        *args: Any,
        **kwargs: Any,
    ) -> SparkConnectException:
        converted = original_convert_exception(info, truncated_message, resp, *args, **kwargs)
        if not isinstance(converted, SparkConnectGrpcException):
            return converted

        classes: List[str] = []
        if "classes" in info.metadata:
            classes = json.loads(info.metadata["classes"])

        # The raw server-side message, mirroring pyspark's _convert_exception. The converted
        # exception's message cannot be reused: the UnknownException branch of the generic
        # conversion prefixes it with "(<reason>) ".
        if resp is not None and resp.HasField("root_error_idx"):
            message = resp.errors[resp.root_error_idx].message
        else:
            message = truncated_message

        # Rebuild the structured error metadata that the generic conversion attached to the
        # converted exception. _display_stacktrace and _sql_state have no public accessors
        # returning the raw values (getSqlState falls back to an error-class lookup);
        # getGrpcStatusCode and the matching constructor kwarg only exist on newer PySpark
        # versions.
        metadata_kwargs: Dict[str, Any] = {
            "errorClass": converted.getCondition(),
            "messageParameters": converted.getMessageParameters(),
            "sql_state": converted._sql_state,
            "server_stacktrace": converted.getStackTrace(),
            "display_server_stacktrace": converted._display_stacktrace,
            "contexts": converted.getQueryContext(),
        }
        if hasattr(converted, "getGrpcStatusCode"):
            metadata_kwargs["grpc_status_code"] = converted.getGrpcStatusCode()

        delta_exception = _convert_delta_exception(classes, message, **metadata_kwargs)
        if delta_exception is not None:
            return delta_exception
        return converted

    pyspark.errors.exceptions.connect.convert_exception = convert_delta_exception
    # SparkConnectClient binds convert_exception by name at import time, so the binding in
    # pyspark.sql.connect.client.core must be replaced as well.
    pyspark.sql.connect.client.core.convert_exception = convert_delta_exception


if not _delta_exception_patched:
    _patch_convert_exception()
    _delta_exception_patched = True
