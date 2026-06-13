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
from typing import Dict, Optional, TYPE_CHECKING

import grpc
from grpc import StatusCode

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


class DeltaConcurrentModificationException(SparkConnectGrpcException, BaseDeltaConcurrentModificationException):
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


class ConcurrentDeleteDeleteException(SparkConnectGrpcException, BaseConcurrentDeleteDeleteException):
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


def _convert_delta_exception(
    info: "ErrorInfo",
    message: str,
    grpc_status_code: grpc.StatusCode = StatusCode.UNKNOWN,
    server_stacktrace: Optional[str] = None,
    display_server_stacktrace: bool = False,
) -> Optional[SparkConnectGrpcException]:
    classes = []
    if "classes" in info.metadata:
        classes = json.loads(info.metadata["classes"])

    sql_state: Optional[str] = info.metadata.get("sqlState")
    error_class: Optional[str] = info.metadata.get("errorClass")
    raw_params = info.metadata.get("messageParameters")
    message_parameters: Optional[Dict[str, str]] = json.loads(raw_params) if raw_params else None

    if "io.delta.exceptions.ConcurrentWriteException" in classes:
        return ConcurrentWriteException(
            message,
            errorClass=error_class,
            messageParameters=message_parameters,
            sql_state=sql_state,
            grpc_status_code=grpc_status_code,
            server_stacktrace=server_stacktrace,
            display_server_stacktrace=display_server_stacktrace,
        )
    if "io.delta.exceptions.MetadataChangedException" in classes:
        return MetadataChangedException(
            message,
            errorClass=error_class,
            messageParameters=message_parameters,
            sql_state=sql_state,
            grpc_status_code=grpc_status_code,
            server_stacktrace=server_stacktrace,
            display_server_stacktrace=display_server_stacktrace,
        )
    if "io.delta.exceptions.ProtocolChangedException" in classes:
        return ProtocolChangedException(
            message,
            errorClass=error_class,
            messageParameters=message_parameters,
            sql_state=sql_state,
            grpc_status_code=grpc_status_code,
            server_stacktrace=server_stacktrace,
            display_server_stacktrace=display_server_stacktrace,
        )
    if "io.delta.exceptions.ConcurrentAppendException" in classes:
        return ConcurrentAppendException(
            message,
            errorClass=error_class,
            messageParameters=message_parameters,
            sql_state=sql_state,
            grpc_status_code=grpc_status_code,
            server_stacktrace=server_stacktrace,
            display_server_stacktrace=display_server_stacktrace,
        )
    if "io.delta.exceptions.ConcurrentDeleteReadException" in classes:
        return ConcurrentDeleteReadException(
            message,
            errorClass=error_class,
            messageParameters=message_parameters,
            sql_state=sql_state,
            grpc_status_code=grpc_status_code,
            server_stacktrace=server_stacktrace,
            display_server_stacktrace=display_server_stacktrace,
        )
    if "io.delta.exceptions.ConcurrentDeleteDeleteException" in classes:
        return ConcurrentDeleteDeleteException(
            message,
            errorClass=error_class,
            messageParameters=message_parameters,
            sql_state=sql_state,
            grpc_status_code=grpc_status_code,
            server_stacktrace=server_stacktrace,
            display_server_stacktrace=display_server_stacktrace,
        )
    if "io.delta.exceptions.ConcurrentTransactionException" in classes:
        return ConcurrentTransactionException(
            message,
            errorClass=error_class,
            messageParameters=message_parameters,
            sql_state=sql_state,
            grpc_status_code=grpc_status_code,
            server_stacktrace=server_stacktrace,
            display_server_stacktrace=display_server_stacktrace,
        )
    if "io.delta.exceptions.DeltaConcurrentModificationException" in classes:
        return DeltaConcurrentModificationException(
            message,
            errorClass=error_class,
            messageParameters=message_parameters,
            sql_state=sql_state,
            grpc_status_code=grpc_status_code,
            server_stacktrace=server_stacktrace,
            display_server_stacktrace=display_server_stacktrace,
        )
    return None
