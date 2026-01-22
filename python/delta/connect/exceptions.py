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
from typing import TYPE_CHECKING

from pyspark.errors.exceptions.connect import SparkConnectException

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


class DeltaConcurrentModificationException(SparkConnectException, BaseDeltaConcurrentModificationException):
    """
    The basic class for all Delta commit conflict exceptions.

    .. versionadded:: 4.0

    .. note:: Evolving
    """


class ConcurrentWriteException(SparkConnectException, BaseConcurrentWriteException):
    """
    Thrown when a concurrent transaction has written data after the current transaction read the
    table.

    .. versionadded:: 4.0

    .. note:: Evolving
    """


class MetadataChangedException(SparkConnectException, BaseMetadataChangedException):
    """
    Thrown when the metadata of the Delta table has changed between the time of read
    and the time of commit.

    .. versionadded:: 4.0

    .. note:: Evolving
    """


class ProtocolChangedException(SparkConnectException, BaseProtocolChangedException):
    """
    Thrown when the protocol version has changed between the time of read
    and the time of commit.

    .. versionadded:: 4.0

    .. note:: Evolving
    """


class ConcurrentAppendException(SparkConnectException, BaseConcurrentAppendException):
    """
    Thrown when files are added that would have been read by the current transaction.

    .. versionadded:: 4.0

    .. note:: Evolving
    """


class ConcurrentDeleteReadException(SparkConnectException, BaseConcurrentDeleteReadException):
    """
    Thrown when the current transaction reads data that was deleted by a concurrent transaction.

    .. versionadded:: 4.0

    .. note:: Evolving
    """


class ConcurrentDeleteDeleteException(SparkConnectException, BaseConcurrentDeleteDeleteException):
    """
    Thrown when the current transaction deletes data that was deleted by a concurrent transaction.

    .. versionadded:: 4.0

    .. note:: Evolving
    """


class ConcurrentTransactionException(SparkConnectException, BaseConcurrentTransactionException):
    """
    Thrown when concurrent transaction both attempt to update the same idempotent transaction.

    .. versionadded:: 4.0

    .. note:: Evolving
    """


def _convert_delta_exception(info: "ErrorInfo", message: str):
    classes = []
    if "classes" in info.metadata:
        classes = json.loads(info.metadata["classes"])

    if "io.delta.exceptions.ConcurrentWriteException" in classes:
        return ConcurrentWriteException(message)
    if "io.delta.exceptions.MetadataChangedException" in classes:
        return MetadataChangedException(message)
    if "io.delta.exceptions.ProtocolChangedException" in classes:
        return ProtocolChangedException(message)
    if "io.delta.exceptions.ConcurrentAppendException" in classes:
        return ConcurrentAppendException(message)
    if "io.delta.exceptions.ConcurrentDeleteReadException" in classes:
        return ConcurrentDeleteReadException(message)
    if "io.delta.exceptions.ConcurrentDeleteDeleteException" in classes:
        return ConcurrentDeleteDeleteException(message)
    if "io.delta.exceptions.ConcurrentTransactionException" in classes:
        return ConcurrentTransactionException(message)
    if "io.delta.exceptions.DeltaConcurrentModificationException" in classes:
        return DeltaConcurrentModificationException(message)
    return None
