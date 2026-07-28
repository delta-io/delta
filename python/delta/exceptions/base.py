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

from pyspark.errors.exceptions.base import PySparkException


class DeltaConcurrentModificationException(PySparkException):
    """
    The basic class for all Delta commit conflict exceptions.

    .. versionadded:: 1.0

    .. note:: Evolving
    """


class ConcurrentWriteException(PySparkException):
    """
    Thrown when a concurrent transaction has written data after the current transaction read the
    table.

    .. versionadded:: 1.0

    .. note:: Evolving
    """


class MetadataChangedException(PySparkException):
    """
    Thrown when the metadata of the Delta table has changed between the time of read
    and the time of commit.

    .. versionadded:: 1.0

    .. note:: Evolving
    """


class ProtocolChangedException(PySparkException):
    """
    Thrown when the protocol version has changed between the time of read
    and the time of commit.

    .. versionadded:: 1.0

    .. note:: Evolving
    """


class ConcurrentAppendException(PySparkException):
    """
    Thrown when files are added that would have been read by the current transaction.

    .. versionadded:: 1.0

    .. note:: Evolving
    """


class ConcurrentDeleteReadException(PySparkException):
    """
    Thrown when the current transaction reads data that was deleted by a concurrent transaction.

    .. versionadded:: 1.0

    .. note:: Evolving
    """


class ConcurrentDeleteDeleteException(PySparkException):
    """
    Thrown when the current transaction deletes data that was deleted by a concurrent transaction.

    .. versionadded:: 1.0

    .. note:: Evolving
    """


class ConcurrentTransactionException(PySparkException):
    """
    Thrown when concurrent transaction both attempt to update the same idempotent transaction.

    .. versionadded:: 1.0

    .. note:: Evolving
    """
