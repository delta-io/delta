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

# Importing delta.exceptions.captured installs the conversion that makes the classic (py4j)
# PySpark client raise the Delta-specific exceptions exported below. In a Spark Connect-only
# install (pyspark-connect) there is no py4j and pyspark does not export SparkContext, so
# captured's "from pyspark import SparkContext" raises ImportError; guard it so that importing
# delta still works there. On that path the equivalent conversion is registered instead by
# delta.connect.exceptions.
try:
    import delta.exceptions.captured  # noqa: F401
except ImportError:
    pass

from delta.exceptions.base import (
    DeltaConcurrentModificationException,
    ConcurrentWriteException,
    MetadataChangedException,
    ProtocolChangedException,
    ConcurrentAppendException,
    ConcurrentDeleteReadException,
    ConcurrentDeleteDeleteException,
    ConcurrentTransactionException,
)

__all__ = [
    "DeltaConcurrentModificationException",
    "ConcurrentWriteException",
    "MetadataChangedException",
    "ProtocolChangedException",
    "ConcurrentAppendException",
    "ConcurrentDeleteReadException",
    "ConcurrentDeleteDeleteException",
    "ConcurrentTransactionException",
]
