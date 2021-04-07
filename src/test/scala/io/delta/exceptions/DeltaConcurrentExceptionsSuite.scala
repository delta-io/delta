/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.exceptions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession

class DeltaConcurrentExceptionsSuite extends SparkFunSuite with SharedSparkSession {

  test("test ConcurrentWriteException") {
    intercept[org.apache.spark.sql.delta.DeltaConcurrentModificationException] {
      throw org.apache.spark.sql.delta.DeltaErrors.concurrentWriteException(None)
    }

    intercept[io.delta.exceptions.DeltaConcurrentModificationException] {
      throw org.apache.spark.sql.delta.DeltaErrors.concurrentWriteException(None)
    }

    intercept[org.apache.spark.sql.delta.ConcurrentWriteException] {
      throw org.apache.spark.sql.delta.DeltaErrors.concurrentWriteException(None)
    }

    intercept[org.apache.spark.sql.delta.DeltaConcurrentModificationException] {
      throw new org.apache.spark.sql.delta.ConcurrentWriteException(None)
    }

    intercept[io.delta.exceptions.DeltaConcurrentModificationException] {
      throw new org.apache.spark.sql.delta.ConcurrentWriteException(None)
    }
  }

  test("test MetadataChangedException") {
    intercept[org.apache.spark.sql.delta.DeltaConcurrentModificationException] {
      throw org.apache.spark.sql.delta.DeltaErrors.metadataChangedException(None)
    }

    intercept[io.delta.exceptions.DeltaConcurrentModificationException] {
      throw org.apache.spark.sql.delta.DeltaErrors.metadataChangedException(None)
    }

    intercept[org.apache.spark.sql.delta.MetadataChangedException] {
      throw org.apache.spark.sql.delta.DeltaErrors.metadataChangedException(None)
    }

    intercept[org.apache.spark.sql.delta.DeltaConcurrentModificationException] {
      throw new org.apache.spark.sql.delta.MetadataChangedException(None)
    }

    intercept[io.delta.exceptions.DeltaConcurrentModificationException] {
      throw new org.apache.spark.sql.delta.MetadataChangedException(None)
    }
  }

  test("test ProtocolChangedException") {
    intercept[org.apache.spark.sql.delta.DeltaConcurrentModificationException] {
      throw org.apache.spark.sql.delta.DeltaErrors.protocolChangedException(None)
    }

    intercept[io.delta.exceptions.DeltaConcurrentModificationException] {
      throw org.apache.spark.sql.delta.DeltaErrors.protocolChangedException(None)
    }

    intercept[org.apache.spark.sql.delta.ProtocolChangedException] {
      throw org.apache.spark.sql.delta.DeltaErrors.protocolChangedException(None)
    }

    intercept[org.apache.spark.sql.delta.DeltaConcurrentModificationException] {
      throw new org.apache.spark.sql.delta.ProtocolChangedException(None)
    }

    intercept[io.delta.exceptions.DeltaConcurrentModificationException] {
      throw new org.apache.spark.sql.delta.ProtocolChangedException(None)
    }
  }

  test("test ConcurrentAppendException") {
    intercept[org.apache.spark.sql.delta.DeltaConcurrentModificationException] {
      throw org.apache.spark.sql.delta.DeltaErrors.concurrentAppendException(None, "")
    }

    intercept[io.delta.exceptions.DeltaConcurrentModificationException] {
      throw org.apache.spark.sql.delta.DeltaErrors.concurrentAppendException(None, "")
    }

    intercept[org.apache.spark.sql.delta.ConcurrentAppendException] {
      throw org.apache.spark.sql.delta.DeltaErrors.concurrentAppendException(None, "")
    }

    intercept[org.apache.spark.sql.delta.DeltaConcurrentModificationException] {
      throw new org.apache.spark.sql.delta.ConcurrentAppendException(None, "")
    }

    intercept[io.delta.exceptions.DeltaConcurrentModificationException] {
      throw new org.apache.spark.sql.delta.ConcurrentAppendException(None, "")
    }
  }

  test("test ConcurrentDeleteReadException") {
    intercept[org.apache.spark.sql.delta.DeltaConcurrentModificationException] {
      throw org.apache.spark.sql.delta.DeltaErrors.
        concurrentDeleteReadException(None, "")
    }

    intercept[io.delta.exceptions.DeltaConcurrentModificationException] {
      throw org.apache.spark.sql.delta.DeltaErrors
        .concurrentDeleteReadException(None, "")
    }

    intercept[org.apache.spark.sql.delta.ConcurrentDeleteReadException] {
      throw org.apache.spark.sql.delta.DeltaErrors
        .concurrentDeleteReadException(None, "")
    }

    intercept[org.apache.spark.sql.delta.DeltaConcurrentModificationException] {
      throw new org.apache.spark.sql.delta.ConcurrentDeleteReadException(None, "")
    }

    intercept[io.delta.exceptions.DeltaConcurrentModificationException] {
      throw new org.apache.spark.sql.delta.ConcurrentDeleteReadException(None, "")
    }
  }

  test("test ConcurrentDeleteDeleteException") {
    intercept[org.apache.spark.sql.delta.DeltaConcurrentModificationException] {
      throw org.apache.spark.sql.delta.DeltaErrors
        .concurrentDeleteDeleteException(None, "")
    }

    intercept[io.delta.exceptions.DeltaConcurrentModificationException] {
      throw org.apache.spark.sql.delta.DeltaErrors
        .concurrentDeleteDeleteException(None, "")
    }

    intercept[org.apache.spark.sql.delta.ConcurrentDeleteDeleteException] {
      throw org.apache.spark.sql.delta.DeltaErrors
        .concurrentDeleteDeleteException(None, "")
    }

    intercept[org.apache.spark.sql.delta.DeltaConcurrentModificationException] {
      throw new org.apache.spark.sql.delta.ConcurrentDeleteDeleteException(None, "")
    }

    intercept[io.delta.exceptions.DeltaConcurrentModificationException] {
      throw new org.apache.spark.sql.delta.ConcurrentDeleteDeleteException(None, "")
    }
  }

  test("test ConcurrentTransactionException") {
    intercept[org.apache.spark.sql.delta.DeltaConcurrentModificationException] {
      throw org.apache.spark.sql.delta.DeltaErrors.concurrentTransactionException(None)
    }

    intercept[io.delta.exceptions.DeltaConcurrentModificationException] {
      throw org.apache.spark.sql.delta.DeltaErrors.concurrentTransactionException(None)
    }

    intercept[org.apache.spark.sql.delta.ConcurrentTransactionException] {
      throw org.apache.spark.sql.delta.DeltaErrors.concurrentTransactionException(None)
    }

    intercept[org.apache.spark.sql.delta.DeltaConcurrentModificationException] {
      throw new org.apache.spark.sql.delta.ConcurrentTransactionException(None)
    }

    intercept[io.delta.exceptions.DeltaConcurrentModificationException] {
      throw new org.apache.spark.sql.delta.ConcurrentTransactionException(None)
    }
  }
}
