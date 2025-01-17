/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.checksum

import io.delta.kernel.data.{ArrayValue, ColumnVector, MapValue, Row}
import io.delta.kernel.internal.actions.{Format, Metadata, Protocol}
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.snapshot.SnapshotHint
import io.delta.kernel.internal.util.InternalUtils.singletonStringColumnVector
import io.delta.kernel.test.{BaseMockJsonHandler, MockEngineUtils}
import io.delta.kernel.types.{StringType, StructType}
import io.delta.kernel.utils.CloseableIterator
import org.scalatest.funsuite.AnyFunSuite

import java.util
import java.util.{Collections, HashMap, Optional, OptionalLong}

class ChecksumReadWriteSuite extends AnyFunSuite with MockEngineUtils {

  private val FAKE_DELTA_LOG_PATH = new Path("/path/to/delta/log")


  test("basic checksum write") {
    val jsonHandler = new MockCheckSumFileJsonWriter()
    val checksumWriter =
      new ChecksumWriter(mockEngine(jsonHandler = jsonHandler), FAKE_DELTA_LOG_PATH)
    val snapshotHint = new SnapshotHint(
      1,
      createTestProtocol(),
      createTestMetadata(),
      OptionalLong.of(1),
      OptionalLong.of(100))
    checksumWriter.maybeWriteCheckSum(snapshotHint, "tnx")

    assert(jsonHandler.capturedCrcRow.getLong(1) == 100L)

  }

  def createTestMetadata(): Metadata = {
    new Metadata(
      "id",
      Optional.of("name"),
      Optional.of("description"),
      new Format("parquet", Collections.emptyMap()),
      "sss",
      new StructType(),
      new ArrayValue() { // partitionColumns
        override def getSize = 1

        override def getElements: ColumnVector = singletonStringColumnVector("c3")
      },
      Optional.empty(),
      new MapValue() { // conf
        override def getSize = 1

        override def getKeys: ColumnVector = singletonStringColumnVector("delta.appendOnly")

        override def getValues: ColumnVector =
          singletonStringColumnVector("true")
      }
    )
  }

  def createTestProtocol(): Protocol = {
    new Protocol(
      /* minReaderVersion= */ 0,
      /* minWriterVersion= */ 1,
      Collections.emptyList(),
      Collections.emptyList()
    )
  }



}

class MockCheckSumFileJsonWriter extends BaseMockJsonHandler {
  var capturedCrcRow: Row = new GenericRow(new StructType(), new util.HashMap[Integer, AnyRef]);

  override def writeJsonFileAtomically(filePath: String,
                                       data: CloseableIterator[Row],
                                       overwrite: Boolean): Unit = {
    if (data.hasNext) capturedCrcRow = data.next()
  }

}
