/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.replay

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.Meta
import io.delta.kernel.TransactionSuite.columnarBatch
import io.delta.kernel.data.{ColumnVector, FilteredColumnarBatch}
import io.delta.kernel.internal.util.{FileNames, Utils}
import io.delta.kernel.test.{MockFileSystemClientUtils, VectorTestUtils}
import io.delta.kernel.types.{LongType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class PaginatedScanFilesIteratorSuite extends AnyFunSuite
    with MockFileSystemClientUtils
    with VectorTestUtils {

  private val testSchema = new StructType().add("id", LongType.LONG)

  private def filteredBatch(filePath: String, values: Seq[Long]): FilteredColumnarBatch = {
    new FilteredColumnarBatch(
      columnarBatch(testSchema, Seq(longVector(values.map(java.lang.Long.valueOf)))),
      Optional.empty[ColumnVector](),
      filePath,
      values.size)
  }

  test("resume from staged commit file after it is published") {
    val stagedCommitPath = FileNames.stagedCommitFile(logPath, 1)
    val publishedCommitPath = FileNames.deltaFile(logPath, 1)
    val predicateHash = 123
    val logSegmentHash = 456
    val pageToken = new PageToken(
      stagedCommitPath,
      2,
      Optional.empty(),
      Meta.KERNEL_VERSION,
      dataPath.toString,
      1,
      predicateHash,
      logSegmentHash)
    val paginationContext = PaginationContext.forPageWithPageToken(
      dataPath.toString,
      1,
      logSegmentHash,
      predicateHash,
      5,
      pageToken)
    val iterator = new PaginatedScanFilesIteratorImpl(
      Utils.toCloseableIterator(
        Seq(
          filteredBatch(publishedCommitPath, Seq(0, 1, 2)),
          filteredBatch(publishedCommitPath, Seq(3, 4, 5, 6, 7))).asJava.iterator()),
      paginationContext)

    try {
      assert(iterator.hasNext)
      val batch = iterator.next()
      assert(batch.getData.getSize === 5)

      val nextPageToken = PageToken.fromRow(iterator.getCurrentPageToken.get)
      assert(nextPageToken.getLastReadLogFilePath === publishedCommitPath)
      assert(nextPageToken.getLastReturnedRowIndex === 7)
    } finally {
      iterator.close()
    }
  }
}
