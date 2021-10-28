/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.internal

import org.apache.hadoop.fs.{FileStatus, Path}

// scalastyle:off funsuite
import org.scalatest.FunSuite

class BufferingLogDeletionIteratorSuite extends FunSuite {
  // scalastyle:on funsuite
  /**
   * Creates FileStatus objects, where the name is the version of a commit, and the modification
   * timestamps come from the input.
   */
  private def createFileStatuses(modTimes: Long*): Iterator[FileStatus] = {
    modTimes.zipWithIndex.map { case (time, version) =>
      new FileStatus(10L, false, 1, 10L, time, new Path(version.toString))
    }.iterator
  }

  /**
   * Creates a log deletion iterator with a retention `maxTimestamp` and `maxVersion` (both
   * inclusive). The input iterator takes the original file timestamps, and the deleted output will
   * return the adjusted timestamps of files that would actually be consumed by the iterator.
   */
  private def testBufferingLogDeletionIterator(
      maxTimestamp: Long,
      maxVersion: Long)(inputTimestamps: Seq[Long], deleted: Seq[Long]): Unit = {
    val i = new BufferingLogDeletionIterator(
      createFileStatuses(inputTimestamps: _*), maxTimestamp, maxVersion, _.getName.toLong)
    deleted.foreach { ts =>
      assert(i.hasNext, s"Was supposed to delete $ts, but iterator returned hasNext: false")
      assert(i.next().getModificationTime === ts, "Returned files out of order!")
    }
    assert(!i.hasNext, "Iterator should be consumed")
  }

  test("BufferingLogDeletionIterator: iterator behavior") {
    val i1 = new BufferingLogDeletionIterator(Iterator.empty, 100, 100, _ => 1)
    intercept[NoSuchElementException](i1.next())
    assert(!i1.hasNext)

    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 100)(
      inputTimestamps = Seq(10),
      deleted = Seq(10)
    )

    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 100)(
      inputTimestamps = Seq(10, 15, 25),
      deleted = Seq(10, 15, 25)
    )
  }

  test("BufferingLogDeletionIterator: " +
    "early exit while handling adjusted timestamps due to timestamp") {
    // only should return 5 because 5 < 7
    testBufferingLogDeletionIterator(maxTimestamp = 7, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8, 12),
      deleted = Seq(5)
    )

    // Should only return 5, because 10 is used to adjust the following 8 to 11
    testBufferingLogDeletionIterator(maxTimestamp = 10, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8, 12),
      deleted = Seq(5)
    )

    // When it is 11, we can delete both 10 and 8
    testBufferingLogDeletionIterator(maxTimestamp = 11, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8, 12),
      deleted = Seq(5, 10, 11)
    )

    // When it is 12, we can return all
    testBufferingLogDeletionIterator(maxTimestamp = 12, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8, 12),
      deleted = Seq(5, 10, 11, 12)
    )

    // Should only return 5, because 10 is used to adjust the following 8 to 11
    testBufferingLogDeletionIterator(maxTimestamp = 10, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8),
      deleted = Seq(5)
    )

    // When it is 11, we can delete both 10 and 8
    testBufferingLogDeletionIterator(maxTimestamp = 11, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8),
      deleted = Seq(5, 10, 11)
    )
  }

  test("BufferingLogDeletionIterator: " +
    "early exit while handling adjusted timestamps due to version") {
    // only should return 5 because we can delete only up to version 0
    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 0)(
      inputTimestamps = Seq(5, 10, 8, 12),
      deleted = Seq(5)
    )

    // Should only return 5, because 10 is used to adjust the following 8 to 11
    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 1)(
      inputTimestamps = Seq(5, 10, 8, 12),
      deleted = Seq(5)
    )

    // When we can delete up to version 2, we can return up to version 2
    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 2)(
      inputTimestamps = Seq(5, 10, 8, 12),
      deleted = Seq(5, 10, 11)
    )

    // When it is version 3, we can return all
    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 3)(
      inputTimestamps = Seq(5, 10, 8, 12),
      deleted = Seq(5, 10, 11, 12)
    )

    // Should only return 5, because 10 is used to adjust the following 8 to 11
    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 1)(
      inputTimestamps = Seq(5, 10, 8),
      deleted = Seq(5)
    )

    // When we can delete up to version 2, we can return up to version 2
    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 2)(
      inputTimestamps = Seq(5, 10, 8),
      deleted = Seq(5, 10, 11)
    )
  }

  test("BufferingLogDeletionIterator: multiple adjusted timestamps") {
    Seq(9, 10, 11).foreach { retentionTimestamp =>
      // Files should be buffered but not deleted, because of the file 11, which has adjusted ts 12
      testBufferingLogDeletionIterator(maxTimestamp = retentionTimestamp, maxVersion = 100)(
        inputTimestamps = Seq(5, 10, 8, 11, 14),
        deleted = Seq(5)
      )
    }

    // Safe to delete everything before (including) file: 11 which has adjusted timestamp 12
    testBufferingLogDeletionIterator(maxTimestamp = 12, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8, 11, 14),
      deleted = Seq(5, 10, 11, 12)
    )

    Seq(0, 1, 2).foreach { retentionVersion =>
      testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = retentionVersion)(
        inputTimestamps = Seq(5, 10, 8, 11, 14),
        deleted = Seq(5)
      )
    }

    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 3)(
      inputTimestamps = Seq(5, 10, 8, 11, 14),
      deleted = Seq(5, 10, 11, 12)
    )

    // Test when the last element is adjusted with both timestamp and version
    Seq(9, 10, 11).foreach { retentionTimestamp =>
      testBufferingLogDeletionIterator(maxTimestamp = retentionTimestamp, maxVersion = 100)(
        inputTimestamps = Seq(5, 10, 8, 9),
        deleted = Seq(5)
      )
    }

    testBufferingLogDeletionIterator(maxTimestamp = 12, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8, 9),
      deleted = Seq(5, 10, 11, 12)
    )

    Seq(0, 1, 2).foreach { retentionVersion =>
      testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = retentionVersion)(
        inputTimestamps = Seq(5, 10, 8, 9),
        deleted = Seq(5)
      )
    }

    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 3)(
      inputTimestamps = Seq(5, 10, 8, 9),
      deleted = Seq(5, 10, 11, 12)
    )

    Seq(9, 10, 11).foreach { retentionTimestamp =>
      testBufferingLogDeletionIterator(maxTimestamp = retentionTimestamp, maxVersion = 100)(
        inputTimestamps = Seq(10, 8, 9),
        deleted = Nil
      )
    }

    // Test the first element causing cascading adjustments
    testBufferingLogDeletionIterator(maxTimestamp = 12, maxVersion = 100)(
      inputTimestamps = Seq(10, 8, 9),
      deleted = Seq(10, 11, 12)
    )

    Seq(0, 1).foreach { retentionVersion =>
      testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = retentionVersion)(
        inputTimestamps = Seq(10, 8, 9),
        deleted = Nil
      )
    }

    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 2)(
      inputTimestamps = Seq(10, 8, 9),
      deleted = Seq(10, 11, 12)
    )

    // Test multiple batches of time adjustments
    testBufferingLogDeletionIterator(maxTimestamp = 12, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8, 9, 12, 15, 14, 14), // 5, 10, 11, 12, 13, 15, 16, 17
      deleted = Seq(5)
    )

    Seq(13, 14, 15, 16).foreach { retentionTimestamp =>
      testBufferingLogDeletionIterator(maxTimestamp = retentionTimestamp, maxVersion = 100)(
        inputTimestamps = Seq(5, 10, 8, 9, 12, 15, 14, 14), // 5, 10, 11, 12, 13, 15, 16, 17
        deleted = Seq(5, 10, 11, 12, 13)
      )
    }

    testBufferingLogDeletionIterator(maxTimestamp = 17, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8, 9, 12, 15, 14, 14), // 5, 10, 11, 12, 13, 15, 16, 17
      deleted = Seq(5, 10, 11, 12, 13, 15, 16, 17)
    )
  }
}
