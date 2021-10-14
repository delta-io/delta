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

package io.delta.standalone.internal

import scala.collection.JavaConverters._

import io.delta.standalone.actions.{CommitInfo, Protocol, Metadata => MetadataJ, RemoveFile => RemoveFileJ, SetTransaction => SetTransactionJ}
import io.delta.standalone.internal.util.TestUtils._
import io.delta.standalone.DeltaLog

import org.apache.hadoop.conf.Configuration

class OptimisticTransactionSuite
  extends OptimisticTransactionSuiteBase
    with OptimisticTransactionSuiteTestVals {

  ///////////////////////////////////////////////////////////////////////////
  // Allowed concurrent actions
  ///////////////////////////////////////////////////////////////////////////

  check(
    "append / append",
    conflicts = false,
    reads = Seq(t => t.metadata()),
    concurrentWrites = Seq(addA),
    actions = Seq(addB))

  check(
    "disjoint txns",
    conflicts = false,
    reads = Seq(t => t.txnVersion("t1")),
    concurrentWrites = Seq(
      new SetTransactionJ("t2", 0, java.util.Optional.of(1234L))),
    actions = Nil)

  check(
    "disjoint delete / read",
    conflicts = false,
    setup = Seq(metadata_partX, addA_partX2),
    reads = Seq(t => t.markFilesAsRead(colXEq1Filter)),
    concurrentWrites = Seq(removeA),
    actions = Seq()
  )

  check(
    "disjoint add / read",
    conflicts = false,
    setup = Seq(metadata_partX),
    reads = Seq(t => t.markFilesAsRead(colXEq1Filter)),
    concurrentWrites = Seq(addA_partX2),
    actions = Seq()
  )

  check(
    "add / read + no write",  // no write = no real conflicting change even though data was added
    conflicts = false,        // so this should not conflict
    setup = Seq(metadata_partX),
    reads = Seq(t => t.markFilesAsRead(colXEq1Filter)),
    concurrentWrites = Seq(addA_partX1),
    actions = Seq())

  ///////////////////////////////////////////////////////////////////////////
  // Disallowed concurrent actions
  ///////////////////////////////////////////////////////////////////////////

  check(
    "delete / delete",
    conflicts = true,
    reads = Nil,
    concurrentWrites = Seq(removeA),
    actions = Seq(removeA_time5)
  )

  check(
    "add / read + write",
    conflicts = true,
    setup = Seq(metadata_partX),
    reads = Seq(t => t.markFilesAsRead(colXEq1Filter)),
    concurrentWrites = Seq(addA_partX1),
    actions = Seq(addB_partX1),
    // commit info should show operation as "Manual Update", because that's the operation used by
    // the harness
    errorMessageHint = Some("[x=1]" :: "Manual Update" :: Nil))

  check(
    "delete / read",
    conflicts = true,
    setup = Seq(metadata_partX, addA_partX1),
    reads = Seq(t => t.markFilesAsRead(colXEq1Filter)),
    concurrentWrites = Seq(removeA),
    actions = Seq(),
    errorMessageHint = Some("a in partition [x=1]" :: "Manual Update" :: Nil))

  check(
    "schema change",
    conflicts = true,
    reads = Seq(t => t.metadata),
    concurrentWrites = Seq(MetadataJ.builder().build()),
    actions = Nil)

  check(
    "conflicting txns",
    conflicts = true,
    reads = Seq(t => t.txnVersion("t1")),
    concurrentWrites = Seq(
      new SetTransactionJ("t1", 0, java.util.Optional.of(1234L))
    ),
    actions = Nil)

  check(
    "upgrade / upgrade",
    conflicts = true,
    reads = Seq(t => t.metadata),
    concurrentWrites = Seq(new Protocol()),
    actions = Seq(new Protocol()))

  check(
    "taint whole table",
    conflicts = true,
    setup = Seq(metadata_partX, addA_partX2),
    reads = Seq(
      t => t.markFilesAsRead(colXEq1Filter),
      // `readWholeTable` should disallow any concurrent change, even if the change
      // is disjoint with the earlier filter
      t => t.readWholeTable()
    ),
    concurrentWrites = Seq(addB_partX3),
    actions = Seq(addC_partX4)
  )

  check(
    "taint whole table + concurrent remove",
    conflicts = true,
    setup = Seq(metadata_colX, addA),
    reads = Seq(
      // `readWholeTable` should disallow any concurrent `RemoveFile`s.
      t => t.readWholeTable()
    ),
    concurrentWrites = Seq(removeA),
    actions = Seq(addB))

  // initial commit without metadata should fail
  // --> see OptimisticTransactionLegacySuite

  // initial commit with multiple metadata actions should fail
  // --> see OptimisticTransactionLegacySuite

  // AddFile with different partition schema compared to metadata should fail
  // --> see OptimisticTransactionLegacySuite

  test("isolation level shouldn't be null") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit((MetadataJ.builder().build() :: Nil).asJava, op, engineInfo)
      log.startTransaction().commit((addA :: Nil).asJava, op, engineInfo)

      val versionLogs = log.getChanges(0, true).asScala.toList

      def getIsolationLevel(version: Int): String = {
        versionLogs(version)
          .getActions
          .asScala
          .collectFirst { case c: CommitInfo => c }
          .map(_.getIsolationLevel.orElseGet(null))
          .get
      }

      assert(getIsolationLevel(0) == "SnapshotIsolation")
      assert(getIsolationLevel(1) == "Serializable")
    }
  }
}
