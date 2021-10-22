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

import java.util.Collections

import scala.collection.JavaConverters._

import io.delta.standalone.actions.{CommitInfo, Protocol, Action => ActionJ, AddFile => AddFileJ, Metadata => MetadataJ, RemoveFile => RemoveFileJ, SetTransaction => SetTransactionJ}
import io.delta.standalone.internal.util.TestUtils._
import io.delta.standalone.DeltaLog
import io.delta.standalone.types.{IntegerType, StringType, StructField, StructType}

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

  private def testSchemaChange(
      schema1: StructType,
      schema2: StructType,
      shouldThrow: Boolean,
      initialActions: Seq[ActionJ] = addA :: Nil,
      commitActions: Seq[ActionJ] = Nil): Unit = {
    withTempDir { dir =>
      val metadata1 = MetadataJ.builder().schema(schema1).build()
      val metadata2 = MetadataJ.builder().schema(schema2).build()

      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)

      log.startTransaction().commit((initialActions :+ metadata1).asJava, op, engineInfo)

      if (shouldThrow) {
        intercept[IllegalStateException] {
          log.startTransaction().commit((commitActions :+ metadata2).asJava, op, engineInfo)
        }
      } else {
        log.startTransaction().commit((commitActions :+ metadata2).asJava, op, engineInfo)
      }
    }
  }

  // Note: See SchemaUtilsSuite for thorough isWriteCompatible(existingSchema, newSchema) unit tests
  test("can change schema to valid schema") {
    // col a is non-nullable
    val schema1 = new StructType(Array(new StructField("a", new IntegerType(), false)))

    // add nullable field
    val schema2 = schema1.add(new StructField("b", new IntegerType(), true))
    testSchemaChange(schema1, schema2, shouldThrow = false)

    // add non-nullable field
    val schema3 = schema1.add(new StructField("b", new IntegerType(), false))
    testSchemaChange(schema1, schema3, shouldThrow = false)

    // relaxed nullability (from non-nullable to nullable)
    val schema4 = new StructType(Array(new StructField("a", new IntegerType(), true)))
    testSchemaChange(schema1, schema4, shouldThrow = false)
  }

  // Note: See SchemaUtilsSuite for thorough isWriteCompatible(existingSchema, newSchema) unit tests
  test("can't change schema to invalid schema - table non empty, files not removed") {
    // col a is nullable
    val schema1 = new StructType(Array(new StructField("a", new IntegerType(), true)))

    // drop a field
    val schema2 = new StructType(Array())
    testSchemaChange(schema1, schema2, shouldThrow = true)

    // restricted nullability (from nullable to non-nullable)
    val schema3 = new StructType(Array(new StructField("a", new IntegerType(), false)))
    testSchemaChange(schema1, schema3, shouldThrow = true)

    // change of datatype
    val schema4 = new StructType(Array(new StructField("a", new StringType(), true)))
    testSchemaChange(schema1, schema4, shouldThrow = true)
  }

  test("can change schema to 'invalid' schema - table empty or all files removed") {
    val schema1 = new StructType(Array(new StructField("a", new IntegerType())))
    val schema2 = new StructType(Array(new StructField("a", new StringType())))
    val addC = new AddFileJ("c", Collections.emptyMap(), 1, 1, true, null, null)

    // change of datatype - table is empty
    testSchemaChange(schema1, schema2, shouldThrow = false, initialActions = Nil)

    // change of datatype - all files are removed and new file added
    testSchemaChange(schema1, schema2, shouldThrow = false, commitActions = removeA :: addC :: Nil)

    // change of datatype - not all files are removed (should throw)
    testSchemaChange(schema1, schema2, shouldThrow = true, initialActions = addA :: addB :: Nil,
      commitActions = removeA :: Nil)
  }
}
