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

import java.util.{Collections, Optional, UUID}

import scala.collection.JavaConverters._

import io.delta.standalone.{DeltaLog, Operation}
import io.delta.standalone.actions.{AddFile => AddFileJ, Format => FormatJ, Metadata => MetadataJ}
import io.delta.standalone.types.{IntegerType, StringType, StructField, StructType}
import io.delta.standalone.internal.util.TestUtils._
import org.apache.hadoop.conf.Configuration

// scalastyle:off funsuite
import org.scalatest.FunSuite

class OptimisticTransactionSuite extends FunSuite {
  // scalastyle:on funsuite

//  val schema = new StructType(Array(
//    new StructField("col1", new IntegerType(), true),
//    new StructField("col2", new StringType(), true)))

  val metadata = new MetadataJ(UUID.randomUUID().toString, null, null, new FormatJ(),
    Collections.emptyList(), Collections.emptyMap(), Optional.of(100L), new StructType(Array.empty))

  val add1 = new AddFileJ("fake/path/1", Collections.emptyMap(), 100, 100, true, null, null)
  val add2 = new AddFileJ("fake/path/2", Collections.emptyMap(), 100, 100, true, null, null)

  val ManualUpdate = new Operation(Operation.Name.MANUAL_UPDATE)

  test("basic") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      val actions = Seq(metadata, add1, add2)
      txn.commit(actions.asJava, ManualUpdate, "test-writer-id")

      val versionLogs = log.getChanges(0, true).asScala.toList
      val readActions = versionLogs(0).getActions.asScala

      assert(actions.toSet.subsetOf(readActions.toSet))
    }
  }

  // TODO: test prepareCommit > assert not already committed

  // TODO: test prepareCommit > assert user didn't commit a CommitInfo

  // TODO: test prepareCommit > have more than 1 Metadata in transaction

  // TODO: test prepareCommit > 1st commit & ensureLogDirectoryExist throws

  // TODO: test prepareCommit > 1st commit & commitValidationEnabled & metadataAbsentException

  // TODO: test prepareCommit > 1st commit & !commitValidationEnabled & no metadataAbsentException

  // TODO: test prepareCommit > protocolDowngradeException (reader)

  // TODO: test prepareCommit > protocolDowngradeException (writer)

  // TODO: test prepareCommit > commitValidationEnabled & addFilePartitioningMismatchException

  // TODO: test prepareCommit > !commitValidationEnabled & no addFilePartitioningMismatchException

  // TODO: test prepareCommit > assertProtocolWrite

  // TODO: test prepareCommit > assertRemovable

  // TODO: test verifyNewMetadata > SchemaMergingUtils.checkColumnNameDuplication

  // TODO: test verifyNewMetadata > SchemaUtils.checkFieldNames(dataSchema) > ParquetSchemaConverter

  // TODO: test verifyNewMetadata > SchemaUtils.checkFieldNames(dataSchema) > invalidColumnName

  // TODO: test verifyNewMetadata > ...checkFieldNames(partitionColumns) > ParquetSchemaConverter

  // TODO: test verifyNewMetadata > ...checkFieldNames(partitionColumns) > invalidColumnName

  // TODO: test verifyNewMetadata > Protocol.checkProtocolRequirements

  // TODO: test commit
  // - commitInfo is actually added to final actions
  // - isBlindAppend == true
  // - isBlindAppend == false
  // - different operation names

  // TODO: test doCommit > IllegalStateException

  // TODO: test doCommit > DeltaConcurrentModificationException
}
