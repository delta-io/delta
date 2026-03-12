/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.internal

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.Operation
import io.delta.kernel.internal.actions.{Format, Metadata, Protocol}
import io.delta.kernel.internal.checksum.CRCInfo
import io.delta.kernel.internal.commit.DefaultFileSystemManagedTableOnlyCommitter
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.lang.Lazy
import io.delta.kernel.internal.metrics.SnapshotQueryContext
import io.delta.kernel.internal.snapshot.LogSegment
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.internal.util.VectorUtils.{buildArrayValue, stringStringMapValue}
import io.delta.kernel.test.MockEngineUtils
import io.delta.kernel.types.{StringType, StructType}
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

class TransactionBuilderImplSuite extends AnyFunSuite with MockEngineUtils {

  /** Creates a mock snapshot with getCurrentCrcInfo overridden to avoid null logReplay NPE. */
  private def createMockSnapshot(dataPath: Path, version: Long): SnapshotImpl = {
    val schema = new StructType()
      .add("id", io.delta.kernel.types.IntegerType.INTEGER)
    val metadata = new Metadata(
      "id",
      Optional.empty(),
      Optional.empty(),
      new Format(),
      schema.toJson,
      schema,
      buildArrayValue(java.util.Arrays.asList(), StringType.STRING),
      Optional.of(123),
      stringStringMapValue(Map.empty[String, String].asJava))
    val logPath = new Path(dataPath, "_delta_log")
    val fs = FileStatus.of(FileNames.deltaFile(logPath, version), 1, 1)
    val logSegment = new LogSegment(
      logPath,
      version,
      Seq(fs).asJava,
      Seq.empty.asJava,
      Seq.empty.asJava,
      fs,
      Optional.empty(),
      Optional.empty())
    val snapshotQueryContext = SnapshotQueryContext.forLatestSnapshot(dataPath.toString)
    new SnapshotImpl(
      dataPath,
      logSegment.getVersion,
      new Lazy(() => logSegment),
      null, // logReplay not needed for this test
      new Protocol(1, 2),
      metadata,
      DefaultFileSystemManagedTableOnlyCommitter.INSTANCE,
      snapshotQueryContext,
      Optional.empty()) {
      override def getCurrentCrcInfo: Optional[CRCInfo] = Optional.empty()
    }
  }

  test("buildTransactionInternal returns early with empty protocol/metadata " +
    "when no metadata or protocol update is needed") {
    val dataPath = new Path("/tmp/test-table")
    val tableImpl = new TableImpl(dataPath.toString, () => System.currentTimeMillis())
    val snapshot = createMockSnapshot(dataPath, version = 0L)

    // Build with no schema, no table properties, no clustering columns
    // so needsMetadataOrProtocolUpdate is false for a non-create transaction
    val builder = new TransactionBuilderImpl(tableImpl, "test-engine", Operation.WRITE)

    val txn = builder.buildTransactionInternal(
      mockEngine(),
      false, // isCreateOrReplace
      Optional.of(snapshot))

    // Verify shouldUpdateProtocol=false and shouldUpdateMetadata=false via reflection
    val shouldUpdateProtocolField =
      classOf[TransactionImpl].getDeclaredField("shouldUpdateProtocol")
    shouldUpdateProtocolField.setAccessible(true)
    assert(
      !shouldUpdateProtocolField.getBoolean(txn),
      "Expected shouldUpdateProtocol to be false for the early-return path")

    val shouldUpdateMetadataField =
      classOf[TransactionImpl].getDeclaredField("shouldUpdateMetadata")
    shouldUpdateMetadataField.setAccessible(true)
    assert(
      !shouldUpdateMetadataField.getBoolean(txn),
      "Expected shouldUpdateMetadata to be false for the early-return path")

    // Verify the transaction uses the snapshot's existing protocol and metadata
    val protocolField = classOf[TransactionImpl].getDeclaredField("protocol")
    protocolField.setAccessible(true)
    assert(protocolField.get(txn) === snapshot.getProtocol())

    val metadataField = classOf[TransactionImpl].getDeclaredField("metadata")
    metadataField.setAccessible(true)
    assert(metadataField.get(txn) === snapshot.getMetadata())
  }
}
