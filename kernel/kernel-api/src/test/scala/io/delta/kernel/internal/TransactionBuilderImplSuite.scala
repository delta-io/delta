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
import io.delta.kernel.types.{IntegerType, StringType, StructType}
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

class TransactionBuilderImplSuite extends AnyFunSuite with MockEngineUtils {

  /**
   * Creates a mock snapshot whose metadata includes a `delta.feature.<name>` property that
   * TransactionMetadataFactory would attempt to process. This is used to verify the early-return
   * path: if the code falls through to the factory, processing the unknown feature throws.
   */
  private def createMockSnapshot(
      dataPath: Path,
      version: Long,
      extraConfig: Map[String, String] = Map.empty): SnapshotImpl = {
    val schema = new StructType().add("id", IntegerType.INTEGER)
    val metadata = new Metadata(
      "id",
      Optional.empty(),
      Optional.empty(),
      new Format(),
      schema.toJson,
      schema,
      buildArrayValue(java.util.Arrays.asList(), StringType.STRING),
      Optional.of(123),
      stringStringMapValue(extraConfig.asJava))
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
      null, // logReplay - not needed; getCurrentCrcInfo is overridden below
      new Protocol(1, 2),
      metadata,
      DefaultFileSystemManagedTableOnlyCommitter.INSTANCE,
      snapshotQueryContext,
      Optional.empty()) {
      override def getCurrentCrcInfo: Optional[CRCInfo] = Optional.empty()
    }
  }

  test("early return when no metadata or protocol update is needed") {
    // The snapshot metadata includes an unrecognized delta.feature.* property. If the code
    // falls through to TransactionMetadataFactory (the bug), extractFeaturePropertyOverrides
    // will try to resolve this feature and throw. The early-return path skips the factory
    // entirely, so no exception is thrown.
    val dataPath = new Path("/tmp/test-table")
    val tableImpl = new TableImpl(dataPath.toString, () => System.currentTimeMillis())
    val snapshot = createMockSnapshot(
      dataPath,
      version = 0L,
      extraConfig = Map("delta.feature.fakeFeatureForEarlyReturnTest" -> "supported"))

    val builder = new TransactionBuilderImpl(tableImpl, "test-engine", Operation.WRITE)

    // With the fix this returns immediately; without the fix this would throw
    val txn = builder.buildTransactionInternal(
      mockEngine(),
      false, // isCreateOrReplace
      Optional.of(snapshot))

    // Verify the returned transaction does not mark protocol/metadata for update
    assert(txn.getSchema(mockEngine()) === snapshot.getMetadata().getSchema())
  }
}
