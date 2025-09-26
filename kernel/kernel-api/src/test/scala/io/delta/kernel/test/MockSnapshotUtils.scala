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
package io.delta.kernel.test
import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.TransactionSuite.testSchema
import io.delta.kernel.internal.{SnapshotImpl, TableConfig}
import io.delta.kernel.internal.actions.{Format, Metadata, Protocol}
import io.delta.kernel.internal.commit.DefaultFileSystemManagedTableOnlyCommitter
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.lang.Lazy
import io.delta.kernel.internal.metrics.SnapshotQueryContext
import io.delta.kernel.internal.snapshot.LogSegment
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.internal.util.VectorUtils.{buildArrayValue, stringStringMapValue}
import io.delta.kernel.types.StringType
import io.delta.kernel.utils.FileStatus

object MockSnapshotUtils extends MockSnapshotUtils

trait MockSnapshotUtils {

  /**
   * Creates a mock snapshot with valid metadata at the given version.
   * @param ictEnablementInfoOpt Controls the enablement state of in-commit timestamps.
   */
  def getMockSnapshot(
      dataPath: Path,
      latestVersion: Long,
      ictEnablementInfoOpt: Option[(Long, Long)] = None,
      timestamp: Long = 0L,
      deltaFileAtEndVersion: Option[FileStatus] = None): SnapshotImpl = {
    val configuration = ictEnablementInfoOpt match {
      case Some((version, _)) if version == 0L =>
        Map(TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true")
      case Some((version, ts)) =>
        Map(
          TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true",
          TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.getKey -> version.toString,
          TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.getKey -> ts.toString)
      case None =>
        Map[String, String]()
    }
    val metadata = new Metadata(
      "id",
      Optional.empty(), /* name */
      Optional.empty(), /* description */
      new Format(),
      testSchema.toJson,
      testSchema,
      buildArrayValue(java.util.Arrays.asList("c3"), StringType.STRING),
      Optional.of(123),
      stringStringMapValue(configuration.asJava));
    val logPath = new Path(dataPath, "_delta_log")

    val fs = deltaFileAtEndVersion.getOrElse(FileStatus.of(
      FileNames.deltaFile(logPath, latestVersion),
      1, /* size */
      1 /* modificationTime */ ))
    val logSegment = new LogSegment(
      logPath, /* logPath */
      latestVersion,
      Seq(fs).asJava, /* deltas */
      Seq.empty.asJava, /* compactions */
      Seq.empty.asJava, /* checkpoints */
      fs, /* deltaAtEndVersion */
      Optional.empty() /* lastSeenChecksum */
    )
    val snapshotQueryContext = SnapshotQueryContext.forLatestSnapshot(dataPath.toString)
    new SnapshotImpl(
      dataPath, /* dataPath */
      logSegment.getVersion, /* version */
      new Lazy(() => logSegment), /* logSegment */
      null, /* logReplay */
      new Protocol(1, 2), /* protocol */
      metadata,
      DefaultFileSystemManagedTableOnlyCommitter.INSTANCE,
      snapshotQueryContext /* snapshotContext */
    )
  }
}
