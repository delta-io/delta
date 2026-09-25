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
package io.delta.kernel.test;

import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Format;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.commit.DefaultFileSystemManagedTableOnlyCommitter;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.metrics.SnapshotQueryContext;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.FileStatus;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

/**
 * Java counterpart of the {@code MockSnapshotUtils} Scala trait, for use by JUnit 5 tests.
 *
 * <p>Only the plain {@code getMockSnapshot} form is ported. The in-commit-timestamp variants stay
 * in Scala until a Java suite needs them.
 */
public final class MockSnapshotFixtures {

  private MockSnapshotFixtures() {}

  /**
   * Matches {@code TransactionSuite.testSchema}, which the Scala helper builds its metadata from.
   */
  public static final StructType TEST_SCHEMA =
      new StructType()
          .add("name", StringType.STRING)
          .add("id", LongType.LONG)
          .add("city", StringType.STRING);

  /**
   * Creates a mock snapshot with valid metadata at the given version.
   *
   * <p>The Scala helper also took a {@code timestamp}, but never read it, so it is not ported.
   */
  public static SnapshotImpl getMockSnapshot(Path dataPath, long latestVersion) {
    Metadata metadata =
        new Metadata(
            "id",
            Optional.empty(), // name
            Optional.empty(), // description
            new Format(),
            TEST_SCHEMA.toJson(),
            TEST_SCHEMA,
            VectorUtils.buildArrayValue(Arrays.asList("c3"), StringType.STRING),
            Optional.of(123L),
            VectorUtils.stringStringMapValue(Collections.emptyMap()));

    Path logPath = new Path(dataPath, "_delta_log");
    FileStatus deltaAtEndVersion =
        FileStatus.of(
            FileNames.deltaFile(logPath, latestVersion), 1 /* size */, 1 /* modificationTime */);

    LogSegment logSegment =
        new LogSegment(
            logPath,
            latestVersion,
            Collections.singletonList(deltaAtEndVersion), // deltas
            Collections.emptyList(), // compactions
            Collections.emptyList(), // checkpoints
            deltaAtEndVersion,
            Optional.empty(), // lastSeenChecksum
            Optional.empty() // maxPublishedDeltaVersion
            );

    return new SnapshotImpl(
        dataPath,
        logSegment.getVersion(),
        new Lazy<>(() -> logSegment),
        null, // logReplay
        new Protocol(1, 2),
        metadata,
        DefaultFileSystemManagedTableOnlyCommitter.INSTANCE,
        SnapshotQueryContext.forLatestSnapshot(dataPath.toString()),
        Optional.empty() // inCommitTimestampOpt
        );
  }
}
