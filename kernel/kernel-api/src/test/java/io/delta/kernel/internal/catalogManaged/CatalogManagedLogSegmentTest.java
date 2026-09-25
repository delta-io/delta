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
package io.delta.kernel.internal.catalogManaged;

import static io.delta.kernel.test.KernelTestFixtures.testMetadata;
import static io.delta.kernel.test.MockFileSystemFixtures.checksumFileStatus;
import static io.delta.kernel.test.MockFileSystemFixtures.classicCheckpointFileStatus;
import static io.delta.kernel.test.MockFileSystemFixtures.dataPathString;
import static io.delta.kernel.test.MockFileSystemFixtures.deltaFileStatuses;
import static io.delta.kernel.test.MockFileSystemFixtures.mockFSListFromEngine;
import static io.delta.kernel.test.MockFileSystemFixtures.parsedRatifiedStagedCommits;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.TableManager;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.InvalidTableException;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.table.SnapshotBuilderImpl;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.FileStatus;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CatalogManagedLogSegmentTest {

  // TODO: test with ratified=inline

  /** Inclusive range, matching Scala's {@code a to b}. */
  private static List<Long> range(long fromInclusive, long toInclusive) {
    return LongStream.rangeClosed(fromInclusive, toInclusive).boxed().collect(Collectors.toList());
  }

  private static List<Long> versions(Long... vs) {
    return java.util.Arrays.asList(vs);
  }

  /**
   * One {@code testLogSegment} case from the Scala suite. Built through a builder because the Scala
   * helper defaulted four of its eight parameters.
   */
  private static final class Case {
    private final String name;
    private long versionToLoad;
    private Optional<Long> checkpointVersionOpt = Optional.empty();
    private List<Long> deltaVersions = Collections.emptyList();
    private List<Long> ratifiedCommitVersions = Collections.emptyList();
    private List<Long> crcVersions = Collections.emptyList();
    private Optional<List<Long>> expectedDeltaAndCommitVersionsOpt = Optional.empty();
    private Optional<Class<? extends Exception>> expectedExceptionClassOpt = Optional.empty();

    Case(String name) {
      this.name = name;
    }

    Case versionToLoad(long v) {
      this.versionToLoad = v;
      return this;
    }

    Case checkpointVersion(long v) {
      this.checkpointVersionOpt = Optional.of(v);
      return this;
    }

    Case deltaVersions(List<Long> v) {
      this.deltaVersions = v;
      return this;
    }

    Case ratifiedCommitVersions(List<Long> v) {
      this.ratifiedCommitVersions = v;
      return this;
    }

    Case crcVersions(List<Long> v) {
      this.crcVersions = v;
      return this;
    }

    Case expectedDeltaAndCommitVersions(List<Long> v) {
      this.expectedDeltaAndCommitVersionsOpt = Optional.of(v);
      return this;
    }

    Case expectedException(Class<? extends Exception> clazz) {
      this.expectedExceptionClassOpt = Optional.of(clazz);
      return this;
    }

    @Override
    public String toString() {
      return name + " - ratified=materialized";
    }
  }

  private static Stream<Arguments> logSegmentCases() {
    return Stream.of(
            // _delta_log: [                          10.checkpoint+json, 11.json, 12.json]
            // catalog:    [8.uuid.json, 9.uuid.json                                      ]
            new Case("Build RT with ratified commits that are before first checkpoint")
                .versionToLoad(12L)
                .checkpointVersion(10L)
                .deltaVersions(range(10L, 12L))
                .ratifiedCommitVersions(range(8L, 9L))
                .expectedDeltaAndCommitVersions(range(11L, 12L)),

            // _delta_log: [          10.checkpoint+json, 11.json, 12.json, 13.json]
            // catalog:    [9.uuid.json, 10.uuid.json, 11.uuid.json                ]
            new Case("Build RT with ratified commits that overlap w first checkpoint + deltas")
                .versionToLoad(13L)
                .checkpointVersion(10L)
                .deltaVersions(range(10L, 13L))
                .ratifiedCommitVersions(range(9L, 11L))
                .expectedDeltaAndCommitVersions(range(11L, 13L)),

            // _delta_log: [10.checkpoint+json, 11.json, 12.json, 13.json, 14.json, 15.json]
            // catalog:    [                  11.uuid.json, 12.uuid.json, 13.uuid.json     ]
            new Case(
                    "Build RT with ratified commits that are contained within first checkpoint "
                        + "+ deltas")
                .versionToLoad(15L)
                .checkpointVersion(10L)
                .deltaVersions(range(10L, 15L))
                .ratifiedCommitVersions(range(11L, 13L))
                .expectedDeltaAndCommitVersions(range(11L, 15L)),

            // _delta_log: [             10.checkpoint+json, 11.json, 12.json               ]
            // catalog:    [9.uuid.json, 10.uuid.json 11.uuid.json, 12.uuid.json, 13.uuid.json]
            new Case("Build RT with ratified commits that supersets the first checkpoint + deltas")
                .versionToLoad(13L)
                .checkpointVersion(10L)
                .deltaVersions(range(10L, 12L))
                .ratifiedCommitVersions(range(9L, 13L))
                .expectedDeltaAndCommitVersions(range(11L, 13L)),

            // _delta_log: [10.checkpoint+json, 11.json, 12.json                              ]
            // catalog:    [                          12.uuid.json, 13.uuid.json, 14.uuid.json]
            new Case("Build RT with ratified commits that overlap with end of deltas")
                .versionToLoad(14L)
                .checkpointVersion(10L)
                .deltaVersions(range(10L, 12L))
                .ratifiedCommitVersions(range(12L, 14L))
                .expectedDeltaAndCommitVersions(range(11L, 14L)),

            // _delta_log: [10.checkpoint+json, 11.json, 12.json                           ]
            // catalog:    [                                     13.uuid.json, 14.uuid.json]
            new Case("Build RT with ratified commits that are after (no gap) the deltas")
                .versionToLoad(14L)
                .checkpointVersion(10L)
                .deltaVersions(range(10L, 12L))
                .ratifiedCommitVersions(range(13L, 14L))
                .expectedDeltaAndCommitVersions(range(11L, 14L)),

            // versionToLoad:     V
            // _delta_log: [10.checkpoint+json, 11.json, 12.json                           ]
            // catalog:    [                                     13.uuid.json, 14.uuid.json]
            new Case(
                    "Build RT with commit versions > versionToLoad - versionToLoad = checkpoint "
                        + "version")
                .versionToLoad(10L)
                .checkpointVersion(10L)
                .deltaVersions(range(10L, 12L))
                .ratifiedCommitVersions(range(13L, 14L))
                .expectedDeltaAndCommitVersions(Collections.emptyList()),

            // versionToLoad:                              V
            // _delta_log: [10.checkpoint+json, 11.json, 12.json                           ]
            // catalog:    [                                     13.uuid.json, 14.uuid.json]
            new Case(
                    "Build RT with commit versions > versionToLoad - versionToLoad = delta version")
                .versionToLoad(12L)
                .checkpointVersion(10L)
                .deltaVersions(range(10L, 12L))
                .ratifiedCommitVersions(range(13L, 14L))
                .expectedDeltaAndCommitVersions(range(11L, 12L)),

            // _delta_log: [0.json,                                      ]
            // catalog:    [        1.uuid.json, 2.uuid.json, 3.uuid.json]
            new Case("Build RT with only deltas and ratified commits (no checkpoint)")
                .versionToLoad(3L)
                .deltaVersions(versions(0L))
                .ratifiedCommitVersions(range(1L, 3L))
                .expectedDeltaAndCommitVersions(range(0L, 3L)),

            // _delta_log: [10.checkpoint+json,             ]
            // catalog:    [                    11.uuid.json]
            new Case("Build RT when checkpoint version is the last version from the filesystem")
                .versionToLoad(11L)
                .checkpointVersion(10L)
                .deltaVersions(versions(10L))
                .ratifiedCommitVersions(versions(11L))
                .expectedDeltaAndCommitVersions(versions(11L)),

            // _delta_log: [10.checkpoint+json, 11.json+crc, 12.json, 13.crc, 15.crc]
            // catalog:    [   13.uuid.json, 14.uuid.json, 15.uuid.json, 16.uuid.json]
            new Case("Build LogSegment with CRC files for unpublished versions")
                .versionToLoad(16L)
                .checkpointVersion(10L)
                .deltaVersions(versions(10L, 11L, 12L))
                .ratifiedCommitVersions(range(13L, 16L))
                .crcVersions(versions(11L, 13L, 15L))
                .expectedDeltaAndCommitVersions(range(11L, 16L)),

            // TODO: Support "Build RT with only ratified commits" in a followup PR. It is still
            // commented out in the Scala suite this was converted from.

            // _delta_log: [10.checkpoint+json, 11.json, 12.json                          ]
            // catalog:    [                                14.uuid.json, 15.uuid.json    ]
            new Case("Build RT with ratified commits that are after (with gap) the deltas => ERROR")
                .versionToLoad(15L)
                .checkpointVersion(10L)
                .deltaVersions(range(10L, 12L))
                .ratifiedCommitVersions(range(14L, 15L))
                .expectedException(InvalidTableException.class))
        .map(Arguments::of);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("logSegmentCases")
  void testLogSegment(Case testCase) {
    List<FileStatus> contents = new ArrayList<>();
    testCase.checkpointVersionOpt.ifPresent(v -> contents.add(classicCheckpointFileStatus(v)));
    contents.addAll(deltaFileStatuses(testCase.deltaVersions));
    testCase.crcVersions.forEach(v -> contents.add(checksumFileStatus(v)));

    Engine engine = mockFSListFromEngine(contents);

    StructType testSchema = new StructType().add("c1", IntegerType.INTEGER);

    SnapshotBuilderImpl builder =
        (SnapshotBuilderImpl)
            ((SnapshotBuilderImpl) TableManager.loadSnapshot(dataPathString()))
                .atVersion(testCase.versionToLoad)
                .withProtocolAndMetadata(
                    new Protocol(1, 2), testMetadata(testSchema, Collections.emptyList()))
                .withLogData(parsedRatifiedStagedCommits(testCase.ratifiedCommitVersions));

    if (testCase.expectedExceptionClassOpt.isPresent()) {
      // Ensure we load the LogSegment to identify any gaps/issues
      Throwable exception =
          assertThrows(Throwable.class, () -> builder.build(engine).getLogSegment());
      assertInstanceOf(testCase.expectedExceptionClassOpt.get(), exception);
      return;
    }

    LogSegment logSegment = builder.build(engine).getLogSegment();
    List<FileStatus> actualDeltaAndCommitFileStatuses = logSegment.getDeltas();

    // Check: we got the expected versions
    List<Long> actualDeltaAndCommitVersions =
        actualDeltaAndCommitFileStatuses.stream()
            .map(x -> FileNames.deltaVersion(x.getPath()))
            .collect(Collectors.toList());
    assertEquals(testCase.expectedDeltaAndCommitVersionsOpt.get(), actualDeltaAndCommitVersions);

    // Check: ratified commits take priority over published deltas when versions overlap
    Set<Long> expectedRatifiedVersions = new LinkedHashSet<>(testCase.ratifiedCommitVersions);
    expectedRatifiedVersions.retainAll(new LinkedHashSet<>(actualDeltaAndCommitVersions));

    for (FileStatus fileStatus : actualDeltaAndCommitFileStatuses) {
      String path = fileStatus.getPath();
      long version = FileNames.deltaVersion(path);
      if (expectedRatifiedVersions.contains(version)) {
        assertTrue(FileNames.isStagedDeltaFile(path), path + " should be a staged delta file");
      } else {
        assertTrue(
            FileNames.isPublishedDeltaFile(path), path + " should be a published delta file");
      }
    }

    // Check: maxPublishedDeltaVersion
    Optional<Long> expectedMaxPublishedDeltaVersion =
        testCase.deltaVersions.stream().filter(v -> v <= testCase.versionToLoad).max(Long::compare);
    assertEquals(expectedMaxPublishedDeltaVersion, logSegment.getMaxPublishedDeltaVersion());

    // Check: lastSeenChecksum
    List<Long> eligibleCrcVersions =
        testCase.crcVersions.stream()
            .filter(
                v ->
                    v <= testCase.versionToLoad
                        && testCase.checkpointVersionOpt.map(cp -> v >= cp).orElse(true))
            .collect(Collectors.toList());

    if (eligibleCrcVersions.isEmpty()) {
      assertFalse(logSegment.getLastSeenChecksum().isPresent());
    } else {
      long expectedVersion = eligibleCrcVersions.get(eligibleCrcVersions.size() - 1);
      String checksumPath = logSegment.getLastSeenChecksum().get().getPath();
      assertEquals(expectedVersion, FileNames.checksumVersion(checksumPath));
    }
  }
}
