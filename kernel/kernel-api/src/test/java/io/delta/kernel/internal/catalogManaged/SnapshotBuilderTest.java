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

import static io.delta.kernel.test.KernelTestFixtures.PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT;
import static io.delta.kernel.test.KernelTestFixtures.emptyColumnarBatch;
import static io.delta.kernel.test.KernelTestFixtures.testMetadata;
import static io.delta.kernel.test.MockFileSystemFixtures.DATA_PATH;
import static io.delta.kernel.test.MockFileSystemFixtures.LOG_PATH;
import static io.delta.kernel.test.MockFileSystemFixtures.dataPathString;
import static io.delta.kernel.test.MockFileSystemFixtures.deltaFileStatus;
import static io.delta.kernel.test.MockFileSystemFixtures.logCompactionStatus;
import static io.delta.kernel.test.MockFileSystemFixtures.mockFSListFromEngine;
import static io.delta.kernel.test.MockFileSystemFixtures.parsedRatifiedStagedCommits;
import static io.delta.kernel.test.MockSnapshotFixtures.getMockSnapshot;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.TableManager;
import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.commit.CommitResponse;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.exceptions.UnsupportedProtocolVersionException;
import io.delta.kernel.exceptions.UnsupportedTableFeatureException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.commit.DefaultFileSystemManagedTableOnlyCommitter;
import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.files.ParsedPublishedDeltaData;
import io.delta.kernel.internal.table.SnapshotBuilderImpl;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class SnapshotBuilderTest {

  private static final Engine EMPTY_MOCK_ENGINE = mockFSListFromEngine(Collections.emptyList());
  private static final Protocol PROTOCOL = new Protocol(1, 2);
  private static final Metadata METADATA =
      testMetadata(new StructType().add("c1", IntegerType.INTEGER), Collections.emptyList());
  private static final SnapshotImpl MOCK_SNAPSHOT_AT_TIMESTAMP_0 =
      getMockSnapshot(DATA_PATH, 0L /* latestVersion */);

  private static List<Long> versions(Long... vs) {
    return Arrays.asList(vs);
  }

  ///////////////////////////////////////
  // Builder Validation Tests -- START //
  ///////////////////////////////////////

  @Test
  void loadTableNullPathThrowsNullPointerException() {
    assertThrows(NullPointerException.class, () -> TableManager.loadSnapshot(null));
  }

  // ===== Version Tests ===== //

  @Test
  void atVersionNegativeVersionThrowsIllegalArgumentException() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> TableManager.loadSnapshot(dataPathString()).atVersion(-1));

    assertEquals("version must be >= 0", ex.getMessage());
  }

  // ===== Timestamp Tests ===== //

  @Test
  void atTimestampNullLatestSnapshotThrowsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () -> TableManager.loadSnapshot(dataPathString()).atTimestamp(1000L, null));
  }

  @Test
  void atTimestampGreaterThanLatestSnapshotThrowsIllegalArgumentException() {
    SnapshotBuilderImpl builder =
        (SnapshotBuilderImpl)
            TableManager.loadSnapshot(dataPathString())
                .atTimestamp(99, MOCK_SNAPSHOT_AT_TIMESTAMP_0);

    KernelException ex =
        assertThrows(KernelException.class, () -> builder.build(EMPTY_MOCK_ENGINE));

    assertTrue(ex.getMessage().contains("The provided timestamp 99 ms"));
    assertTrue(ex.getMessage().contains("is after the latest available version"));
  }

  @Test
  void atTimestampAndVersionBothProvidedThrowsIllegalArgumentException() {
    SnapshotBuilderImpl builder =
        (SnapshotBuilderImpl)
            TableManager.loadSnapshot(dataPathString())
                .atVersion(1)
                .atTimestamp(0L, MOCK_SNAPSHOT_AT_TIMESTAMP_0);

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> builder.build(EMPTY_MOCK_ENGINE));

    assertEquals("timestamp and version cannot be provided together", ex.getMessage());
  }

  @Test
  void atTimestampProtocolAndMetadataWithTimestampThrowsIllegalArgumentException() {
    SnapshotBuilderImpl builder =
        (SnapshotBuilderImpl)
            TableManager.loadSnapshot(dataPathString())
                .atTimestamp(0L, MOCK_SNAPSHOT_AT_TIMESTAMP_0)
                .withProtocolAndMetadata(PROTOCOL, METADATA);

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> builder.build(EMPTY_MOCK_ENGINE));

    assertEquals(
        "protocol and metadata can only be provided if a version is provided", ex.getMessage());
  }

  // ===== Committer Tests ===== //

  @Test
  void withCommitterNullCommitterThrowsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () -> TableManager.loadSnapshot(dataPathString()).withCommitter(null));
  }

  @Test
  void whenNoCommitterIsProvidedTheDefaultCommitterIsCreated() {
    Committer committer =
        ((SnapshotBuilderImpl) TableManager.loadSnapshot(dataPathString()))
            .atVersion(1)
            // avoid trying to use engine to load log segment
            .withProtocolAndMetadata(PROTOCOL, METADATA)
            .build(EMPTY_MOCK_ENGINE)
            .getCommitter();

    assertInstanceOf(DefaultFileSystemManagedTableOnlyCommitter.class, committer);
  }

  /** Stands in for the local {@code CustomCommitter} class the Scala suite declared inline. */
  private static final class CustomCommitter implements Committer {
    @Override
    public CommitResponse commit(
        Engine engine, CloseableIterator<Row> finalizedActions, CommitMetadata commitMetadata) {
      throw new UnsupportedOperationException("Not implemented");
    }
  }

  @Test
  void customCommitterIsCorrectlyPropagated() {
    Committer committer =
        ((SnapshotBuilderImpl) TableManager.loadSnapshot(dataPathString()))
            .atVersion(1)
            .withCommitter(new CustomCommitter())
            // avoid trying to use engine to load log segment
            .withProtocolAndMetadata(PROTOCOL, METADATA)
            .build(EMPTY_MOCK_ENGINE)
            .getCommitter();

    assertInstanceOf(CustomCommitter.class, committer);
  }

  // ===== Protocol and Metadata Tests ===== //

  @Test
  void withProtocolAndMetadataNullProtocolThrowsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () -> TableManager.loadSnapshot(dataPathString()).withProtocolAndMetadata(null, METADATA));

    assertThrows(
        NullPointerException.class,
        () -> TableManager.loadSnapshot(dataPathString()).withProtocolAndMetadata(PROTOCOL, null));
  }

  @Test
  void withProtocolAndMetadataOnlyIfVersionIsProvided() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                TableManager.loadSnapshot(dataPathString())
                    .withProtocolAndMetadata(PROTOCOL, METADATA)
                    .build(EMPTY_MOCK_ENGINE));

    assertEquals(
        "protocol and metadata can only be provided if a version is provided", ex.getMessage());
  }

  @Test
  void withProtocolAndMetadataInvalidReaderVersionThrowsKernelException() {
    UnsupportedProtocolVersionException ex =
        assertThrows(
            UnsupportedProtocolVersionException.class,
            () ->
                TableManager.loadSnapshot(dataPathString())
                    .atVersion(10)
                    .withProtocolAndMetadata(new Protocol(999, 2), METADATA)
                    .build(EMPTY_MOCK_ENGINE));

    assertEquals(
        UnsupportedProtocolVersionException.ProtocolVersionType.READER, ex.getVersionType());
    assertTrue(ex.getMessage().contains("Unsupported Delta protocol reader version"));
  }

  @Test
  void withProtocolAndMetadataUnknownReaderFeatureThrowsKernelException() {
    UnsupportedTableFeatureException ex =
        assertThrows(
            UnsupportedTableFeatureException.class,
            () ->
                TableManager.loadSnapshot(dataPathString())
                    .atVersion(10)
                    .withProtocolAndMetadata(
                        new Protocol(
                            3,
                            7,
                            Collections.singleton("unknownReaderFeature"),
                            Collections.emptySet()),
                        METADATA)
                    .build(EMPTY_MOCK_ENGINE));

    assertTrue(ex.getMessage().contains("Unsupported Delta table feature"));
  }

  // ===== LogData Tests ===== //

  @Test
  void withLogDataNullInputThrowsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () -> TableManager.loadSnapshot(dataPathString()).withLogData(null));
  }

  private static Stream<ParsedLogData> nonStagedRatifiedCommits() {
    return Stream.of(
        ParsedCatalogCommitData.forInlineData(1, emptyColumnarBatch()),
        ParsedPublishedDeltaData.forFileStatus(deltaFileStatus(1, LOG_PATH)),
        ParsedLogData.forFileStatus(logCompactionStatus(0, 1)));
  }

  @ParameterizedTest(name = "type={0}")
  @MethodSource("nonStagedRatifiedCommits")
  void withLogDataNonStagedRatifiedCommitThrowsIllegalArgumentException(
      ParsedLogData parsedLogData) {
    SnapshotBuilderImpl builder =
        (SnapshotBuilderImpl)
            TableManager.loadSnapshot(dataPathString())
                .atVersion(1)
                .withLogData(Collections.singletonList(parsedLogData));

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> builder.build(EMPTY_MOCK_ENGINE));

    assertTrue(ex.getMessage().contains("Only staged ratified commits are supported"));
  }

  @Test
  void withLogDataNonContiguousInputThrowsIllegalArgumentException() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                TableManager.loadSnapshot(dataPathString())
                    .atVersion(2)
                    .withLogData(parsedRatifiedStagedCommits(versions(0L, 2L)))
                    .build(EMPTY_MOCK_ENGINE));

    assertTrue(ex.getMessage().contains("Log data must be sorted and contiguous"));
  }

  @Test
  void withLogDataNonSortedInputThrowsIllegalArgumentException() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                TableManager.loadSnapshot(dataPathString())
                    .atVersion(2)
                    .withLogData(parsedRatifiedStagedCommits(versions(2L, 1L, 0L)))
                    .build(EMPTY_MOCK_ENGINE));

    assertTrue(ex.getMessage().contains("Log data must be sorted and contiguous"));
  }

  /////////////////////////////////////
  // Builder Validation Tests -- END //
  /////////////////////////////////////

  @Test
  void ifProtocolAndMetadataAreProvidedThenLogSegmentIsNotLoaded() {
    SnapshotImpl snapshot =
        (SnapshotImpl)
            ((SnapshotBuilderImpl) TableManager.loadSnapshot(dataPathString()))
                .atVersion(13)
                .withProtocolAndMetadata(PROTOCOL, METADATA)
                .withLogData(Collections.emptyList())
                .build(EMPTY_MOCK_ENGINE);

    assertFalse(snapshot.getLazyLogSegment().isPresent());
  }

  // ===== MaxCatalogVersion Tests ===== //

  @Test
  void withMaxCatalogVersionNegativeVersionThrowsIllegalArgumentException() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> TableManager.loadSnapshot(dataPathString()).withMaxCatalogVersion(-1));

    assertEquals("A valid version must be >= 0", ex.getMessage());
  }

  @Test
  void withMaxCatalogVersionZeroIsValid() {
    // Should not throw
    TableManager.loadSnapshot(dataPathString())
        .atVersion(0)
        .withProtocolAndMetadata(PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT, METADATA)
        .withMaxCatalogVersion(0)
        .build(EMPTY_MOCK_ENGINE);
  }

  @Test
  void withMaxCatalogVersionPositiveVersionIsValid() {
    // Should not throw
    TableManager.loadSnapshot(dataPathString())
        .atVersion(10)
        .withProtocolAndMetadata(PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT, METADATA)
        .withMaxCatalogVersion(10)
        .build(EMPTY_MOCK_ENGINE);
  }

  @Test
  void withMaxCatalogVersionVersionTimeTravelMustBeAtMostMaxCatalogVersion() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                TableManager.loadSnapshot(dataPathString())
                    .atVersion(15)
                    .withMaxCatalogVersion(10)
                    .build(EMPTY_MOCK_ENGINE));

    assertEquals(
        "Cannot time-travel to version 15 after the max catalog version 10", ex.getMessage());
  }

  @Test
  void withMaxCatalogVersionVersionTimeTravelEqualToMaxCatalogVersionIsValid() {
    // Should not throw
    TableManager.loadSnapshot(dataPathString())
        .atVersion(10)
        .withProtocolAndMetadata(PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT, METADATA)
        .withMaxCatalogVersion(10)
        .build(EMPTY_MOCK_ENGINE);
  }

  @Test
  void withMaxCatalogVersionVersionTimeTravelLessThanMaxCatalogVersionIsValid() {
    // Should not throw
    TableManager.loadSnapshot(dataPathString())
        .atVersion(5)
        .withProtocolAndMetadata(PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT, METADATA)
        .withMaxCatalogVersion(10)
        .build(EMPTY_MOCK_ENGINE);
  }

  @Test
  void withMaxCatalogVersionTimestampTimeTravelLatestSnapshotMustHaveVersionEqualToMax() {
    SnapshotImpl mockSnapshotAtVersion5 = getMockSnapshot(DATA_PATH, 5L /* latestVersion */);

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                TableManager.loadSnapshot(dataPathString())
                    .atTimestamp(0L, mockSnapshotAtVersion5)
                    .withMaxCatalogVersion(10)
                    .build(EMPTY_MOCK_ENGINE));

    assertEquals(
        "The latestSnapshot provided for timestamp-based time-travel queries must "
            + "have version = maxCatalogVersion",
        ex.getMessage());
  }

  @Test
  void withMaxCatalogVersionTimestampTimeTravelWithMatchingLatestSnapshotVersionIsValid() {
    SnapshotImpl mockSnapshotAtVersion10 = getMockSnapshot(DATA_PATH, 10L /* latestVersion */);

    // Input validation should not throw (but will throw later when trying to construct log segment)
    Exception ex =
        assertThrows(
            Exception.class,
            () ->
                TableManager.loadSnapshot(dataPathString())
                    .atTimestamp(500L, mockSnapshotAtVersion10)
                    .withMaxCatalogVersion(10)
                    .build(EMPTY_MOCK_ENGINE));

    // Should fail on log segment loading, not on validation
    assertFalse(
        ex.getMessage().contains("latestSnapshot provided for timestamp-based time-travel"));
  }

  @Test
  void withMaxCatalogVersionWithoutVersionLogDataMustEndWithMaxCatalogVersion() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                TableManager.loadSnapshot(dataPathString())
                    .withLogData(parsedRatifiedStagedCommits(versions(0L, 1L, 2L)))
                    .withMaxCatalogVersion(5)
                    .build(EMPTY_MOCK_ENGINE));

    assertEquals("Provided catalog commits must end with max catalog version", ex.getMessage());
  }

  @Test
  void withMaxCatalogVersionWithoutVersionLogDataEndingWithMaxCatalogVersionIsValid() {
    // Input validation should not throw (but will throw later when trying to construct log segment)
    Exception ex =
        assertThrows(
            Exception.class,
            () ->
                TableManager.loadSnapshot(dataPathString())
                    .withLogData(parsedRatifiedStagedCommits(versions(0L, 1L, 2L, 3L, 4L, 5L)))
                    .withMaxCatalogVersion(5)
                    .build(EMPTY_MOCK_ENGINE));

    // Should fail on log segment loading, not on validation
    assertFalse(
        ex.getMessage().contains("Provided catalog commits must end with max catalog version"));
  }

  @Test
  void withMaxCatalogVersionEmptyLogDataWithMaxCatalogVersionIsValid() {
    // Should not throw - empty logData is allowed
    TableManager.loadSnapshot(dataPathString())
        .atVersion(5)
        .withProtocolAndMetadata(PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT, METADATA)
        .withLogData(Collections.emptyList())
        .withMaxCatalogVersion(5)
        .build(EMPTY_MOCK_ENGINE);
  }

  @Test
  void withMaxCatalogVersionVersionTimeTravelWithLogDataNotIncludingVersionFails() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                TableManager.loadSnapshot(dataPathString())
                    .atVersion(5)
                    .withLogData(parsedRatifiedStagedCommits(versions(0L, 1L, 2L, 3L)))
                    .withMaxCatalogVersion(10)
                    .build(EMPTY_MOCK_ENGINE));

    assertEquals(
        "Provided catalog commits must include versionToLoad for time-travel queries",
        ex.getMessage());
  }

  @Test
  void withMaxCatalogVersionVersionTimeTravelWithLogDataEndingAtVersionIsValid() {
    // Should not throw - logData ends exactly at requested version
    TableManager.loadSnapshot(dataPathString())
        .atVersion(5)
        .withProtocolAndMetadata(PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT, METADATA)
        .withLogData(parsedRatifiedStagedCommits(versions(0L, 1L, 2L, 3L, 4L, 5L)))
        .withMaxCatalogVersion(10)
        .build(EMPTY_MOCK_ENGINE);
  }

  @Test
  void withMaxCatalogVersionVersionTimeTravelWithLogDataBeyondVersionIsValid() {
    // Should not throw - logData extends beyond requested version
    TableManager.loadSnapshot(dataPathString())
        .atVersion(5)
        .withProtocolAndMetadata(PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT, METADATA)
        .withLogData(
            parsedRatifiedStagedCommits(versions(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)))
        .withMaxCatalogVersion(10)
        .build(EMPTY_MOCK_ENGINE);
  }

  @Test
  void validateMaxCatalogVersionPresenceCatalogManagedTableRequiresMaxCatalogVersion() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                TableManager.loadSnapshot(dataPathString())
                    .atVersion(1)
                    .withProtocolAndMetadata(PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT, METADATA)
                    .build(EMPTY_MOCK_ENGINE));

    assertEquals("Must provide maxCatalogVersion for catalogManaged tables", ex.getMessage());
  }

  @Test
  void validateMaxCatalogVersionPresenceNonCatalogManagedTableCannotHaveMaxCatalogVersion() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                TableManager.loadSnapshot(dataPathString())
                    .atVersion(1)
                    // protocol without catalogManaged
                    .withProtocolAndMetadata(PROTOCOL, METADATA)
                    .withMaxCatalogVersion(1)
                    .build(EMPTY_MOCK_ENGINE));

    assertEquals(
        "Should not provide maxCatalogVersion for file-system managed tables", ex.getMessage());
  }
}
