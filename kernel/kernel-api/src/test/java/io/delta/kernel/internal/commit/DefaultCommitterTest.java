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
package io.delta.kernel.internal.commit;

import static io.delta.kernel.test.KernelTestFixtures.PROTOCOL_12;
import static io.delta.kernel.test.KernelTestFixtures.PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT;
import static io.delta.kernel.test.KernelTestFixtures.commitMetadata;
import static io.delta.kernel.test.KernelTestFixtures.emptyActionsIterator;
import static io.delta.kernel.test.KernelTestFixtures.readState;
import static io.delta.kernel.test.KernelTestFixtures.testCommitInfo;
import static io.delta.kernel.test.KernelTestFixtures.testMetadata;
import static io.delta.kernel.test.MockEngineFixtures.mockEngine;
import static io.delta.kernel.test.MockFileSystemFixtures.dataPathString;
import static io.delta.kernel.test.MockFileSystemFixtures.mockFSListFromEngine;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.TableManager;
import io.delta.kernel.commit.CommitFailedException;
import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.commit.CommitResponse;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelEngineException;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.table.SnapshotBuilderImpl;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.test.MockEngineFixtures.BaseMockFileSystemClient;
import io.delta.kernel.test.MockEngineFixtures.BaseMockJsonHandler;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DefaultCommitterTest {

  private static final CommitMetadata BASIC_FILESYSTEM_COMMIT_METADATA_NO_PM_CHANGE =
      commitMetadata(1L)
          .commitInfo(testCommitInfo(false))
          .readPandMOpt(readState(PROTOCOL_12))
          .build();

  private static Stream<Arguments> catalogManagedProtocolPairs() {
    return Stream.of(
        Arguments.of(PROTOCOL_12, PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT, "Upgrade"),
        Arguments.of(PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT, PROTOCOL_12, "Downgrade"),
        Arguments.of(
            PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT,
            PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT,
            "CatalogManagedWrite"));
  }

  @ParameterizedTest(name = "{2}")
  @MethodSource("catalogManagedProtocolPairs")
  void defaultCommitterDoesNotSupportCatalogManagedTables(
      Protocol readProtocol, Protocol newProtocol, String testCase) {
    Engine emptyMockEngine = mockFSListFromEngine(Collections.emptyList());
    StructType schema = new StructType().add("col1", IntegerType.INTEGER);
    Metadata metadata = testMetadata(schema, Collections.emptyList());

    SnapshotBuilderImpl builder =
        (SnapshotBuilderImpl)
            TableManager.loadSnapshot(dataPathString())
                .withProtocolAndMetadata(readProtocol, metadata)
                .atVersion(1);
    if (readProtocol.supportsFeature(TableFeatures.CATALOG_MANAGED_RW_FEATURE)) {
      builder = (SnapshotBuilderImpl) builder.withMaxCatalogVersion(1);
    }
    Committer committer = builder.build(emptyMockEngine).getCommitter();

    assertInstanceOf(DefaultFileSystemManagedTableOnlyCommitter.class, committer);

    UnsupportedOperationException ex =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                committer.commit(
                    emptyMockEngine,
                    emptyActionsIterator(),
                    commitMetadata(3L)
                        .readPandMOpt(readState(readProtocol, metadata))
                        .newProtocolOpt(Optional.of(newProtocol))
                        .newMetadataOpt(Optional.of(metadata))
                        .build()));

    assertTrue(
        ex.getMessage()
            .contains(
                "No io.delta.kernel.commit.Committer has been provided to Kernel, so "
                    + "Kernel is using a default Committer that only supports committing to "
                    + "filesystem-managed Delta tables, not catalog-managed Delta tables. Since "
                    + "this table is catalog-managed, this commit operation is unsupported"));
  }

  ////////////////////////////////////////////////////////
  // DefaultCommitter exception handling tests -- START //
  ////////////////////////////////////////////////////////

  private static Stream<Arguments> exceptionCases() {
    return Stream.of(
        Arguments.of(
            "FileAlreadyExistsException -> CFE(true, true)",
            new FileAlreadyExistsException("_delta_log/001.json"),
            Optional.of(true),
            Optional.of(true),
            CommitFailedException.class,
            FileAlreadyExistsException.class),
        Arguments.of(
            "IOException -> CFE(true, false)",
            new IOException("Network timeout writing to _delta_log/001.json"),
            Optional.of(true),
            Optional.of(false),
            CommitFailedException.class,
            IOException.class),
        Arguments.of(
            "RuntimeException wrapped and thrown as KernelEngineException",
            new RuntimeException("Some runtime error"),
            Optional.empty(),
            Optional.empty(),
            KernelEngineException.class,
            RuntimeException.class));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("exceptionCases")
  void defaultCommitterHandlesExceptions(
      String description,
      Exception exceptionToThrow,
      Optional<Boolean> expectedRetryableOpt,
      Optional<Boolean> expectedConflictOpt,
      Class<?> expectedThrownType,
      Class<?> expectedCauseType) {
    Engine throwingEngine =
        mockEngine(
            null,
            new BaseMockJsonHandler() {
              @Override
              public void writeJsonFileAtomically(
                  String filePath, CloseableIterator<Row> data, boolean overwrite)
                  throws IOException {
                if (exceptionToThrow instanceof IOException) {
                  throw (IOException) exceptionToThrow;
                }
                throw (RuntimeException) exceptionToThrow;
              }
            },
            null,
            null);

    Exception ex =
        assertThrows(
            Exception.class,
            () ->
                DefaultFileSystemManagedTableOnlyCommitter.INSTANCE.commit(
                    throwingEngine,
                    emptyActionsIterator(),
                    BASIC_FILESYSTEM_COMMIT_METADATA_NO_PM_CHANGE));

    assertEquals(expectedThrownType, ex.getClass());
    assertEquals(expectedCauseType, ex.getCause().getClass());

    expectedRetryableOpt.ifPresent(
        expectedRetryable -> {
          CommitFailedException commitEx = assertInstanceOf(CommitFailedException.class, ex);
          assertEquals(expectedRetryable, commitEx.isRetryable());
        });

    expectedConflictOpt.ifPresent(
        expectedConflict -> {
          CommitFailedException commitEx = assertInstanceOf(CommitFailedException.class, ex);
          assertEquals(expectedConflict, commitEx.isConflict());
        });
  }

  //////////////////////////////////////////////////////
  // DefaultCommitter exception handling tests -- END //
  //////////////////////////////////////////////////////

  @Test
  void successCommitReturnsParsedLogDataContainingFileStatusForThatCommitFile()
      throws CommitFailedException {
    AtomicReference<FileStatus> writtenFileStatus = new AtomicReference<>();

    Engine fakeWriteReadJsonEngine =
        mockEngine(
            new BaseMockFileSystemClient() {
              @Override
              public FileStatus getFileStatus(String path) {
                return writtenFileStatus.get();
              }
            },
            new BaseMockJsonHandler() {
              @Override
              public void writeJsonFileAtomically(
                  String filePath, CloseableIterator<Row> data, boolean overwrite) {
                // (path, size, modTime)
                writtenFileStatus.set(FileStatus.of(filePath, 1234L, 4567L));
              }
            },
            null,
            null);

    CommitResponse commitResult =
        DefaultFileSystemManagedTableOnlyCommitter.INSTANCE.commit(
            fakeWriteReadJsonEngine,
            emptyActionsIterator(),
            BASIC_FILESYSTEM_COMMIT_METADATA_NO_PM_CHANGE);

    ParsedLogData commit = commitResult.getCommitLogData();

    assertTrue(commit.isFile());
    assertEquals(BASIC_FILESYSTEM_COMMIT_METADATA_NO_PM_CHANGE.getVersion(), commit.getVersion());
    assertSame(writtenFileStatus.get(), commit.getFileStatus());
  }
}
