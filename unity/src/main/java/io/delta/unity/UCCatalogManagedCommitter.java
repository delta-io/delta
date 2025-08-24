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

package io.delta.unity;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Preconditions.checkState;
import static io.delta.unity.utils.OperationTimer.timeCheckedOperation;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.commit.CommitFailedException;
import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.commit.CommitResponse;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link Committer} that handles commits to Delta tables managed by Unity
 * Catalog. That is, these Delta tables must have the catalogManaged table feature supported.
 */
public class UCCatalogManagedCommitter implements Committer {
  private static final Logger logger = LoggerFactory.getLogger(UCCatalogManagedCommitter.class);

  private final UCClient ucClient;
  private final String ucTableId;
  private final Path tablePath;

  /**
   * Creates a new UCCatalogManagedCommitter for the specified Unity Catalog-managed Delta table.
   *
   * @param ucClient the Unity Catalog client to use for commit operations
   * @param ucTableId the unique Unity Catalog table identifier
   * @param tablePath the path to the Delta table in the underlying storage system
   */
  public UCCatalogManagedCommitter(UCClient ucClient, String ucTableId, String tablePath) {
    this.ucClient = requireNonNull(ucClient, "ucClient is null");
    this.ucTableId = requireNonNull(ucTableId, "ucTableId is null");
    this.tablePath = new Path(requireNonNull(tablePath, "tablePath is null"));
  }

  @Override
  public CommitResponse commit(
      Engine engine, CloseableIterator<Row> finalizedActions, CommitMetadata commitMetadata)
      throws CommitFailedException {
    requireNonNull(engine, "engine is null");
    requireNonNull(finalizedActions, "finalizedActions is null");
    requireNonNull(commitMetadata, "commitMetadata is null");
    validateLogPathBelongsToThisUcTable(commitMetadata);

    final CommitMetadata.CommitType commitType = commitMetadata.getCommitType();

    if (commitType == CommitMetadata.CommitType.CATALOG_CREATE) {
      return createImpl(engine, finalizedActions, commitMetadata);
    }
    if (commitType == CommitMetadata.CommitType.CATALOG_WRITE) {
      return writeImpl(engine, finalizedActions, commitMetadata);
    }

    throw new UnsupportedOperationException("Unsupported commit type: " + commitType);
  }

  /**
   * Handles CATALOG_CREATE by writing the published delta file for version 0.
   *
   * <p>Note that this assumes that the table is being created within a staging location, and that
   * the Connector will post-commit inform UC of this 000.json file.
   */
  // TODO: [delta-io/delta#5118] If UC changes CREATE semantics, update logic here.
  private CommitResponse createImpl(
      Engine engine, CloseableIterator<Row> finalizedActions, CommitMetadata commitMetadata)
      throws CommitFailedException {
    final FileStatus kernelPublishedDeltaFileStatus =
        writePublishedDeltaFileForVersion0(engine, finalizedActions, commitMetadata);

    return new CommitResponse(ParsedLogData.forFileStatus(kernelPublishedDeltaFileStatus));
  }

  /**
   * Handles CATALOG_WRITE by writing the staged commit file and then committing (e.g. REST or RPC
   * call) to UC server.
   */
  private CommitResponse writeImpl(
      Engine engine, CloseableIterator<Row> finalizedActions, CommitMetadata commitMetadata)
      throws CommitFailedException {
    if (commitMetadata.getNewProtocolOpt().isPresent()) {
      // TODO: support this
      throw new UnsupportedOperationException("Protocol change is not yet implemented");
    }
    if (commitMetadata.getNewMetadataOpt().isPresent()) {
      // TODO: support this
      throw new UnsupportedOperationException("Metadata change is not yet implemented");
    }

    final FileStatus kernelStagedCommitFileStatus =
        writeStagedCommitFile(engine, finalizedActions, commitMetadata);

    commitToUC(commitMetadata, kernelStagedCommitFileStatus);

    return new CommitResponse(ParsedLogData.forFileStatus(kernelStagedCommitFileStatus));
  }

  ////////////////////
  // Helper methods //
  ////////////////////

  private String normalize(Path path) {
    return path.toUri().normalize().toString();
  }

  private void validateLogPathBelongsToThisUcTable(CommitMetadata cm) {
    final String expectedDeltaLogPathNormalized = normalize(new Path(tablePath, "_delta_log"));
    final String providedDeltaLogPathNormalized = normalize(new Path(cm.getDeltaLogDirPath()));
    checkArgument(
        expectedDeltaLogPathNormalized.equals(providedDeltaLogPathNormalized),
        "Delta log path '%s' does not match expected '%s'",
        expectedDeltaLogPathNormalized,
        providedDeltaLogPathNormalized);
  }

  private FileStatus writeStagedCommitFile(
      Engine engine, CloseableIterator<Row> finalizedActions, CommitMetadata commitMetadata)
      throws CommitFailedException {
    checkArgument(
        commitMetadata.getVersion() > 0, "Can only write staged commit files for versions > 0");
    try {
      // Do not use Put-If-Absent for staged commit files since we assume that UUID-based
      // commit files are globally unique, and so we will never have concurrent writers
      // attempting to write the same commit file.
      return writeDeltaFile(
          engine,
          finalizedActions,
          commitMetadata.generateNewStagedCommitFilePath(),
          true /* overwrite */);
    } catch (IOException ex) {
      throw new CommitFailedException(
          true /* retryable */,
          false /* conflict */,
          "Failed to write staged commit file due to: " + ex.getMessage(),
          ex);
    }
  }

  private FileStatus writePublishedDeltaFileForVersion0(
      Engine engine, CloseableIterator<Row> finalizedActions, CommitMetadata commitMetadata)
      throws CommitFailedException {
    checkArgument(
        commitMetadata.getVersion() == 0,
        "Expected version 0, but got %s",
        commitMetadata.getVersion());
    try {
      return writeDeltaFile(
          engine,
          finalizedActions,
          commitMetadata.getPublishedDeltaFilePath(),
          false /* overwrite */);
    } catch (FileAlreadyExistsException ex) {
      logger.warn(
          "[{}] File already exists for version 0, treating as successful commit: {}",
          ucTableId,
          commitMetadata.getPublishedDeltaFilePath());
      // This can happen if a previous commit attempt from this writer succeeded in writing 00.json,
      // but the client was not successfully notified of that write (e.g. due to network
      // issues).
      //
      // Since we know we are conflicting with our previous write, we can safely return a successful
      // response.
      return FileStatus.of(commitMetadata.getPublishedDeltaFilePath());
    } catch (IOException ex) {
      throw new CommitFailedException(
          true /* retryable */,
          false /* conflict */,
          "Failed to write published delta file due to: " + ex.getMessage(),
          ex);
    }
  }

  private FileStatus writeDeltaFile(
      Engine engine, CloseableIterator<Row> finalizedActions, String filePath, boolean overwrite)
      throws IOException {
    return timeCheckedOperation(
        logger,
        "Write file: " + filePath,
        ucTableId,
        () -> {
          // Note: the engine is responsible for closing the actions iterator once it has been
          //       fully consumed.
          engine.getJsonHandler().writeJsonFileAtomically(filePath, finalizedActions, overwrite);

          // TODO: [delta-io/delta#5021] Use FileSystemClient::getFileStatus API instead
          return FileStatus.of(filePath);
        });
  }

  private void commitToUC(CommitMetadata commitMetadata, FileStatus kernelStagedCommitFileStatus)
      throws CommitFailedException {
    timeCheckedOperation(
        logger,
        "Commit staged commit file to UC: " + kernelStagedCommitFileStatus.getPath(),
        ucTableId,
        () -> {
          // commitToUc is only for normal catalog WRITES, not for CREATE, or UPGRADE, or DOWNGRADE,
          // or anything filesystem related.
          checkState(
              commitMetadata.getCommitType() == CommitMetadata.CommitType.CATALOG_WRITE,
              "Only supported commit type is CATALOG_WRITE, but got: %s");

          try {
            ucClient.commit(
                ucTableId,
                tablePath.toUri(),
                Optional.of(getUcCommitPayload(commitMetadata, kernelStagedCommitFileStatus)),
                Optional.empty() /* lastKnownBackfilledVersion */, // TODO: take this in as a hint
                false /* isDisown */,
                Optional.empty() /* newMetadata */, // TODO: support sending newMetadata
                Optional.empty()); /* newProtocol */ // TODO: support sending newProtocol
            return null;
          } catch (io.delta.storage.commit.CommitFailedException cfe) {
            throw storageCFEtoKernelCFE(cfe);
          } catch (IOException ex) {
            throw new CommitFailedException(
                true /* retryable */, false /* conflict */, ex.getMessage(), ex);
          } catch (UCCommitCoordinatorException ucce) {
            // For now, this catches all UC exceptions such as:
            // - CommitLimitReachedException -> TODO: publish in this case
            // - InvalidTargetTableException
            // - UpgradeNotAllowedException
            // We can add specific catch statements for these exceptions if needed in the future.
            throw new CommitFailedException(
                false /* retryable */, false /* conflict */, ucce.getMessage(), ucce);
          }
        });
  }

  private Commit getUcCommitPayload(
      CommitMetadata commitMetadata, FileStatus kernelStagedCommitFileStatus) {
    return new Commit(
        commitMetadata.getVersion(),
        kernelFileStatusToHadoopFileStatus(kernelStagedCommitFileStatus),
        // commitMetadata validates that the ICT is present if writing to a catalogManaged table
        commitMetadata.getCommitInfo().getInCommitTimestamp().get());
  }

  @VisibleForTesting
  public static org.apache.hadoop.fs.FileStatus kernelFileStatusToHadoopFileStatus(
      io.delta.kernel.utils.FileStatus kernelFileStatus) {
    return new org.apache.hadoop.fs.FileStatus(
        kernelFileStatus.getSize() /* length */,
        false /* isDirectory */,
        1 /* blockReplication */,
        128 * 1024 * 1024 /* blockSize (128MB) */,
        kernelFileStatus.getModificationTime() /* modificationTime */,
        kernelFileStatus.getModificationTime() /* accessTime */,
        org.apache.hadoop.fs.permission.FsPermission.getFileDefault() /* permission */,
        "unknown" /* owner */,
        "unknown" /* group */,
        new org.apache.hadoop.fs.Path(kernelFileStatus.getPath()) /* path */);
  }

  private static CommitFailedException storageCFEtoKernelCFE(
      io.delta.storage.commit.CommitFailedException storageCFE) {
    return new CommitFailedException(
        storageCFE.getRetryable(),
        storageCFE.getConflict(),
        storageCFE.getMessage(),
        storageCFE.getCause());
  }
}
