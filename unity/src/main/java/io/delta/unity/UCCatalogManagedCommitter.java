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
import static java.util.Objects.requireNonNull;

import io.delta.kernel.commit.CommitFailedException;
import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.commit.CommitResponse;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorException;
import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UCCatalogManagedCommitter implements Committer {
  private static final Logger logger = LoggerFactory.getLogger(UCCatalogManagedCommitter.class);

  private final UCClient ucClient;
  private final String ucTableId;
  private final Path tablePath;

  public UCCatalogManagedCommitter(UCClient ucClient, String ucTableId, String tablePath) {
    this.ucClient = ucClient;
    this.ucTableId = ucTableId;
    this.tablePath = new Path(tablePath);
  }

  @Override
  public CommitResponse commit(
      Engine engine, CloseableIterator<Row> finalizedActions, CommitMetadata commitMetadata)
      throws CommitFailedException {
    requireNonNull(engine, "engine is null");
    requireNonNull(finalizedActions, "finalizedActions is null");
    requireNonNull(commitMetadata, "commitMetadata is null");
    validateCommitMetadataLogPath(commitMetadata);

    final CommitMetadata.CommitType commitType = commitMetadata.getCommitType();

    if (commitType != CommitMetadata.CommitType.CATALOG_WRITE) {
      // TODO: Support other scenarios in the future
      throw new UnsupportedOperationException("Unsupported commit type: " + commitType);
    }

    // TODO: validate matching ucTableId?

    // TODO: lastKnownBackfilledVersion?

    final FileStatus kernelFileStatus =
        writeStagedCommitFile(engine, finalizedActions, commitMetadata);

    commitToUC(commitMetadata, kernelFileStatus);

    return new CommitResponse(ParsedLogData.forFileStatus(kernelFileStatus));
  }

  ////////////////////
  // Helper methods //
  ////////////////////

  private String normalize(Path path) {
    return path.toUri().normalize().toString();
  }

  private void validateCommitMetadataLogPath(CommitMetadata cm) {
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
    final String stagedCommitFilePath = commitMetadata.getNewStagedCommitFilePath();

    return OperationTimer.timeOperation(
        logger,
        "Write staged commit file",
        ucTableId,
        () -> {
          try {
            // Do not use Put-If-Absent for staged commit files since we assume that UUID-based
            // commit files are globally unique, and so we will never have concurrent writers
            // attempting to write the same commit file.

            engine
                .getJsonHandler()
                .writeJsonFileAtomically(
                    stagedCommitFilePath, finalizedActions, true /* overwrite */);

            // TODO: [delta-io/delta#5021] Use FileSystemClient::getFileStatus API instead
            return FileStatus.of(stagedCommitFilePath);
          } catch (IOException ex) {
            throw new CommitFailedException(
                true /* retryable */,
                false /* conflict */,
                "Failed to write staged commit file due to: " + ex.getMessage(),
                ex);
          }
        });
  }

  private void commitToUC(CommitMetadata commitMetadata, FileStatus kernelFileStatus)
      throws CommitFailedException {
    OperationTimer.timeOperation(
        logger,
        "Commit to UC",
        ucTableId,
        () -> {
          final boolean isDisown =
              commitMetadata.getCommitType()
                  == CommitMetadata.CommitType.CATALOG_DOWNGRADE_TO_FILESYSTEM;

          try {
            ucClient.commit(
                ucTableId,
                tablePath.toUri(),
                Optional.of(getCommitStructPayload(commitMetadata, kernelFileStatus)),
                Optional.empty() /* lastKnownBackfilledVersion */,
                isDisown,
                Optional.empty() /* newMetadata */,
                Optional.empty());
            return null;
          } catch (io.delta.storage.commit.CommitFailedException cfe) {
            throw storageCFEtoKernelCFE(cfe);
          } catch (IOException ex) {
            throw new CommitFailedException(
                true /* retryable */, false /* conflict */, ex.getMessage(), ex);
          } catch (UCCommitCoordinatorException ucce) {
            throw new CommitFailedException(
                false /* retryable */, false /* conflict */, ucce.getMessage(), ucce);
          }
        });
  }

  private Commit getCommitStructPayload(
      CommitMetadata commitMetadata, FileStatus kernelFileStatus) {
    return new Commit(
        commitMetadata.getVersion(),
        kernelFileStatusToHadoopFileStatus(kernelFileStatus),
        // commitMetadata validates the ICT is present if writing to a catalogManaged table
        commitMetadata.getCommitInfo().getInCommitTimestamp().get());
  }

  private static org.apache.hadoop.fs.FileStatus kernelFileStatusToHadoopFileStatus(
      io.delta.kernel.utils.FileStatus kernelFS) {
    return new org.apache.hadoop.fs.FileStatus(
        kernelFS.getSize() /* length */,
        false /* isDirectory */,
        1 /* blockReplication */,
        128 * 1024 * 1024 /* blockSize (128MB) */,
        kernelFS.getModificationTime() /* modificationTime */,
        kernelFS.getModificationTime() /* accessTime */,
        org.apache.hadoop.fs.permission.FsPermission.getFileDefault() /* permission */,
        "user" /* owner */,
        "group" /* group */,
        new org.apache.hadoop.fs.Path(kernelFS.getPath()) /* path */);
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
