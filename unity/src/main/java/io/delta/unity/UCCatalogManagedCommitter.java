package io.delta.unity;

import io.delta.kernel.commit.*;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UCCatalogManagedCommitter implements Committer {

  private static final Logger logger = LoggerFactory.getLogger(UCCatalogManagedCommitter.class);

  private final UCClient ucClient;
  private final String ucTableId;
  private final URI tablePathURI;

  public UCCatalogManagedCommitter(UCClient ucClient, String ucTableId, String tablePath) {
    this.ucClient = ucClient;
    this.ucTableId = ucTableId;
    this.tablePathURI = new Path(tablePath).toUri();
  }

  // TODO: client <-> server version compatibility check
  // TODO: Kernel should check we are using a catalog-managed-capable committer

  @Override
  public CommitResponse commit(
      Engine engine, CloseableIterator<Row> finalizedActions, CommitMetadata commitMetadata)
      throws CommitFailedException {
    logger.info(
        "[{}] Attempting to stage + commit to UC table at version {}",
        ucTableId,
        commitMetadata.getVersion());

    // TODO: log and enforce / do logic with the table type

    final String stagedCommitFilePath =
        CommitUtils.getStagedCommitFilePath(
            commitMetadata.getDeltaLogDirPath(), commitMetadata.getVersion());

    try {
      logger.info(
          "[{}] Attempting to write staged commit file {}", ucTableId, stagedCommitFilePath);
      engine
          .getJsonHandler()
          .writeJsonFileAtomically(stagedCommitFilePath, finalizedActions, false /* overwrite */);
      logger.info("[{}] Successfully wrote staged commit file {}", ucTableId, stagedCommitFilePath);
    } catch (IOException ex) {
      logger.error("[{}] Failed to write staged commit file {}", ucTableId, ex.getMessage(), ex);

      throw new CommitFailedException(
          true /* retryable */, false /* conflict */, "Failed to write staged commit file", ex);
    }

    final FileStatus kernelFileStatus;
    try {
      logger.info(
          "[{}] Attempting to get written file status for file {}",
          ucTableId,
          stagedCommitFilePath);
      kernelFileStatus = engine.getFileSystemClient().getFileStatus(stagedCommitFilePath);
      logger.info("[{}] Successfully got file status {}", ucTableId, kernelFileStatus);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    try {
      logger.info(
          "[{}] Attempting to issue commit to UC server at version {}",
          ucTableId,
          commitMetadata.getVersion());

      ucClient.commit(
          ucTableId,
          tablePathURI,
          Optional.of(
              new Commit(
                  commitMetadata.getVersion(),
                  kernelFileStatusToHadoopFileStatus(kernelFileStatus),
                  commitMetadata
                      .getCommitInfo()
                      .getInCommitTimestamp()
                      .orElse(commitMetadata.getCommitInfo().getTimestamp()))),
          Optional.empty(),
          false,
          Optional.empty(),
          Optional.empty());

      logger.info(
          "[{}] Successfully committed to UC table at version {}",
          ucTableId,
          commitMetadata.getVersion());

      return new CommitResponse(ParsedLogData.forFileStatus(kernelFileStatus));
    } catch (Exception e) {
      logger.error("[{}] Failed to commit to UC table: {}", ucTableId, e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  private static org.apache.hadoop.fs.FileStatus kernelFileStatusToHadoopFileStatus(
      io.delta.kernel.utils.FileStatus kernelFS) {
    return new org.apache.hadoop.fs.FileStatus(
        kernelFS.getSize(), // length
        false, // isDir (assuming file, not directory)
        1, // block replication (default)
        0, // block size (default)
        kernelFS.getModificationTime(), // modification time
        0, // access time (default)
        null, // permission (default)
        null, // owner (default)
        null, // group (default)
        new org.apache.hadoop.fs.Path(kernelFS.getPath()) // path
        );
  }
}
