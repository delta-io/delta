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

package io.delta.kernel.defaults.benchmarks;

import io.delta.kernel.commit.CommitFailedException;
import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.commit.CommitResponse;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.benchmarks.models.CCv2Info;
import io.delta.kernel.defaults.benchmarks.models.TableInfo;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.spark.sql.delta.coordinatedcommits.InMemoryUCClient;
import org.apache.spark.sql.delta.coordinatedcommits.InMemoryUCCommitCoordinator;
import scala.Option;

/**
 * This class encapsulates all the infrastructure needed to work with CCv2 tables. It's created from
 * a {@link TableInfo} that has CCv2 configuration.
 *
 * <p>The context pre-populates the commit coordinator with staged commits from the CCv2Info
 * configuration, allowing benchmarks to read from and write to CCv2 tables.
 */
public class CCv2Context {

  private final BenchmarkingCCv2Committer committer;
  private final List<ParsedLogData> parsedLogData;

  /**
   * Private constructor. Use {@link #createFromTableInfo(TableInfo, Engine)} to create instances.
   */
  private CCv2Context(BenchmarkingCCv2Committer committer, List<ParsedLogData> parsedLogData) {
    this.committer = committer;
    this.parsedLogData = parsedLogData;
  }

  /** @return the committer for CCv2 commits */
  public Committer getCommitter() {
    return committer;
  }

  /** @return the list of parsed log data (staged commits) for SnapshotBuilder */
  public List<ParsedLogData> getParsedLogData() {
    return parsedLogData;
  }

  /**
   * Creates a CCv2Context from a TableInfo that has CCv2 configuration.
   *
   * @param tableInfo the TableInfo containing CCv2 configuration
   * @param engine the Engine to use for filesystem operations
   * @return a CCv2Context ready for use
   * @throws IllegalArgumentException if the TableInfo is not a CCv2 table
   * @throws RuntimeException if there's an error setting up the CCv2 infrastructure
   */
  public static CCv2Context createFromTableInfo(TableInfo tableInfo, Engine engine) {
    if (!tableInfo.isCCv2Enabled()) {
      throw new IllegalArgumentException("TableInfo does not have CCv2 configuration");
    }

    try {
      CCv2Info ccv2Info = tableInfo.getCCv2Info();
      String tableRoot = tableInfo.getResolvedTableRoot();

      // Since this isn't a real UC, generate dummy tableId and tableUri
      String tableId = UUID.randomUUID().toString();
      URI tableUri = Paths.get(tableRoot).toUri();

      // Create in-memory commit coordinator and pre-populate with staged commits from the
      // CCv2Info log_tail
      InMemoryUCCommitCoordinator coordinator = new InMemoryUCCommitCoordinator();
      List<ParsedLogData> parsedLogDataList = new ArrayList<>();
      for (CCv2Info.StagedCommit stagedCommit : ccv2Info.getLogTail()) {
        // Get file info for the staged commit using Engine's filesystem. We do this to
        // get accurate file size and modification time.
        String stagedCommitPath = stagedCommit.getFullPath(tableRoot);
        FileStatus fileStatus = engine.getFileSystemClient().getFileStatus(stagedCommitPath);

        coordinator.commitToCoordinator(
            tableId,
            tableUri,
            Option.apply(stagedCommitPath), // commitFileName (full path)
            Option.apply(stagedCommit.getVersion()), // commitVersion
            Option.apply(fileStatus.getSize()), // commitFileSize
            Option.apply(fileStatus.getModificationTime()), // commitFileModTime
            Option.apply(System.currentTimeMillis()), // commitTimestamp
            Option.empty(), // lastKnownBackfilledVersion
            false, // isDisownCommit
            Option.empty(), // protocolOpt
            Option.empty() // metadataOpt
            );

        parsedLogDataList.add(ParsedCatalogCommitData.forFileStatus(fileStatus));
      }

      // Create a committer using an in-memory UC client. We don't need real UC integration for
      // benchmarks.
      String metastoreId = "benchmark-metastore-" + tableId;
      InMemoryUCClient ucClient = new InMemoryUCClient(metastoreId, coordinator);
      BenchmarkingCCv2Committer committer =
          new BenchmarkingCCv2Committer(ucClient, tableId, tableUri, tableRoot);

      return new CCv2Context(committer, parsedLogDataList);

    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to create CCv2Context: I/O error reading staged commits", e);
    }
  }

  /**
   * Helper method to convert Kernel FileStatus to Hadoop FileStatus.
   *
   * @param kernelFileStatus Kernel FileStatus to convert
   * @return Hadoop FileStatus
   */
  private static org.apache.hadoop.fs.FileStatus kernelFileStatusToHadoopFileStatus(
      FileStatus kernelFileStatus) {
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

  /**
   * Committer implementation for CCv2 benchmarks.
   *
   * <p>This committer writes staged commits to the `_staged_commits/` directory and registers them
   * with the Unity Catalog coordinator.
   */
  static class BenchmarkingCCv2Committer implements Committer {
    private final UCClient ucClient;
    private final String tableId;
    private final URI tableUri;
    private final String tableRoot;

    BenchmarkingCCv2Committer(UCClient ucClient, String tableId, URI tableUri, String tableRoot) {
      this.ucClient = ucClient;
      this.tableId = tableId;
      this.tableUri = tableUri;
      this.tableRoot = tableRoot;
    }

    @Override
    public CommitResponse commit(
        Engine engine, CloseableIterator<Row> finalizedActions, CommitMetadata commitMetadata)
        throws CommitFailedException {

      long version = commitMetadata.getVersion();
      String stagedCommitsDir = Paths.get(tableRoot, "_delta_log", "_staged_commits").toString();

      String stagedCommitFileName = FileNames.stagedCommitFile(stagedCommitsDir, version);
      String stagedCommitPath = Paths.get(stagedCommitsDir, stagedCommitFileName).toString();

      try {
        // Write the staged commit file
        engine
            .getJsonHandler()
            .writeJsonFileAtomically(stagedCommitPath, finalizedActions, false /* overwrite */);

        FileStatus stagedFileStatus = engine.getFileSystemClient().getFileStatus(stagedCommitPath);
        Commit commit =
            new Commit(
                version,
                kernelFileStatusToHadoopFileStatus(stagedFileStatus),
                System.currentTimeMillis() // commitTimestamp
                );

        ucClient.commit(
            tableId,
            tableUri,
            Optional.of(commit),
            Optional.empty(), // lastKnownBackfilledVersion
            false, // disown
            Optional.empty(), // newMetadata
            Optional.empty() // newProtocol
            );

        // Return commit response with the staged commit file
        return new CommitResponse(ParsedCatalogCommitData.forFileStatus(stagedFileStatus));

      } catch (IOException e) {
        throw new CommitFailedException(
            true /* retryable */, false /* conflict */, "Failed to commit: " + e.getMessage(), e);
      } catch (Exception e) {
        throw new CommitFailedException(
            false /* retryable */, false /* conflict */, "Failed to commit: " + e.getMessage(), e);
      }
    }
  }
}
