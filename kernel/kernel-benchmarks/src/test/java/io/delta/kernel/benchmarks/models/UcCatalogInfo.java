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

package io.delta.kernel.benchmarks.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.commit.Commit;
import io.delta.unity.InMemoryUCClient;
import io.delta.unity.UCCatalogManagedCommitter;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import scala.collection.mutable.ArrayBuffer;

/**
 * Configuration for Unity catalog managed tables used in benchmarks.
 *
 * <p>Contains information about staged commits in the `_delta_log/_staged_commits/` directory.
 *
 * <p>Example JSON structure:
 *
 * <pre>{@code
 * {
 *   "uc_table_id": "12345678-1234-1234-1234-123456789abc",
 *   "max_ratified_version": 2,
 *   "log_tail": [
 *     {
 *       "staged_commit_file_name": "00000000000000000002.a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d.json"
 *     }
 *   ]
 * }
 * }</pre>
 */
public class UcCatalogInfo {

  /**
   * A single staged commit in the log tail.
   *
   * <p>Each staged commit is a JSON file in `_delta_log/_staged_commits/` containing Delta log
   * actions.
   */
  public static class StagedCommit {
    /**
     * The filename of the staged commit, relative to `_delta_log/_staged_commits/`.
     *
     * <p>Example: "00000000000000000002.a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d.json"
     */
    @JsonProperty("staged_commit_file_name")
    private String stagedCommitFileName;

    /** Default constructor for Jackson deserialization. */
    public StagedCommit() {}

    /**
     * Constructor for creating a StagedCommit.
     *
     * @param stagedCommitFileName the filename of the staged commit
     */
    public StagedCommit(String stagedCommitFileName) {
      this.stagedCommitFileName = stagedCommitFileName;
    }

    /**
     * Gets the version number from the filename.
     *
     * @return the version number
     */
    @JsonIgnore
    public long getVersion() {
      return FileNames.deltaVersion(stagedCommitFileName);
    }

    /** @return the filename of the staged commit */
    public String getStagedCommitFileName() {
      return stagedCommitFileName;
    }

    /**
     * Gets the full path to the staged commit file.
     *
     * @param tableRoot the root path of the Delta table
     * @return the full path
     */
    public String getFullPath(String tableRoot) {
      Path logPath = new Path(tableRoot, "_delta_log");
      Path stagedCommitsDir = new Path(logPath, FileNames.STAGED_COMMIT_DIRECTORY);
      return new Path(stagedCommitsDir, stagedCommitFileName).toUri().toString();
    }

    @Override
    public String toString() {
      return String.format(
          "StagedCommit{version=%d, stagedCommitFileName='%s'}",
          getVersion(), stagedCommitFileName);
    }

    public Commit toCommit(Engine engine, String tableRoot) throws IOException {
      String stagedCommitPath = getFullPath(tableRoot);
      FileStatus fileStatus = engine.getFileSystemClient().getFileStatus(stagedCommitPath);

      // While catalog managed tables expect to use inCommitTimestamp, we use
      // modification time here for simplicity.
      long commitTimestamp = fileStatus.getModificationTime();

      org.apache.hadoop.fs.FileStatus hadoopFileStatus =
          UCCatalogManagedCommitter.kernelFileStatusToHadoopFileStatus(fileStatus);
      return new Commit(getVersion(), hadoopFileStatus, commitTimestamp);
    }
  }

  /**
   * The list of staged commits for this table.
   *
   * <p>These commits are not yet backfilled to the regular Delta log.
   */
  @JsonProperty(value = "log_tail", required = true)
  private List<StagedCommit> logTail;

  /** The Unity Catalog table ID. */
  @JsonProperty(value = "uc_table_id", required = true)
  private String ucTableId;

  /** The maximum ratified version for this table in Unity Catalog. */
  @JsonProperty(value = "max_ratified_version", required = true)
  private long maxRatifiedVersion;

  /** Default constructor for Jackson deserialization. */
  public UcCatalogInfo() {}

  /**
   * Creates a UcCatalogInfo with the given staged commits.
   *
   * @param logTail the list of staged commits
   */
  public UcCatalogInfo(List<StagedCommit> logTail) {
    this.logTail = logTail;
  }

  /** @return the list of staged commits, or an empty list if none */
  public List<StagedCommit> getLogTail() {
    return logTail;
  }

  /** @return the Unity Catalog table ID */
  public String getUcTableId() {
    return ucTableId;
  }

  /**
   * Creates an InMemoryUCClient for this table with the staged commits pre-loaded.
   *
   * @param engine the Engine to use for reading staged commit files
   * @param tableRoot the root path of the Delta table
   * @return an initialized InMemoryUCClient
   * @throws IOException if there's an error reading staged commit files
   */
  public InMemoryUCClient createUCClient(Engine engine, String tableRoot) throws IOException {
    ArrayBuffer<Commit> commits = new ArrayBuffer<>();
    if (!logTail.isEmpty()) {
      // Commits must be added to TableData in order of version. We sort staged commits by version.
      List<StagedCommit> sortedLogTail =
          logTail.stream()
              .sorted(Comparator.comparingLong(StagedCommit::getVersion))
              .collect(Collectors.toList());

      for (StagedCommit stagedCommit : sortedLogTail) {
        commits.addOne(stagedCommit.toCommit(engine, tableRoot));
      }
    }

    InMemoryUCClient ucClient = new InMemoryUCClient("benchmark-metastore");
    InMemoryUCClient.TableData tableData =
        new InMemoryUCClient.TableData(maxRatifiedVersion, commits);
    ucClient.createTableIfNotExistsOrThrow(ucTableId, tableData);

    return ucClient;
  }

  /**
   * Loads a UcCatalogInfo from a JSON file.
   *
   * @param jsonPath the path to the JSON file
   * @return the parsed UcCatalogInfo
   * @throws IOException if there is an error reading the file
   */
  public static UcCatalogInfo fromJsonPath(String jsonPath) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(new File(jsonPath), UcCatalogInfo.class);
  }

  @Override
  public String toString() {
    return String.format("UcCatalogInfo{logTail=%s}", logTail);
  }
}
