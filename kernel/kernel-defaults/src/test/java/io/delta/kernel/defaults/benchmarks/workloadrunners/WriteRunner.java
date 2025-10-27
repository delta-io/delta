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

package io.delta.kernel.defaults.benchmarks.workloadrunners;

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import io.delta.kernel.*;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.benchmarks.models.WorkloadSpec;
import io.delta.kernel.defaults.benchmarks.models.WriteSpec;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.File;
import java.io.IOException;
import java.util.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A WorkloadRunner that executes write workloads as benchmarks. This runner performs one or more
 * commits to a Delta table and measures the performance of those commits.
 *
 * <p>The runner executes commits specified in the {@link WriteSpec}, where each commit contains a
 * set of Delta log actions (add/remove files) defined in external JSON files.
 *
 * <p>If run as a benchmark using {@link #executeAsBenchmark(Blackhole)}, this measures the time to
 * execute all commits. Setup (loading commit files) and cleanup (reverting changes) are not
 * included in the benchmark timing.
 */
public class WriteRunner extends WorkloadRunner {
  private final Engine engine;
  private final WriteSpec workloadSpec;
  private List<List<Row>> commitActions;
  private List<Long> committedVersions;
  private Snapshot currentSnapshot;
  private long originalVersion;
  private Set<String> initialDeltaLogFiles;

  /**
   * Constructs the WriteRunner from the workload spec and engine.
   *
   * @param workloadSpec The write workload specification.
   * @param engine The engine to use for executing the workload.
   */
  public WriteRunner(WriteSpec workloadSpec, Engine engine) {
    this.workloadSpec = workloadSpec;
    this.engine = engine;
    this.committedVersions = new ArrayList<>();
  }

  @Override
  public void setup() throws Exception {
    commitActions = new ArrayList<>();

    String tableRoot = workloadSpec.getTableInfo().getResolvedTableRoot();

    // Get the current snapshot
    SnapshotBuilder builder = TableManager.loadSnapshot(tableRoot);
    currentSnapshot = builder.build(engine);

    // Capture initial listing of delta log files
    initialDeltaLogFiles = captureFileListing();

    // Load and parse all commit files
    for (WriteSpec.CommitSpec commitSpec : workloadSpec.getCommits()) {
      String commitFilePath =
          workloadSpec.getTableInfo().getTableInfoPath()
              + "/specs/"
              + workloadSpec.getCaseName()
              + "/"
              + commitSpec.getDataFilesPath();
      List<Row> actions = parseCommitFile(commitFilePath);
      commitActions.add(actions);
    }
  }

  /** @return the name of this workload. */
  @Override
  public String getName() {
    return "write";
  }

  /** @return The workload specification used to create this runner. */
  @Override
  public WorkloadSpec getWorkloadSpec() {
    return workloadSpec;
  }

  /**
   * Executes the write workload as a benchmark, consuming results via the provided Blackhole.
   *
   * <p>This method executes all commits specified in the workload spec in sequence. The timing
   * includes only the commit execution, not the setup or cleanup. We reuse the post-commit snapshot
   * from each transaction to avoid reloading from disk, which makes the benchmark more efficient
   * and realistic.
   *
   * @param blackhole The Blackhole to consume results and avoid dead code elimination.
   */
  @Override
  public void executeAsBenchmark(Blackhole blackhole) throws Exception {
    // Execute all commits in sequence (timed)
    for (List<Row> actions : commitActions) {
      // Create transaction from table (first iteration) or from post-commit snapshot (subsequent)
      UpdateTableTransactionBuilder txnBuilder =
          currentSnapshot.buildUpdateTableTransaction("Delta-Kernel-Benchmarks", Operation.WRITE);

      // Build and commit the transaction
      Transaction txn = txnBuilder.build(engine);

      // Convert actions list to CloseableIterable
      CloseableIterator<Row> actionsIter = toCloseableIterator(actions.iterator());
      io.delta.kernel.utils.CloseableIterable<Row> dataActions =
          io.delta.kernel.utils.CloseableIterable.inMemoryIterable(actionsIter);

      TransactionCommitResult result = txn.commit(engine, dataActions);

      long version = result.getVersion();
      committedVersions.add(version);
      blackhole.consume(version);

      // Use the post-commit snapshot for the next transaction
      // Post-commit snapshot should always be present unless there was a conflict
      currentSnapshot =
          result
              .getPostCommitSnapshot()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Post-commit snapshot not available. This indicates a conflict occurred during "
                              + "the benchmark, which should not happen. Ensure no other processes are writing "
                              + "to the table: "
                              + workloadSpec.getTableInfo().getResolvedTableRoot()));
    }
  }

  /**
   * Parses a Delta log JSON file and returns a list of action Rows.
   *
   * <p>Uses Kernel's built-in JsonHandler to read Delta log format JSON files.
   *
   * @param commitFilePath path to the commit file containing Delta log JSON actions
   * @return list of Row objects representing the actions (AddFile, RemoveFile, etc.)
   * @throws IOException if there's an error reading or parsing the file
   */
  private List<Row> parseCommitFile(String commitFilePath) throws IOException {
    /** Schema for reading commit files with both "add" and "remove" actions. */
    final StructType COMMIT_FILE_SCHEMA =
        new StructType()
            .add("add", AddFile.FULL_SCHEMA, true /* nullable */)
            .add("remove", RemoveFile.FULL_SCHEMA, true /* nullable */);

    List<Row> actions = new ArrayList<>();

    File file = new File(commitFilePath);
    if (!file.exists()) {
      throw new IOException("Commit file not found: " + commitFilePath);
    }

    // Create a FileStatus for the commit file
    FileStatus fileStatus = FileStatus.of(commitFilePath, file.length(), file.lastModified());

    // Use Kernel's JsonHandler to read the file
    try (CloseableIterator<FileStatus> fileIter =
            toCloseableIterator(Collections.singletonList(fileStatus).iterator());
        CloseableIterator<ColumnarBatch> batchIter =
            engine.getJsonHandler().readJsonFiles(fileIter, COMMIT_FILE_SCHEMA, Optional.empty())) {

      while (batchIter.hasNext()) {
        ColumnarBatch batch = batchIter.next();

        // Process each row in the batch
        try (CloseableIterator<Row> rowIter = batch.getRows()) {
          while (rowIter.hasNext()) {
            Row singleActionRow = rowIter.next();

            // Extract the actual action Row and wrap it in SingleAction format
            // Check if this row has an "add" action
            if (!singleActionRow.isNullAt(COMMIT_FILE_SCHEMA.indexOf("add"))) {
              Row addRow = singleActionRow.getStruct(COMMIT_FILE_SCHEMA.indexOf("add"));
              // Wrap in SingleAction format for commit
              actions.add(SingleAction.createAddFileSingleAction(addRow));
            }
            // Check if this row has a "remove" action
            else if (!singleActionRow.isNullAt(COMMIT_FILE_SCHEMA.indexOf("remove"))) {
              Row removeRow = singleActionRow.getStruct(COMMIT_FILE_SCHEMA.indexOf("remove"));
              // Wrap in SingleAction format for commit
              actions.add(SingleAction.createRemoveFileSingleAction(removeRow));
            } else {
              // Throw an error if the action is not recognized (not "add" or "remove")
              throw new IOException(
                  "Unrecognized action in commit file row: " + singleActionRow.toString());
            }
          }
        }
      }
    }

    return actions;
  }

  /**
   * Cleans up the state created during benchmark execution by reverting all committed changes.
   */
  @Override
  public void cleanup() throws Exception {
    // Delete any files that weren't present initially
    Set<String> currentFiles = captureFileListing();
    for (String filePath : currentFiles) {
      if (!initialDeltaLogFiles.contains(filePath)) {
        engine.getFileSystemClient().delete(filePath);
      }
    }

    committedVersions.clear();
  }

  /**
   * @return a set of all file paths in the the `_delta_log/` directory of the table.
   */
  private Set<String> captureFileListing() throws IOException {
    // Construct path prefix for all files in `_delta_log/`. The prefix is for file with name `0`
    // because the filesystem client lists all _sibling_ files in the directory with a path greater
    // than `0`.
    String deltaLogPathPrefix =
        new io.delta.kernel.internal.fs.Path(
                workloadSpec.getTableInfo().getResolvedTableRoot(), "_delta_log/0")
            .toUri()
            .getPath();

    Set<String> files = new HashSet<>();
    try (CloseableIterator<FileStatus> filesIter =
        engine.getFileSystemClient().listFrom(deltaLogPathPrefix)) {
      while (filesIter.hasNext()) {
        FileStatus file = filesIter.next();
        files.add(file.getPath());
      }
    }
    return files;
  }
}
