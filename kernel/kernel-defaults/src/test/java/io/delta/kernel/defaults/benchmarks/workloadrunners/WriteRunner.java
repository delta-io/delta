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
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.benchmarks.models.WorkloadSpec;
import io.delta.kernel.defaults.benchmarks.models.WriteSpec;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.kernel.utils.FileStatus;
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
  private List<List<DataFileStatus>> commit_contents;
  private Snapshot currentSnapshot;
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
    this.commit_contents = new ArrayList<>();
  }

  @Override
  public void setup() throws Exception {
    String tableRoot = workloadSpec.getTableInfo().getResolvedTableRoot();

    // Get the current snapshot
    SnapshotBuilder builder = TableManager.loadSnapshot(tableRoot);
    currentSnapshot = builder.build(engine);

    // Capture initial listing of delta log files. This is used during cleanup to revert changes.
    initialDeltaLogFiles = captureFileListing();

    // Load and parse all commit files
    this.commit_contents.clear();
    for (WriteSpec.CommitSpec commitSpec : workloadSpec.getCommits()) {
      commit_contents.add(
          commitSpec.readDataFiles(
              workloadSpec.getSpecDirectoryPath(), currentSnapshot.getSchema()));
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
    // Execute all commits in sequence
    for (List<DataFileStatus> actions : commit_contents) {
      UpdateTableTransactionBuilder txnBuilder =
          currentSnapshot.buildUpdateTableTransaction("Delta-Kernel-Benchmarks", Operation.WRITE);

      Transaction txn = txnBuilder.build(engine);
      Row txnState = txn.getTransactionState(engine);
      DataWriteContext writeContext =
          Transaction.getWriteContext(engine, txnState, new HashMap<>() /* partitionValues */);
      CloseableIterator<Row> add_actions =
          Transaction.generateAppendActions(
              engine, txnState, toCloseableIterator(actions.iterator()), writeContext);
      TransactionCommitResult result =
          txn.commit(engine, CloseableIterable.inMemoryIterable(add_actions));

      long version = result.getVersion();
      blackhole.consume(version);

      // Use the post-commit snapshot for the next transaction
      // Post-commit snapshot should always be present unless there was a conflict
      currentSnapshot =
          result
              .getPostCommitSnapshot()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Post-commit snapshot not available. This indicates a conflict "
                              + "occurred during the benchmark, which should not happen. "
                              + "Ensure no other processes are writing to the table: "
                              + workloadSpec.getTableInfo().getResolvedTableRoot()));
    }
  }

  /** Cleans up the state created during benchmark execution by reverting all committed changes. */
  @Override
  public void cleanup() throws Exception {
    // Delete any files that weren't present initially
    Set<String> currentFiles = captureFileListing();
    for (String filePath : currentFiles) {
      if (!initialDeltaLogFiles.contains(filePath)) {
        engine.getFileSystemClient().delete(filePath);
      }
    }
  }

  /** @return a set of all file paths in the the `_delta_log/` directory of the table. */
  private Set<String> captureFileListing() throws IOException {
    // Construct path prefix for all files in `_delta_log/`. The prefix is for file with name `0`
    // because the filesystem client lists all _sibling_ files in the directory with a path greater
    // than `0`.
    String deltaLogPathPrefix =
        new Path(workloadSpec.getTableInfo().getResolvedTableRoot(), "_delta_log/0")
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
