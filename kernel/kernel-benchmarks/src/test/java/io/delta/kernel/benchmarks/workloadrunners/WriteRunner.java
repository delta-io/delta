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

package io.delta.kernel.benchmarks.workloadrunners;

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import io.delta.kernel.*;
import io.delta.kernel.benchmarks.models.WorkloadSpec;
import io.delta.kernel.benchmarks.models.WriteSpec;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.kernel.utils.FileStatus;
import java.io.FileNotFoundException;
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
  private final List<List<DataFileStatus>> commitContents;
  private Snapshot snapshot;
  private Optional<Set<String>> initialDeltaLogFiles = Optional.empty();

  /**
   * Constructs the WriteRunner from the workload spec and engine.
   *
   * @param workloadSpec The write workload specification.
   * @param engine The engine to use for executing the workload.
   */
  public WriteRunner(WriteSpec workloadSpec, Engine engine) {
    this.workloadSpec = workloadSpec;
    this.engine = engine;
    this.commitContents = new ArrayList<>();
  }

  @Override
  public void setup() throws Exception {
    String tableRoot = workloadSpec.getTableInfo().getResolvedTableRoot();

    // Capture initial listing of delta log files. This is used during cleanup to revert changes.
    if (!initialDeltaLogFiles.isPresent()) {
      initialDeltaLogFiles = Optional.of(captureFileListing());
    }

    // Load the initial snapshot of the table. This will be used as the starting point for commits
    // and will be updated after each commit using the post-commit snapshot.
    snapshot = loadSnapshot(engine, workloadSpec.getTableInfo(), Optional.empty());

    if (commitContents.isEmpty()) {
      for (WriteSpec.CommitSpec commitSpec : workloadSpec.getCommits()) {
        commitContents.add(
            commitSpec.readDataFiles(workloadSpec.getSpecDirectoryPath(), snapshot.getSchema()));
      }
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
    for (List<DataFileStatus> actions : commitContents) {
      UpdateTableTransactionBuilder txnBuilder =
          snapshot.buildUpdateTableTransaction("Delta-Kernel-Benchmarks", Operation.WRITE);

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
      snapshot =
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
    if (!initialDeltaLogFiles.isPresent()) {
      throw new RuntimeException("Cannot cleanup before setup is called.");
    }
    // Delete any files that weren't present initially
    Set<String> currentFiles = captureFileListing();
    Set<String> initialFiles =
        initialDeltaLogFiles.orElseThrow(
            () -> new RuntimeException("Cannot cleanup before setup is called."));
    for (String filePath : currentFiles) {
      if (!initialFiles.contains(filePath)) {
        engine.getFileSystemClient().delete(filePath);
      }
    }
  }

  /**
   * @return all file paths in the `_delta_log/` directory (including `_staged_commits/` if Unity
   *     Catalog managed and `_sidecars/` if it exists)
   */
  private Set<String> captureFileListing() throws IOException {
    List<String> prefixes =
        new ArrayList<>(
            Arrays.asList("_delta_log", "_delta_log/_sidecars", "_delta_log/_staged_commits"));

    Set<String> files = new HashSet<>();
    for (String prefix : prefixes) {
      // Construct path prefix for all files in `_delta_log/`. The prefix is for file with name `0`
      // because the filesystem client lists all _sibling_ files in the directory with a path
      // greater than `0`.
      String deltaLogPathPrefix =
          new Path(workloadSpec.getTableInfo().getResolvedTableRoot(), new Path(prefix, "0"))
              .toUri()
              .getPath();

      // List from the lowest version in the prefix
      try (CloseableIterator<FileStatus> filesIter =
          engine.getFileSystemClient().listFrom(deltaLogPathPrefix)) {
        while (filesIter.hasNext()) {
          files.add(filesIter.next().getPath());
        }
      } catch (FileNotFoundException e) {
        // Ignore if the directory does not exist
      }
    }
    return files;
  }
}
