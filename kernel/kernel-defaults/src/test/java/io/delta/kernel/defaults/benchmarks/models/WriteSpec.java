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

package io.delta.kernel.defaults.benchmarks.models;

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.benchmarks.workloadrunners.WorkloadRunner;
import io.delta.kernel.defaults.benchmarks.workloadrunners.WriteRunner;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Workload specification for write benchmarks. Defines test cases for writing to Delta tables with
 * one or more commits containing add/remove actions.
 *
 * <h2>Usage</h2>
 *
 * <p>To run this workload, use {@link WorkloadSpec#getRunner(Engine)} to get the appropriate {@link
 * WriteRunner}.
 *
 * @see WriteRunner
 */
public class WriteSpec extends WorkloadSpec {

  /**
   * Container for a single commit's configuration.
   *
   * <p>Each commit references a file containing the Delta log JSON actions (add/remove files) to be
   * committed.
   */
  public static class CommitSpec {
    /**
     * Path to the commit file containing Delta log JSON actions. The path is relative to the spec
     * directory (where spec.json is located).
     *
     * <p>Example: "commit_a.json"
     */
    @JsonProperty("data_files_path")
    private String dataFilesPath;

    /** Schema for reading commit files with both "add" and "remove" actions. */
    @JsonIgnore
    private static final StructType COMMIT_SPEC_SCHEMA =
        new StructType()
            .add("add", AddFile.FULL_SCHEMA, true /* nullable */)
            .add("remove", RemoveFile.FULL_SCHEMA, true /* nullable */);

    /** Default constructor for Jackson. */
    public CommitSpec() {}

    public CommitSpec(String dataFilesPath) {
      this.dataFilesPath = dataFilesPath;
    }

    public String getDataFilesPath() {
      return dataFilesPath;
    }

    /**
     * Uses Kernel's built-in JsonHandler to read commit spec containing data files.
     *
     * @param engine the Engine instance to use for reading JSON files
     * @param specPath the base path where the commit file is located
     * @throws IOException if there's an error reading or parsing the file
     */
    public List<Row> parseActions(Engine engine, String specPath) throws IOException {
      String commitFilePath = new Path(specPath, getDataFilesPath()).toString();

      System.out.println("CommitSpec: Reading commit file at " + commitFilePath);
      File file = new File(commitFilePath);
      if (!file.exists()) {
        throw new IOException("Commit file not found: " + commitFilePath);
      }

      // Create a FileStatus for the commit file
      FileStatus fileStatus = FileStatus.of(commitFilePath, file.length(), file.lastModified());

      // Use Kernel's JsonHandler to read the file and collect actions
      List<Row> actions = new ArrayList<>();
      try (CloseableIterator<FileStatus> fileIter =
              toCloseableIterator(Collections.singletonList(fileStatus).iterator());
          CloseableIterator<ColumnarBatch> batchIter =
              engine
                  .getJsonHandler()
                  .readJsonFiles(fileIter, COMMIT_SPEC_SCHEMA, Optional.empty())) {

        while (batchIter.hasNext()) {
          ColumnarBatch batch = batchIter.next();

          // Process each row in the batch
          try (CloseableIterator<Row> rowIter = batch.getRows()) {
            while (rowIter.hasNext()) {
              Row singleActionRow = rowIter.next();

              // Extract the actual action Row and wrap it in SingleAction format
              // Check if this row has an "add" action
              if (!singleActionRow.isNullAt(COMMIT_SPEC_SCHEMA.indexOf("add"))) {
                Row addRow = singleActionRow.getStruct(COMMIT_SPEC_SCHEMA.indexOf("add"));
                // Wrap in SingleAction format for commit
                actions.add(SingleAction.createAddFileSingleAction(addRow));
              }
              // Check if this row has a "remove" action
              else if (!singleActionRow.isNullAt(COMMIT_SPEC_SCHEMA.indexOf("remove"))) {
                Row removeRow = singleActionRow.getStruct(COMMIT_SPEC_SCHEMA.indexOf("remove"));
                // Wrap in SingleAction format for commit
                actions.add(SingleAction.createRemoveFileSingleAction(removeRow));
              } else {
                // Throw an error if the action is not recognized (not "add" or "remove")
                throw new IOException("Unrecognized action in commit file row: " + singleActionRow);
              }
            }
          }
        }
      }
      return actions;
    }
  }

  /**
   * List of commits to execute in sequence. Each commit contains a reference to a file with Delta
   * log JSON actions. All commits are executed as part of the timed benchmark.
   */
  @JsonProperty("commits")
  private List<CommitSpec> commits;

  // Default constructor for Jackson
  public WriteSpec() {
    super("write");
  }

  /**
   * Gets the list of commits to execute.
   *
   * @return list of commit specifications
   */
  public List<CommitSpec> getCommits() {
    return commits != null ? commits : Collections.emptyList();
  }

  /** @return the full name of this workload, derived from table name, case name, and type. */
  @Override
  public String getFullName() {
    return this.tableInfo.name + "/" + caseName + "/write";
  }

  @Override
  public WorkloadRunner getRunner(Engine engine) {
    return new WriteRunner(this, engine);
  }

  /**
   * Generates workload variants from this test case specification.
   *
   * <p>Currently, WriteSpec generates a single variant (itself). In the future, this could be
   * extended to generate variants for different write patterns or configurations.
   *
   * @return list of WriteSpec variants, each representing a separate workload execution
   */
  @Override
  public List<WorkloadSpec> getWorkloadVariants() {
    return Collections.singletonList(this);
  }

  @Override
  public String toString() {
    return String.format(
        "Write{caseName='%s', commits=%d, tableInfo='%s'}",
        caseName, getCommits().size(), tableInfo);
  }
}
