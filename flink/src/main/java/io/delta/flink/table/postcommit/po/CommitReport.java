/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.table.postcommit.po;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.List;
import java.util.Objects;

/**
 * Commit metrics payload matching the server-side {@code CommitReport} schema.
 *
 * <p>The {@code @JsonTypeInfo(WRAPPER_OBJECT)} + {@code @JsonTypeName("commit_report")} annotations
 * cause Jackson to serialize this as {@code {"commit_report": { ... }}}, matching the server's
 * expected envelope format.
 *
 * <p>All fields are optional ({@code NON_NULL} inclusion) so that only populated fields appear in
 * the JSON.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonTypeName("commit_report")
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CommitReport {

  @JsonProperty("num_files_added")
  private Long numFilesAdded;

  @JsonProperty("num_files_removed")
  private Long numFilesRemoved;

  @JsonProperty("num_bytes_added")
  private Long numBytesAdded;

  @JsonProperty("num_bytes_removed")
  private Long numBytesRemoved;

  @JsonProperty("num_clustered_bytes_added")
  private Long numClusteredBytesAdded;

  @JsonProperty("num_clustered_bytes_removed")
  private Long numClusteredBytesRemoved;

  @JsonProperty("file_size_histogram")
  private FileSizeHistogram fileSizeHistogram;

  @JsonProperty("num_rows_inserted")
  private Long numRowsInserted;

  @JsonProperty("num_rows_removed")
  private Long numRowsRemoved;

  @JsonProperty("num_rows_updated")
  private Long numRowsUpdated;

  public CommitReport() {}

  // -- accessors & fluent setters --

  public Long getNumFilesAdded() {
    return numFilesAdded;
  }

  public CommitReport numFilesAdded(Long numFilesAdded) {
    this.numFilesAdded = numFilesAdded;
    return this;
  }

  public Long getNumFilesRemoved() {
    return numFilesRemoved;
  }

  public CommitReport numFilesRemoved(Long numFilesRemoved) {
    this.numFilesRemoved = numFilesRemoved;
    return this;
  }

  public Long getNumBytesAdded() {
    return numBytesAdded;
  }

  public CommitReport numBytesAdded(Long numBytesAdded) {
    this.numBytesAdded = numBytesAdded;
    return this;
  }

  public Long getNumBytesRemoved() {
    return numBytesRemoved;
  }

  public CommitReport numBytesRemoved(Long numBytesRemoved) {
    this.numBytesRemoved = numBytesRemoved;
    return this;
  }

  public Long getNumClusteredBytesAdded() {
    return numClusteredBytesAdded;
  }

  public CommitReport numClusteredBytesAdded(Long numClusteredBytesAdded) {
    this.numClusteredBytesAdded = numClusteredBytesAdded;
    return this;
  }

  public Long getNumClusteredBytesRemoved() {
    return numClusteredBytesRemoved;
  }

  public CommitReport numClusteredBytesRemoved(Long numClusteredBytesRemoved) {
    this.numClusteredBytesRemoved = numClusteredBytesRemoved;
    return this;
  }

  public FileSizeHistogram getFileSizeHistogram() {
    return fileSizeHistogram;
  }

  public CommitReport fileSizeHistogram(FileSizeHistogram fileSizeHistogram) {
    this.fileSizeHistogram = fileSizeHistogram;
    return this;
  }

  public Long getNumRowsInserted() {
    return numRowsInserted;
  }

  public CommitReport numRowsInserted(Long numRowsInserted) {
    this.numRowsInserted = numRowsInserted;
    return this;
  }

  public Long getNumRowsRemoved() {
    return numRowsRemoved;
  }

  public CommitReport numRowsRemoved(Long numRowsRemoved) {
    this.numRowsRemoved = numRowsRemoved;
    return this;
  }

  public Long getNumRowsUpdated() {
    return numRowsUpdated;
  }

  public CommitReport numRowsUpdated(Long numRowsUpdated) {
    this.numRowsUpdated = numRowsUpdated;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CommitReport)) return false;
    CommitReport that = (CommitReport) o;
    return Objects.equals(numFilesAdded, that.numFilesAdded)
        && Objects.equals(numFilesRemoved, that.numFilesRemoved)
        && Objects.equals(numBytesAdded, that.numBytesAdded)
        && Objects.equals(numBytesRemoved, that.numBytesRemoved)
        && Objects.equals(numClusteredBytesAdded, that.numClusteredBytesAdded)
        && Objects.equals(numClusteredBytesRemoved, that.numClusteredBytesRemoved)
        && Objects.equals(fileSizeHistogram, that.fileSizeHistogram)
        && Objects.equals(numRowsInserted, that.numRowsInserted)
        && Objects.equals(numRowsRemoved, that.numRowsRemoved)
        && Objects.equals(numRowsUpdated, that.numRowsUpdated);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        numFilesAdded,
        numFilesRemoved,
        numBytesAdded,
        numBytesRemoved,
        numClusteredBytesAdded,
        numClusteredBytesRemoved,
        fileSizeHistogram,
        numRowsInserted,
        numRowsRemoved,
        numRowsUpdated);
  }

  // ---------------------------------------------------------------------------
  //  FileSizeHistogram
  // ---------------------------------------------------------------------------

  /** Histogram of file sizes in a commit, matching the server-side schema. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class FileSizeHistogram {

    @JsonProperty("sorted_bin_boundaries")
    private List<Long> sortedBinBoundaries;

    @JsonProperty("file_counts")
    private List<Long> fileCounts;

    @JsonProperty("total_bytes")
    private List<Long> totalBytes;

    @JsonProperty("commit_version")
    private Long commitVersion;

    public FileSizeHistogram() {}

    public List<Long> getSortedBinBoundaries() {
      return sortedBinBoundaries;
    }

    public FileSizeHistogram sortedBinBoundaries(List<Long> sortedBinBoundaries) {
      this.sortedBinBoundaries = sortedBinBoundaries;
      return this;
    }

    public List<Long> getFileCounts() {
      return fileCounts;
    }

    public FileSizeHistogram fileCounts(List<Long> fileCounts) {
      this.fileCounts = fileCounts;
      return this;
    }

    public List<Long> getTotalBytes() {
      return totalBytes;
    }

    public FileSizeHistogram totalBytes(List<Long> totalBytes) {
      this.totalBytes = totalBytes;
      return this;
    }

    public Long getCommitVersion() {
      return commitVersion;
    }

    public FileSizeHistogram commitVersion(Long commitVersion) {
      this.commitVersion = commitVersion;
      return this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof FileSizeHistogram)) return false;
      FileSizeHistogram that = (FileSizeHistogram) o;
      return Objects.equals(sortedBinBoundaries, that.sortedBinBoundaries)
          && Objects.equals(fileCounts, that.fileCounts)
          && Objects.equals(totalBytes, that.totalBytes)
          && Objects.equals(commitVersion, that.commitVersion);
    }

    @Override
    public int hashCode() {
      return Objects.hash(sortedBinBoundaries, fileCounts, totalBytes, commitVersion);
    }
  }
}
