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
package io.delta.kernel.internal;

import io.delta.kernel.exceptions.InvalidTableException;
import java.util.List;

/**
 * Contains methods to create developer-facing exceptions. See <a
 * href="https://github.com/delta-io/delta/blob/master/kernel/EXCEPTION_PRINCIPLES.md">Exception
 * Principles</a> for more information on what these are and how to use them.
 */
public class DeltaErrorsInternal {

  private DeltaErrorsInternal() {}

  public static IllegalStateException missingRemoveFileSizeDuringCommit() {
    return new IllegalStateException(
        "Kernel APIs for creating remove file rows require that "
            + "file size be provided but found null file size");
  }

  public static IllegalStateException invalidTimestampFormatForPartitionValue(
      String partitionValue) {
    return new IllegalStateException(
        String.format(
            "Invalid timestamp format for value: %s. Expected formats: "
                + "'yyyy-MM-dd HH:mm:ss[.SSSSSS]' or ISO-8601 (e.g. 2020-01-01T00:00:00Z)'",
            partitionValue));
  }

  public static UnsupportedOperationException defaultCommitterDoesNotSupportCatalogManagedTables() {
    return new UnsupportedOperationException(
        "No io.delta.kernel.commit.Committer has been provided to Kernel, so Kernel is using a "
            + "default Committer that only supports committing to filesystem-managed Delta tables, "
            + "not catalog-managed Delta tables. Since this table is catalog-managed, this "
            + "commit operation is unsupported.");
  }

  public static IllegalStateException logicalPhysicalSchemaMismatch(
      int num_partition_cols, int physical_size, int logical_size) {
    return new IllegalStateException(
        String.format(
            "The number of partition columns (%s) plus the physical schema size (%s) does not "
                + "equal the logical schema size (%s).",
            num_partition_cols, physical_size, logical_size));
  }

  public static InvalidTableException catalogCommitsPrecedePublishedDeltas(
      String tablePath, long earliestCatalogCommitVersion, List<Long> foundPublishedVersions) {
    return new InvalidTableException(
        tablePath,
        String.format(
            "Missing delta file: found staged ratified commit for version %s but no published "
                + "delta file. Found published deltas for versions: %s",
            earliestCatalogCommitVersion, foundPublishedVersions));
  }

  public static InvalidTableException publishedDeltasAndCatalogCommitsNotContiguous(
      String tablePath, List<Long> publishedDeltaVersions, List<Long> catalogCommitVersions) {
    return new InvalidTableException(
        tablePath,
        String.format(
            "Missing delta files: found published delta files for versions %s and staged "
                + "ratified commits for versions %s",
            publishedDeltaVersions, catalogCommitVersions));
  }

  public static InvalidTableException publishedDeltasNotContiguous(
      String tablePath, List<Long> foundVersions) {
    return new InvalidTableException(
        tablePath,
        String.format("Missing delta files: versions are not contiguous: (%s)", foundVersions));
  }
}
