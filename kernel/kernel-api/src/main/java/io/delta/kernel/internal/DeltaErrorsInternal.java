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
}
