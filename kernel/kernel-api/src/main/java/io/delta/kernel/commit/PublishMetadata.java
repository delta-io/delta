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

package io.delta.kernel.commit;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.annotation.Experimental;
import io.delta.kernel.internal.files.LogDataUtils;
import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import io.delta.kernel.internal.lang.ListUtils;
import java.util.List;

/** Metadata required for publishing catalog commits to the Delta log. */
@Experimental
public class PublishMetadata {

  private final long snapshotVersion;
  private final String logPath;

  /**
   * List of contiguous catalog commits to be published, in ascending order of version number.
   *
   * <p>Must be non-empty and must end with a catalog commit whose version matches {@code
   * snapshotVersion}.
   */
  private final List<ParsedCatalogCommitData> ascendingCatalogCommits;

  public PublishMetadata(
      long snapshotVersion, String logPath, List<ParsedCatalogCommitData> ascendingCatalogCommits) {
    this.snapshotVersion = snapshotVersion;
    this.logPath = requireNonNull(logPath, "logPath is null");
    this.ascendingCatalogCommits =
        requireNonNull(ascendingCatalogCommits, "ascendingCatalogCommits is null");

    validateCommitsNonEmpty();
    validateCommitsContiguous();
    validateLastCommitMatchesSnapshotVersion();
  }

  /** @return the snapshot version up to which all catalog commits must be published */
  public long getSnapshotVersion() {
    return snapshotVersion;
  }

  /** @return the path to the Delta log directory, located at {@code <table_root>/_delta_log} */
  public String getLogPath() {
    return logPath;
  }

  /**
   * @return the list of contiguous catalog commits to be published, in ascending order of version
   *     number
   */
  public List<ParsedCatalogCommitData> getAscendingCatalogCommits() {
    return ascendingCatalogCommits;
  }

  private void validateCommitsNonEmpty() {
    checkArgument(!ascendingCatalogCommits.isEmpty(), "ascendingCatalogCommits must be non-empty");
  }

  private void validateCommitsContiguous() {
    LogDataUtils.validateLogDataIsSortedContiguous(ascendingCatalogCommits);
  }

  private void validateLastCommitMatchesSnapshotVersion() {
    final long lastCommitVersion = ListUtils.getLast(ascendingCatalogCommits).getVersion();

    checkArgument(
        lastCommitVersion == snapshotVersion,
        "Last catalog commit version %d must equal snapshot version %d",
        lastCommitVersion,
        snapshotVersion);
  }
}
