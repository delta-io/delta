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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import java.util.List;

/** Metadata required for publishing catalog commits to the Delta log. */
public class PublishMetadata {

  private final long snapshotVersion;
  private final String logPath;
  private final List<ParsedCatalogCommitData> ascendingCatalogCommits;

  public PublishMetadata(
      long snapshotVersion, String logPath, List<ParsedCatalogCommitData> ascendingCatalogCommits) {
    this.snapshotVersion = snapshotVersion;
    this.logPath = requireNonNull(logPath, "logPath is null");
    this.ascendingCatalogCommits =
        requireNonNull(ascendingCatalogCommits, "ascendingCatalogCommits is null");
  }

  /** @return the snapshot version up to which all catalog commits must be published */
  public long getSnapshotVersion() {
    return snapshotVersion;
  }

  /** @return the path to the Delta log directory */
  public String getLogPath() {
    return logPath;
  }

  /** @return the list of catalog commits to be published, in ascending order of version number */
  public List<ParsedCatalogCommitData> getAscendingCatalogCommits() {
    return ascendingCatalogCommits;
  }
}
