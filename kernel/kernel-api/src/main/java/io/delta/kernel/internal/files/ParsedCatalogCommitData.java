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

package io.delta.kernel.internal.files;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.FileStatus;
import java.net.URI;
import java.util.Optional;

/**
 * A catalog commit represents an atomic change to a table, as ratified by the catalog.
 *
 * <p>Can be staged and written to a staged commit file, like so: {@code
 * _delta_log/_staged_commits/00000000000000000001.uuid-1234.json}.
 *
 * <p>Can also be a commit the catalog reports after it has been published, like so: {@code
 * _delta_log/00000000000000000001.json}. See {@link #forCatalogFileStatus}.
 *
 * <p>Can also be inline.
 */
public final class ParsedCatalogCommitData extends ParsedDeltaData {

  /**
   * Creates catalog commit data for a staged commit file.
   *
   * <p>Callers converting commits returned by a catalog should use {@link #forCatalogFileStatus},
   * which also accepts commits the catalog reports after they have been published.
   *
   * @throws IllegalArgumentException if the file is not a staged commit file
   */
  public static ParsedCatalogCommitData forFileStatus(FileStatus fileStatus) {
    checkArgument(
        FileNames.isStagedDeltaFile(fileStatus.getPath()),
        "Expected a staged commit file but got %s",
        fileStatus.getPath());

    final String path = fileStatus.getPath();
    final long version = FileNames.deltaVersion(path);
    return new ParsedCatalogCommitData(version, Optional.of(fileStatus), Optional.empty());
  }

  /**
   * Creates catalog commit data for a commit file returned by a catalog, in either physical form:
   *
   * <ul>
   *   <li>staged: {@code <logPath>/_staged_commits/<version>.<uuid>.json}
   *   <li>published: {@code <logPath>/<version>.json}
   * </ul>
   *
   * <p>The Delta protocol allows a catalog to return commits that have already been published, so
   * both forms are valid catalog input.
   *
   * @param logPath the table's {@code _delta_log} directory
   * @param expectedVersion the commit version reported by the catalog
   * @param fileStatus the commit file
   * @throws IllegalArgumentException if the file is not a staged commit file directly under {@code
   *     <logPath>/_staged_commits} or a published Delta file directly under {@code logPath}, or if
   *     its version does not equal {@code expectedVersion}
   */
  public static ParsedCatalogCommitData forCatalogFileStatus(
      String logPath, long expectedVersion, FileStatus fileStatus) {
    final Path filePath = new Path(fileStatus.getPath());
    final Path expectedParent;
    if (FileNames.isPublishedDeltaFile(filePath.toString())) {
      checkArgument(
          !filePath.getParent().getName().equals(FileNames.STAGED_COMMIT_DIRECTORY),
          "Expected a staged commit file but got a published commit file name under %s: %s",
          FileNames.STAGED_COMMIT_DIRECTORY,
          filePath);
      expectedParent = new Path(logPath);
    } else if (FileNames.isCommitFile(filePath.toString())) {
      // A commit file that is not published has the staged form: <version>.<uuid>.json
      expectedParent = new Path(FileNames.stagedCommitDirectory(new Path(logPath)));
    } else {
      throw new IllegalArgumentException("Expected a catalog commit file but got " + filePath);
    }

    checkArgument(
        isSameDirectory(filePath.getParent(), expectedParent),
        "Expected catalog commit file %s to be located in %s",
        filePath,
        expectedParent);

    final long version = FileNames.deltaVersion(filePath);
    checkArgument(
        version == expectedVersion,
        "Catalog commit file %s has version %s but the catalog reported version %s",
        filePath,
        version,
        expectedVersion);

    return new ParsedCatalogCommitData(version, Optional.of(fileStatus), Optional.empty());
  }

  public static ParsedCatalogCommitData forInlineData(long version, ColumnarBatch inlineData) {
    return new ParsedCatalogCommitData(version, Optional.empty(), Optional.of(inlineData));
  }

  private ParsedCatalogCommitData(
      long version, Optional<FileStatus> fileStatusOpt, Optional<ColumnarBatch> inlineDataOpt) {
    super(version, fileStatusOpt, inlineDataOpt);
  }

  @Override
  public Class<? extends ParsedLogData> getGroupByCategoryClass() {
    return ParsedCatalogCommitData.class;
  }

  /**
   * Compares two directories by path, and by scheme and authority only when both sides specify
   * them. Catalogs may return fully qualified file paths for a table whose path is unqualified.
   */
  private static boolean isSameDirectory(Path actual, Path expected) {
    final URI actualUri = actual.toUri();
    final URI expectedUri = expected.toUri();
    if (actualUri.getScheme() != null
        && expectedUri.getScheme() != null
        && !actualUri.getScheme().equalsIgnoreCase(expectedUri.getScheme())) {
      return false;
    }
    if (actualUri.getAuthority() != null
        && expectedUri.getAuthority() != null
        && !actualUri.getAuthority().equals(expectedUri.getAuthority())) {
      return false;
    }
    return actualUri.getPath().equals(expectedUri.getPath());
  }

  // TODO: String getPublishedDeltaFilePath(); Requires forInlineData to take in logPath
}
