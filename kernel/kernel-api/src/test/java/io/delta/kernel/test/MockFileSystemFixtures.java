/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.kernel.test;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Java counterpart of the {@code MockFileSystemClientUtils} Scala trait, for use by JUnit 5 tests.
 *
 * <p>Only the members the Java suites need are ported: the fake table paths, the file-status
 * builders, and the {@code listFrom} mock engine. The checkpoint and ICT helpers are left in Scala
 * until a Java suite needs them.
 */
public final class MockFileSystemFixtures {

  private MockFileSystemFixtures() {}

  public static final Path DATA_PATH = new Path("/fake/path/to/table/");
  public static final Path LOG_PATH = new Path(DATA_PATH, "_delta_log");

  /** The table path as the builder APIs take it. */
  public static String dataPathString() {
    return DATA_PATH.toString();
  }

  public static ParsedLogData parsedRatifiedStagedCommit(long version) {
    return ParsedLogData.forFileStatus(stagedCommitFile(version));
  }

  public static List<ParsedLogData> parsedRatifiedStagedCommits(List<Long> versions) {
    return versions.stream()
        .map(MockFileSystemFixtures::parsedRatifiedStagedCommit)
        .collect(Collectors.toList());
  }

  /** Staged commit file status where the timestamp = 10*version. */
  public static FileStatus stagedCommitFile(long v) {
    return FileStatus.of(FileNames.stagedCommitFile(LOG_PATH, v), v, v * 10);
  }

  /** Delta file status where the timestamp = 10*version. */
  public static FileStatus deltaFileStatus(long v) {
    return deltaFileStatus(v, LOG_PATH);
  }

  public static FileStatus deltaFileStatus(long v, Path path) {
    return FileStatus.of(FileNames.deltaFile(path, v), v, v * 10);
  }

  /** Delta file statuses where the timestamp = 10*version. */
  public static List<FileStatus> deltaFileStatuses(List<Long> deltaVersions) {
    if (deltaVersions.size() != deltaVersions.stream().distinct().count()) {
      throw new IllegalArgumentException("delta versions must be distinct: " + deltaVersions);
    }
    return deltaVersions.stream()
        .map(MockFileSystemFixtures::deltaFileStatus)
        .collect(Collectors.toList());
  }

  /** Compaction file status where the timestamp = 10*startVersion. */
  public static FileStatus logCompactionStatus(long start, long end) {
    return FileStatus.of(
        FileNames.logCompactionPath(LOG_PATH, start, end).toString(), start, start * 10);
  }

  /** Checksum file status for a given version. */
  public static FileStatus checksumFileStatus(long deltaVersion) {
    return FileStatus.of(FileNames.checksumFile(LOG_PATH, deltaVersion).toString(), 10, 10);
  }

  /** Classic checkpoint file status where the timestamp = 10*version. */
  public static FileStatus classicCheckpointFileStatus(long v) {
    return FileStatus.of(FileNames.checkpointFileSingular(LOG_PATH, v).toString(), v, v * 10);
  }

  /**
   * Mirrors {@code MockFileSystemClientUtils.listFromProvider}: the entries in the same directory
   * as {@code filePath} that sort at or after it.
   *
   * <p>Listing nested directories is not supported, matching the Scala helper.
   */
  public static List<FileStatus> listFrom(List<FileStatus> files, String filePath) {
    Path parentPath = new Path(filePath).getParent();
    return files.stream()
        .filter(fs -> Objects.equals(new Path(fs.getPath()).getParent(), parentPath))
        .filter(fs -> fs.getPath().compareTo(filePath) >= 0)
        .sorted(Comparator.comparing(FileStatus::getPath))
        .collect(Collectors.toList());
  }

  /**
   * Mirrors {@code createMockFSListFromEngine}: an {@link Engine} answering {@code listFrom} from
   * the given contents, with {@code resolvePath} as the identity function.
   */
  public static Engine mockFSListFromEngine(List<FileStatus> contents) {
    List<FileStatus> snapshot = new ArrayList<>(contents);
    return mockFSListFromEngine(filePath -> listFrom(snapshot, filePath));
  }

  public static Engine mockFSListFromEngine(Function<String, List<FileStatus>> listFromProvider) {
    return MockEngineFixtures.mockEngine(
        new MockEngineFixtures.BaseMockFileSystemClient() {
          @Override
          public CloseableIterator<FileStatus> listFrom(String filePath) {
            return MockEngineFixtures.toCloseableIterator(
                listFromProvider.apply(filePath).iterator());
          }

          @Override
          public String resolvePath(String path) {
            return path;
          }
        },
        null,
        null,
        null);
  }
}
