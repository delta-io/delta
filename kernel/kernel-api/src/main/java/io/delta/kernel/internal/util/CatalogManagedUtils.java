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

package io.delta.kernel.internal.util;

import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.lang.ListUtils;
import io.delta.kernel.utils.FileStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CatalogManagedUtils {
  private CatalogManagedUtils() {}

  /**
   * Merges a List of `listedDeltas` FileStatuses with a List of `suffixCommits` ParsedLogDatas,
   * returning a List of FileStatuses.
   *
   * <p>Ensures that all of `listedDeltas` are retained and only appends suffix commits with
   * versions strictly greater than the last version in `listedDeltas`.
   *
   * <p>Assumes that both input lists are sorted by version in ascending order.
   */
  public static List<FileStatus> mergeListedDeltasAndSuffixDeltas(
      List<FileStatus> listedDeltas, List<ParsedLogData> suffixCommits) {
    if (listedDeltas.isEmpty()) {
      return suffixCommits.stream().map(ParsedLogData::getFileStatus).collect(Collectors.toList());
    }
    if (suffixCommits.isEmpty()) {
      return listedDeltas;
    }

    final long lastListedDeltaVersion =
        FileNames.deltaVersion(ListUtils.getLast(listedDeltas).getPath());

    final int firstSuffixIndexGreaterThanLastListedDelta =
        ListUtils.firstIndexWhere(suffixCommits, commit -> commit.version > lastListedDeltaVersion);

    if (firstSuffixIndexGreaterThanLastListedDelta == -1) {
      return listedDeltas;
    }

    final List<FileStatus> suffixCommitsFileStatuses =
        suffixCommits.subList(firstSuffixIndexGreaterThanLastListedDelta, suffixCommits.size())
            .stream()
            .map(ParsedLogData::getFileStatus)
            .collect(Collectors.toList());

    final List<FileStatus> output = new ArrayList<>(listedDeltas);
    output.addAll(suffixCommitsFileStatuses);

    // TODO: logging, refactor SnapshotManager::logDebugFileStatuses to common logging util

    return output;
  }
}
