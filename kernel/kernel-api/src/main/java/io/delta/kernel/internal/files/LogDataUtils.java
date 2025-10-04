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

import io.delta.kernel.internal.lang.ListUtils;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class LogDataUtils {

  private LogDataUtils() {}

  public static void validateLogDataContainsOnlyRatifiedStagedCommits(
      List<? extends ParsedLogData> logDatas) {
    for (ParsedLogData logData : logDatas) {
      checkArgument(
          logData instanceof ParsedCatalogCommitData && logData.isFile(),
          "Only staged ratified commits are supported, but found: " + logData);
    }
  }

  public static void validateLogDataIsSortedContiguous(List<? extends ParsedLogData> logDatas) {
    if (logDatas.size() > 1) {
      for (int i = 1; i < logDatas.size(); i++) {
        final ParsedLogData prev = logDatas.get(i - 1);
        final ParsedLogData curr = logDatas.get(i);
        checkArgument(
            prev.getVersion() + 1 == curr.getVersion(),
            String.format(
                "Log data must be sorted and contiguous, but found: %s and %s", prev, curr));
      }
    }
  }

  /**
   * Combines a list of published Deltas and ratified Deltas into a single list of Deltas such that
   * there is exactly one {@link ParsedDeltaData} per version. When there is both a published Delta
   * and a ratified staged Delta for the same version, prioritizes the ratified Delta.
   *
   * <p>The method requires but does not validate the following:
   *
   * <ul>
   *   <li>{@code publishedDeltas} are sorted and contiguous
   *   <li>{@code ratifiedDeltas} are sorted and contiguous
   *   <li>the commit versions present in {@code publishedDeltas} and {@code ratifiedDeltas}, when
   *       combined, reflect a contiguous version range. In other words, if the two do not overlap,
   *       publishedDeltas.last = ratifiedDeltas.first + 1).
   * </ul>
   */
  public static List<ParsedDeltaData> combinePublishedAndRatifiedDeltasWithCatalogPriority(
      List<ParsedDeltaData> publishedDeltas, List<ParsedDeltaData> ratifiedDeltas) {
    if (ratifiedDeltas.isEmpty()) {
      return publishedDeltas;
    }

    if (publishedDeltas.isEmpty()) {
      return ratifiedDeltas;
    }

    final long firstRatified = ratifiedDeltas.get(0).getVersion();
    final long lastRatified = ListUtils.getLast(ratifiedDeltas).getVersion();

    return Stream.of(
            publishedDeltas.stream().filter(x -> x.getVersion() < firstRatified),
            ratifiedDeltas.stream(),
            publishedDeltas.stream().filter(x -> x.getVersion() > lastRatified))
        .flatMap(Function.identity())
        .collect(Collectors.toList());
  }
}
