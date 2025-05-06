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

package io.delta.kernel.internal.checkpoints;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.checkpoints.CheckpointInstance.CheckpointFormat;
import io.delta.kernel.utils.FileStatus;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CompleteCheckpointGroup {
  public static Optional<CompleteCheckpointGroup> getLatestCompleteCheckpointGroup(
      List<CheckpointInstance> checkpoints, CheckpointInstance notLaterThan) {
    return checkpoints.stream()
        .filter(c -> c.isNotLaterThan(notLaterThan))
        .collect(Collectors.groupingBy(c -> c))
        .entrySet()
        .stream()
        .map(
            entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), tryCreateFrom(entry.getValue())))
        .filter(entry -> entry.getValue().isPresent())
        .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().get()))
        .max(Map.Entry.comparingByKey())
        .map(Map.Entry::getValue);
  }

  public static Optional<CompleteCheckpointGroup> tryCreateFrom(
      List<CheckpointInstance> checkpoints) {
    try {
      return Optional.of(new CompleteCheckpointGroup(checkpoints));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }

  public final long version;
  public final CheckpointInstance.CheckpointFormat format;
  public final List<CheckpointInstance> checkpoints;

  public CompleteCheckpointGroup(List<CheckpointInstance> checkpoints) {
    ///////////////////////
    // Input validations //
    ///////////////////////

    requireNonNull(checkpoints, "checkpoints is null");
    checkArgument(!checkpoints.isEmpty(), "checkpoints cannot be empty");

    final CheckpointInstance first = checkpoints.get(0);

    checkpoints.forEach(
        x -> {
          checkArgument(x.version == first.version, "All checkpoints must have the same version");
          checkArgument(x.format == first.format, "All checkpoints must have the same format");
          checkArgument(x.fileStatus.isPresent(), "All checkpoints must have a file status");

          if (x.format == CheckpointFormat.MULTI_PART) {
            checkArgument(x.partNum.isPresent(), "All multi-part checkpoints must have partNum");
            checkArgument(x.numParts.isPresent(), "All multi-part checkpoints must have numParts");
            checkArgument(
                x.numParts.get().equals(first.numParts.get()),
                "All multi-part checkpoints must have the same numParts");
          }
        });

    if (first.format == CheckpointFormat.CLASSIC || first.format == CheckpointFormat.V2) {
      checkArgument(
          checkpoints.size() == 1,
          "Classic and V2 checkpoint group must have exactly one checkpoint");
    } else if (first.format == CheckpointFormat.MULTI_PART) {
      validateMultiPartVersions(checkpoints);
    } else {
      throw new IllegalArgumentException("Unknown checkpoint format: " + first.format);
    }

    ////////////////////////////////
    // Member variable assignment //
    ////////////////////////////////

    this.version = first.version;
    this.format = first.format;
    this.checkpoints = checkpoints;
  }

  public List<FileStatus> getFileStatuses() {
    return checkpoints.stream().map(x -> x.fileStatus.get()).collect(Collectors.toList());
  }

  /** Requires: All input checkpoints are valid multi-part checkpoints. */
  @VisibleForTesting
  public static void validateMultiPartVersions(List<CheckpointInstance> checkpoints) {
    final int expectedNumParts = checkpoints.get(0).numParts.get();

    checkArgument(checkpoints.size() == expectedNumParts);

    final Set<Integer> actualPartNums =
        checkpoints.stream().map(x -> x.partNum.get()).collect(Collectors.toSet());

    final Set<Integer> expectedPartNums =
        IntStream.rangeClosed(1, expectedNumParts).boxed().collect(Collectors.toSet());

    checkArgument(
        actualPartNums.equals(expectedPartNums),
        "Expected part numbers %s, but got %s",
        expectedPartNums,
        actualPartNums);
  }
}
