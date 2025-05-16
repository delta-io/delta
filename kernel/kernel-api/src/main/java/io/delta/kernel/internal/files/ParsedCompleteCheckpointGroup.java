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
import static java.util.Objects.requireNonNull;

import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.files.ParsedLogFile.CheckpointFile;
import io.delta.kernel.internal.files.ParsedLogFile.ClassicCheckpointFile;
import io.delta.kernel.internal.files.ParsedLogFile.MultipartCheckpointFile;
import io.delta.kernel.internal.files.ParsedLogFile.V2CheckpointFile;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ParsedCompleteCheckpointGroup {
  ///////////////////////////////////////
  // Static enums, fields, and methods //
  ///////////////////////////////////////

  public static Optional<ParsedCompleteCheckpointGroup> tryCreateFrom(
      List<CheckpointFile> checkpointFiles) {
    requireNonNull(checkpointFiles, "checkpointFiles is null");

    try {
      return Optional.of(new ParsedCompleteCheckpointGroup(checkpointFiles));
    } catch (IllegalArgumentException | ClassCastException e) {
      return Optional.empty();
    }
  }

  ///////////////////////////////
  // Member fields and methods //
  ///////////////////////////////

  private final long version;
  private final List<CheckpointFile> checkpointFiles;

  private ParsedCompleteCheckpointGroup(List<CheckpointFile> checkpointFiles) {
    ///////////////////////
    // Input validations //
    ///////////////////////

    requireNonNull(checkpointFiles, "checkpointFiles is null");
    checkArgument(!checkpointFiles.isEmpty(), "checkpointFiles cannot be empty");

    final CheckpointFile first = checkpointFiles.get(0);

    if (first instanceof ClassicCheckpointFile || first instanceof V2CheckpointFile) {
      checkArgument(
          checkpointFiles.size() == 1,
          "Classic and V2 checkpoint group must have exactly one checkpoint file");
    } else if (first instanceof MultipartCheckpointFile) {
      validateMultiPartCheckpoints(checkpointFiles);
    } else {
      throw new IllegalArgumentException("Unknown checkpoint format: " + first);
    }

    ////////////////////////////////
    // Member variable assignment //
    ////////////////////////////////

    this.version = first.getVersion();
    this.checkpointFiles = checkpointFiles;
  }

  public long getVersion() {
    return version;
  }

  public List<CheckpointFile> getCheckpointFiles() {
    return checkpointFiles;
  }

  /** Assumes: At the very least, the first checkpoint file is a multi-part checkpoint. */
  @VisibleForTesting
  public static void validateMultiPartCheckpoints(List<CheckpointFile> checkpointFiles) {
    final Set<Integer> actualPartNums = new HashSet<>();

    if (!(checkpointFiles.get(0) instanceof MultipartCheckpointFile)) {
      throw new IllegalArgumentException(
          "Expected first checkpoint file to be a multi-part checkpoint file");
    }

    final MultipartCheckpointFile first = (MultipartCheckpointFile) checkpointFiles.get(0);

    checkArgument(
        first.getNumParts() == checkpointFiles.size(),
        "A multi-part checkpoint group must have the same number of parts as the number of files");

    checkpointFiles.forEach(
        x -> {
          if (!(x instanceof MultipartCheckpointFile)) {
            throw new IllegalArgumentException(
                "Expected all checkpoint files to be multi-part checkpoint files");
          }

          final MultipartCheckpointFile multiPartCheckpoint = (MultipartCheckpointFile) x;

          checkArgument(
              multiPartCheckpoint.getVersion() == first.getVersion(),
              "All multi-part checkpoints must have the same version");

          checkArgument(
              multiPartCheckpoint.getNumParts() == first.getNumParts(),
              "All multi-part checkpoints must have the same numParts");

          actualPartNums.add(multiPartCheckpoint.getPart());
        });

    final Set<Integer> expectedPartNums =
        IntStream.rangeClosed(1, first.getNumParts()).boxed().collect(Collectors.toSet());

    checkArgument(
        actualPartNums.equals(expectedPartNums),
        "Expected part numbers %s, but got %s",
        expectedPartNums,
        actualPartNums);
  }
}
