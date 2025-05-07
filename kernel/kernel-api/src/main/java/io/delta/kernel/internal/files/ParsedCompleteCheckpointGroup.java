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
import io.delta.kernel.internal.files.ParsedLogFile.ParsedLogType;
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
      List<ParsedCheckpointFile> checkpointFiles) {
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
  private final ParsedLogType elementType;
  private final List<ParsedCheckpointFile> checkpointFiles;

  private ParsedCompleteCheckpointGroup(List<ParsedCheckpointFile> checkpointFiles) {
    ///////////////////////
    // Input validations //
    ///////////////////////

    requireNonNull(checkpointFiles, "checkpointFiles is null");
    checkArgument(!checkpointFiles.isEmpty(), "checkpointFiles cannot be empty");

    final ParsedCheckpointFile first = checkpointFiles.get(0);

    if (first.getType() == ParsedLogType.CHECKPOINT_CLASSIC
        || first.getType() == ParsedLogType.V2_CHECKPOINT_MANIFEST) {
      checkArgument(
          checkpointFiles.size() == 1,
          "Classic and V2 checkpoint group must have exactly one checkpoint file");
    } else if (first.getType() == ParsedLogType.MULTIPART_CHECKPOINT) {
      validateMultiPartVersions(checkpointFiles);
    } else {
      throw new IllegalArgumentException("Unknown checkpoint format: " + first.getType());
    }

    ////////////////////////////////
    // Member variable assignment //
    ////////////////////////////////

    this.version = first.getVersion();
    this.elementType = first.getType();
    this.checkpointFiles = checkpointFiles;
  }

  public long getVersion() {
    return version;
  }

  public ParsedLogType getElementType() {
    return elementType;
  }

  public List<ParsedCheckpointFile> getCheckpointFiles() {
    return checkpointFiles;
  }

  /** Assumes: only called with MULTIPART_CHECKPOINT files */
  @VisibleForTesting
  public static void validateMultiPartVersions(List<ParsedCheckpointFile> checkpointFiles) {
    final Set<Integer> actualPartNums = new HashSet<>();

    // We do NOT expect this to throw -- we expect all checkpointFiles to be MULTIPART_CHECKPOINT
    final ParsedMultiPartCheckpointFile first = castToMultiPartOrThrow(checkpointFiles.get(0));

    checkArgument(
        first.getNumParts() == checkpointFiles.size(),
        "A multi-part checkpoint group must have the same number of parts as the number of files");

    checkpointFiles.forEach(
        x -> {
          final ParsedMultiPartCheckpointFile multiPartCheckpoint = castToMultiPartOrThrow(x);

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

  private static ParsedMultiPartCheckpointFile castToMultiPartOrThrow(
      ParsedCheckpointFile checkpoint) {
    if (checkpoint instanceof ParsedMultiPartCheckpointFile) {
      return (ParsedMultiPartCheckpointFile) checkpoint;
    } else {
      throw new ClassCastException(
          "Cannot cast to ParsedMultiPartCheckpointFile: " + checkpoint.getType());
    }
  }
}
