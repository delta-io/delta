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
import io.delta.kernel.internal.files.ParsedLogData.ParsedLogType;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Represents a complete list of checkpoint "files" - the actual content of the checkpoints may be
 * materialized to disk (with a file status) or stored inline (as a columnar batch).
 *
 * <p>The question of determining if a checkpoint is "complete" is only applicable to multi-part
 * checkpoints. Classic checkpoints and V2 checkpoints are always "complete".
 *
 * <p>For example, a list of file statuses [001.001.003.checkpoint.parquet,
 * 001.002.003.checkpoint.parquet] represents an *incomplete* multi-part checkpoint, as part 003 is
 * missing.
 */
public class ParsedCompleteCheckpointGroup {
  ///////////////////////////////////////
  // Static enums, fields, and methods //
  ///////////////////////////////////////

  public static Optional<ParsedCompleteCheckpointGroup> tryCreateFrom(
      List<ParsedCheckpointData> checkpointDatas) {
    requireNonNull(checkpointDatas, "checkpointDatas is null");

    try {
      return Optional.of(new ParsedCompleteCheckpointGroup(checkpointDatas));
    } catch (IllegalArgumentException | ClassCastException e) {
      return Optional.empty();
    }
  }

  ///////////////////////////////
  // Member fields and methods //
  ///////////////////////////////

  public final long version;
  public final List<ParsedCheckpointData> checkpointDatas;

  private ParsedCompleteCheckpointGroup(List<ParsedCheckpointData> checkpointDatas) {
    ///////////////////////
    // Input validations //
    ///////////////////////

    requireNonNull(checkpointDatas, "checkpointFiles is null");
    checkArgument(!checkpointDatas.isEmpty(), "checkpointFiles cannot be empty");

    final ParsedCheckpointData first = checkpointDatas.get(0);

    if (first.type == ParsedLogType.CLASSIC_CHECKPOINT
        || first.type == ParsedLogType.V2_CHECKPOINT) {
      checkArgument(
          checkpointDatas.size() == 1,
          "Classic and V2 checkpoint group must have exactly one checkpoint file");
    } else if (first.type == ParsedLogType.MULTIPART_CHECKPOINT) {
      validateMultiPartCheckpoints(checkpointDatas);
    } else {
      throw new IllegalArgumentException("Unknown checkpoint format: " + first);
    }

    ////////////////////////////////
    // Member variable assignment //
    ////////////////////////////////

    this.version = first.version;
    this.checkpointDatas = checkpointDatas;
  }

  /** Assumes: At the very least, the first checkpoint is a multi-part checkpoint. */
  @VisibleForTesting
  public static void validateMultiPartCheckpoints(List<ParsedCheckpointData> checkpointDatas) {
    final Set<Integer> actualPartNums = new HashSet<>();

    final ParsedMultiPartCheckpointData first =
        (ParsedMultiPartCheckpointData) checkpointDatas.get(0);

    checkArgument(
        first.numParts == checkpointDatas.size(),
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
