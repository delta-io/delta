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
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.FileStatus;
import java.util.Objects;
import java.util.Optional;

public class ParsedMultiPartCheckpointData extends ParsedCheckpointData {
  public static ParsedMultiPartCheckpointData forFileStatus(FileStatus fileStatus) {
    final long version = FileNames.checkpointVersion(fileStatus.getPath());
    final Tuple2<Integer, Integer> partInfo =
        FileNames.multiPartCheckpointPartAndNumParts(fileStatus.getPath());
    return new ParsedMultiPartCheckpointData(
        version, partInfo._1, partInfo._2, Optional.of(fileStatus), Optional.empty());
  }

  public static ParsedMultiPartCheckpointData forInlineData(
      long version, int part, int numParts, ColumnarBatch inlineData) {
    return new ParsedMultiPartCheckpointData(
        version, part, numParts, Optional.empty(), Optional.of(inlineData));
  }

  public final int part;
  public final int numParts;

  private ParsedMultiPartCheckpointData(
      long version,
      int part,
      int numParts,
      Optional<FileStatus> fileStatusOpt,
      Optional<ColumnarBatch> inlineDataOpt) {
    super(version, fileStatusOpt, inlineDataOpt);
    checkArgument(numParts > 0, "numParts must be greater than 0");
    checkArgument(part > 0 && part <= numParts, "part must be between 1 and numParts");
    this.part = part;
    this.numParts = numParts;
  }

  @Override
  protected int getCheckpointTypePriority() {
    return 1; // Classic (0) < MultiPart (1) < V2 (2). MultiPart has middle priority.
  }

  @Override
  protected void appendAdditionalToStringFields(StringBuilder sb) {
    sb.append(", part=").append(part).append(", numParts=").append(numParts);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ParsedMultiPartCheckpointData that = (ParsedMultiPartCheckpointData) o;
    return part == that.part && numParts == that.numParts;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), part, numParts);
  }

  public int compareToMultiPart(ParsedMultiPartCheckpointData that) {
    final int numPartsComparison = Long.compare(this.numParts, that.numParts);
    if (numPartsComparison != 0) {
      return numPartsComparison;
    } else {
      return getTieBreaker(that);
    }
  }
}
