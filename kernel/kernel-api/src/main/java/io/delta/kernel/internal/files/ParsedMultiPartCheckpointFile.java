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

import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.FileStatus;

public class ParsedMultiPartCheckpointFile extends ParsedCheckpointFile {
  ///////////////////////////////////////
  // Static enums, fields, and methods //
  ///////////////////////////////////////

  public static ParsedMultiPartCheckpointFile parsedMultiPartCheckpointFile(FileStatus fileStatus) {
    final long version = FileNames.checkpointVersion(fileStatus.getPath());
    final Tuple2<Integer, Integer> partAndNumParts =
        FileNames.multiPartCheckpointPartAndNumParts(fileStatus.getPath());
    return new ParsedMultiPartCheckpointFile(
        fileStatus, version, partAndNumParts._1, partAndNumParts._2);
  }

  ///////////////////////////////
  // Member fields and methods //
  ///////////////////////////////

  private final int part;
  private final int numParts;

  private ParsedMultiPartCheckpointFile(
      FileStatus fileStatus, long version, int part, int numParts) {
    super(fileStatus, version, ParsedLogType.MULTIPART_CHECKPOINT);
    this.part = part;
    this.numParts = numParts;
  }

  public int getPart() {
    return part;
  }

  public int getNumParts() {
    return numParts;
  }
}
