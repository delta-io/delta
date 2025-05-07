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

public class ParsedCompactionFile extends ParsedLogFile {

  ///////////////////////////////////////
  // Static enums, fields, and methods //
  ///////////////////////////////////////

  public static ParsedCompactionFile parsedCompactionFile(FileStatus fileStatus) {
    final Tuple2<Long, Long> versions = FileNames.logCompactionVersions(fileStatus.getPath());
    return new ParsedCompactionFile(fileStatus, versions._1, versions._2);
  }

  ///////////////////////////////
  // Member fields and methods //
  ///////////////////////////////

  private final long startVersion;
  private final long endVersion;

  private ParsedCompactionFile(FileStatus fileStatus, long startVersion, long endVersion) {
    super(fileStatus, endVersion, ParsedLogType.LOG_COMPACTION);
    this.startVersion = startVersion;
    this.endVersion = endVersion;
  }

  public long getStartVersion() {
    return startVersion;
  }

  public long getEndVersion() {
    return endVersion;
  }
}
