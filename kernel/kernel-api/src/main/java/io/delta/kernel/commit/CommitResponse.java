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

package io.delta.kernel.commit;

import io.delta.kernel.annotation.Experimental;
import io.delta.kernel.internal.files.ParsedLogData;

/** Response container for the result of a commit operation. */
@Experimental
public class CommitResponse {

  // TODO: Create a DeltaLogData extends ParsedLogData that includes commit timestamp information.
  /**
   * The parsed log data resulting from the commit operation. Note that for catalog-managed tables,
   * this may be the ratified staged commit, the ratified inline commit, or even a published Delta
   * file that the {@link Committer} implementation decided to publish after committing to the
   * managing catalog.
   */
  public final ParsedLogData commitLogData;

  public CommitResponse(ParsedLogData commitLogData) {
    this.commitLogData = commitLogData;
  }
}
