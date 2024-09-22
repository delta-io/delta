/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.engine.coordinatedcommits;

import io.delta.kernel.annotation.Evolving;

/**
 * Response container for {@link io.delta.kernel.engine.CommitCoordinatorClientHandler#commit}.
 *
 * @since 3.3.0
 */
@Evolving
public class CommitResponse {

  private final Commit commit;

  public CommitResponse(Commit commit) {
    this.commit = commit;
  }

  /**
   * Get the commit object.
   *
   * @return the commit object.
   */
  public Commit getCommit() {
    return commit;
  }
}
