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
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractCommitInfo;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractMetadata;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractProtocol;

/**
 * A container class to inform the {@link
 * io.delta.kernel.coordinatedcommits.CommitCoordinatorClient} about any changes in
 * Protocol/Metadata
 *
 * @since 3.3.0
 */
@Evolving
public class UpdatedActions {
  private final AbstractCommitInfo commitInfo;

  private final AbstractMetadata newMetadata;

  private final AbstractProtocol newProtocol;

  private final AbstractMetadata oldMetadata;

  private final AbstractProtocol oldProtocol;

  public UpdatedActions(
      AbstractCommitInfo commitInfo,
      AbstractMetadata newMetadata,
      AbstractProtocol newProtocol,
      AbstractMetadata oldMetadata,
      AbstractProtocol oldProtocol) {
    this.commitInfo = commitInfo;
    this.newMetadata = newMetadata;
    this.newProtocol = newProtocol;
    this.oldMetadata = oldMetadata;
    this.oldProtocol = oldProtocol;
  }

  /**
   * Get the commit info for this commit attempt.
   *
   * @return the commit info.
   */
  public AbstractCommitInfo getCommitInfo() {
    return commitInfo;
  }

  /**
   * Get the new metadata which needs to be committed.
   *
   * @return the new metadata.
   */
  public AbstractMetadata getNewMetadata() {
    return newMetadata;
  }

  /**
   * Get the new protocol which needs to be committed.
   *
   * @return the new protocol.
   */
  public AbstractProtocol getNewProtocol() {
    return newProtocol;
  }

  /**
   * Get the metadata from the read snapshot of this transaction.
   *
   * @return the old metadata.
   */
  public AbstractMetadata getOldMetadata() {
    return oldMetadata;
  }

  /**
   * Get the protocol from the read snapshot of this transaction.
   *
   * @return the old protocol.
   */
  public AbstractProtocol getOldProtocol() {
    return oldProtocol;
  }
}
