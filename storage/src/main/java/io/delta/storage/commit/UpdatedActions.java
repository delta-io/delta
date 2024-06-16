/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.storage.commit;

import io.delta.storage.commit.actions.AbstractCommitInfo;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;

/**
 * A container class to inform the CommitCoordinatorClient about any changes in Protocol/Metadata
 */
public class UpdatedActions {

  private AbstractCommitInfo commitInfo;

  private AbstractMetadata newMetadata;

  private AbstractProtocol newProtocol;

  private AbstractMetadata oldMetadata;

  private AbstractProtocol oldProtocol;

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

  public AbstractCommitInfo getCommitInfo() {
    return commitInfo;
  }

  public AbstractMetadata getNewMetadata() {
    return newMetadata;
  }

  public AbstractProtocol getNewProtocol() {
    return newProtocol;
  }

  public AbstractMetadata getOldMetadata() {
    return oldMetadata;
  }

  public AbstractProtocol getOldProtocol() {
    return oldProtocol;
  }
}
