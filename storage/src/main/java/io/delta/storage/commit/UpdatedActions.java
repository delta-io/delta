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
import io.delta.storage.commit.actions.AbstractDomainMetadata;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import java.util.Collections;
import java.util.List;

/**
 * A container class to inform the CommitCoordinatorClient about changes to commit actions that
 * Unity Catalog may need to apply atomically with the commit.
 */
public class UpdatedActions {

  private AbstractCommitInfo commitInfo;

  private AbstractMetadata newMetadata;

  private AbstractProtocol newProtocol;

  private AbstractMetadata oldMetadata;

  private AbstractProtocol oldProtocol;

  private List<AbstractDomainMetadata> oldDomainMetadata;

  private List<AbstractDomainMetadata> newDomainMetadata;

  public UpdatedActions(
      AbstractCommitInfo commitInfo,
      AbstractMetadata newMetadata,
      AbstractProtocol newProtocol,
      AbstractMetadata oldMetadata,
      AbstractProtocol oldProtocol) {
    this(
        commitInfo,
        newMetadata,
        newProtocol,
        oldMetadata,
        oldProtocol,
        Collections.emptyList(),
        Collections.emptyList());
  }

  public UpdatedActions(
      AbstractCommitInfo commitInfo,
      AbstractMetadata newMetadata,
      AbstractProtocol newProtocol,
      AbstractMetadata oldMetadata,
      AbstractProtocol oldProtocol,
      List<AbstractDomainMetadata> oldDomainMetadata,
      List<AbstractDomainMetadata> newDomainMetadata) {
    this.commitInfo = commitInfo;
    this.newMetadata = newMetadata;
    this.newProtocol = newProtocol;
    this.oldMetadata = oldMetadata;
    this.oldProtocol = oldProtocol;
    this.oldDomainMetadata =
        oldDomainMetadata == null ? Collections.emptyList() : oldDomainMetadata;
    this.newDomainMetadata =
        newDomainMetadata == null ? Collections.emptyList() : newDomainMetadata;
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

  public List<AbstractDomainMetadata> getOldDomainMetadata() {
    return oldDomainMetadata;
  }

  public List<AbstractDomainMetadata> getNewDomainMetadata() {
    return newDomainMetadata;
  }
}
