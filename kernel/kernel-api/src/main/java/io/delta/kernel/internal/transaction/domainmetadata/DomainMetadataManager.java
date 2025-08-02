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

package io.delta.kernel.internal.transaction.domainmetadata;

import static io.delta.kernel.internal.util.Preconditions.checkState;

import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.transaction.TransactionV2State;
import java.util.ArrayList;
import java.util.List;

public class DomainMetadataManager {

  private final TransactionV2State txnState;
  private final UserDomainMetadataHandler userDomainMetadataHandler;

  public DomainMetadataManager(TransactionV2State txnState) {
    this.txnState = txnState;
    this.userDomainMetadataHandler = new UserDomainMetadataHandler(txnState);
  }

  public void addDomainMetadata(String domain, String config) {
    checkDomainMetadataSupported();

    if (DomainMetadata.isUserControlledDomain(domain)) {
      userDomainMetadataHandler.add(domain, config);
    } else {
      // TODO: rowTracking
      throw new IllegalArgumentException("Unsupported domain: " + domain);
    }
  }

  public void removeDomainMetadata(String domain) {
    checkDomainMetadataSupported();
    checkIsUserControlledDomain(domain);
    userDomainMetadataHandler.remove(domain);
  }

  public List<DomainMetadata> getAllDomainMetadatasForCommit() {

    // append to the lists of added and removed domain metadatas -- for user-controlled domains
    // append to the lists of added and removed domain metadatas -- for row-tracking domains
    // append to the lists of added and removed domain metadatas -- for clustering domains

    // append to the list of removed domain metadatas -- in the case of REPLACE table

    // generate all of the correct added domain metadatas
    // generate all of the tombstones for any removed domain metadatas

    // return final result
    final List<DomainMetadata> result = new ArrayList<>();
    result.addAll(userDomainMetadataHandler.getDomainMetadatasForCommit());
    // TODO: clustering
    // TODO: rowTracking
    return result;
  }

  private void checkDomainMetadataSupported() {
    checkState(
        TableFeatures.isDomainMetadataSupported(txnState.getEffectiveProtocol()),
        "Domain metadata is not supported in the current protocol version");
  }

  private void checkIsUserControlledDomain(String domain) {
    checkState(
        DomainMetadata.isUserControlledDomain(domain),
        "Domain is not user-controlled: " + domain);
  }
}
