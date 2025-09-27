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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Preconditions.checkState;

import io.delta.kernel.ResolvedTable;
import io.delta.kernel.exceptions.DomainDoesNotExistException;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.table.ResolvedTableInternal;
import io.delta.kernel.internal.transaction.TransactionV2State;
import java.util.*;

public class UserDomainMetadataHandler {

  private final TransactionV2State txnState;
  private final Map<String, DomainMetadata> domainsToAdd;
  private final Set<String> domainsToRemove;
  private Optional<List<DomainMetadata>> computedDomainMetadatasForCommit;

  public UserDomainMetadataHandler(TransactionV2State txnState) {
    this.txnState = txnState;
    this.domainsToAdd = new HashMap<>();
    this.domainsToRemove = new HashSet<>();
    this.computedDomainMetadatasForCommit = Optional.empty();
  }

  public void add(String domain, String config) {
    checkArgument(
        !domainsToRemove.contains(domain),
        "Cannot add a domain that is removed in this transaction");

    domainsToAdd.put(domain, new DomainMetadata(domain, config, false /* removed */));
    computedDomainMetadatasForCommit = Optional.empty();
  }

  public void remove(String domain) {
    checkArgument(
        !domainsToAdd.containsKey(domain),
        "Cannot remove a domain that is added in this transaction");

    domainsToRemove.add(domain);
    computedDomainMetadatasForCommit = Optional.empty();
  }

  public List<DomainMetadata> getDomainMetadatasForCommit() {
    if (computedDomainMetadatasForCommit.isPresent()) {
      return computedDomainMetadatasForCommit.get();
    }

    if (txnState.isReplace()) {
      removeAllDomainMetadatasForReplace();
    }

    final List<DomainMetadata> result = new ArrayList<>(domainsToAdd.values());

    if (domainsToRemove.isEmpty()) {
      // If no domain metadatas are removed then we don't need to generate tombstones, which
      // requires loading the existing domain metadatas from the resolvedTable, which is an
      // expensive operation
      computedDomainMetadatasForCommit = Optional.of(result);
      return result;
    }

    result.addAll(generateDomainMetadataTombstones());

    computedDomainMetadatasForCommit = Optional.of(result);

    return result;
  }

  private void removeAllDomainMetadatasForReplace() {
    checkState(txnState.readTableOpt.isPresent(), "Replace assumes readTableOpt is present");

    // In the case of replace table we need to completely reset the table state by removing
    // any existing domain metadata
    txnState
        .readTableOpt
        .get()
        .getActiveDomainMetadataMap()
        .forEach(
            (domainName, domainMetadata) -> {
              if (!domainsToAdd.containsKey(domainName)) {
                // We only need to remove the domain if it is not added (& thus overwritten)
                // in this current transaction. We cannot add and remove the same domain in
                // one transaction.
                remove(domainName);
              }
            });
  }

  private List<DomainMetadata> generateDomainMetadataTombstones() {
    final List<DomainMetadata> result = new ArrayList<>();

    final Map<String, DomainMetadata> resolvedTableDomainMetadataMap =
        txnState
            .readTableOpt
            .map(ResolvedTableInternal::getActiveDomainMetadataMap)
            .orElse(Collections.emptyMap());

    for (String domainName : domainsToRemove) {
      if (resolvedTableDomainMetadataMap.containsKey(domainName)) {
        // Note: we know domainName is not already in finalDomainMetadatas because we do not allow
        // removing and adding a domain with the same identifier in a single txn!
        final DomainMetadata domainToRemove = resolvedTableDomainMetadataMap.get(domainName);
        checkState(
            !domainToRemove.isRemoved(),
            "snapshotDomainMetadataMap should only contain active domain metadata");
        result.add(domainToRemove.removed());
      } else {
        // We must throw an error if the domain does not exist. Otherwise, there could be
        // unexpected behavior within conflict resolution. For example, consider the following
        // 1. Table has no domains set in V0
        // 2. txnA is started and wants to remove domain "foo"
        // 3. txnB is started and adds domain "foo" and commits V1 before txnA
        // 4. txnA needs to perform conflict resolution against the V1 commit from txnB
        // Conflict resolution should fail but since the domain does not exist we cannot create
        // a tombstone to mark it as removed and correctly perform conflict resolution.
        throw new DomainDoesNotExistException(
            txnState.dataPath,
            domainName,
            txnState.readTableOpt.map(ResolvedTable::getVersion).orElse(-1L));
      }
    }

    return result;
  }
}
