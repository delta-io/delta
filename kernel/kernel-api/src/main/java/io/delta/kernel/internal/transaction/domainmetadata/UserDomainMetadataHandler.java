package io.delta.kernel.internal.transaction.domainmetadata;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Preconditions.checkState;

import io.delta.kernel.exceptions.DomainDoesNotExistException;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.transaction.TransactionV2State;
import java.util.*;

public class UserDomainMetadataHandler implements DomainMetadataHandler {

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

  @Override
  public void add(String domain, String config) {
    checkArgument(
        !domainsToRemove.contains(domain),
        "Cannot add a domain that is removed in this transaction");

    domainsToAdd.put(domain, new DomainMetadata(domain, config, false /* removed */));
    computedDomainMetadatasForCommit = Optional.empty();
  }

  @Override
  public void remove(String domain) {
    checkArgument(
        !domainsToAdd.containsKey(domain),
        "Cannot remove a domain that is added in this transaction");

    domainsToRemove.add(domain);
    computedDomainMetadatasForCommit = Optional.empty();
  }

  @Override
  public List<DomainMetadata> getDomainMetadatasForCommit() {
    if (computedDomainMetadatasForCommit.isPresent()) {
      return computedDomainMetadatasForCommit.get();
    }

    if (txnState.isReplace()) {
      checkState(txnState.readTableOpt.isPresent(), "Replace assumes readTableOpt is present");

      // In the case of replace table we need to completely reset the table state by removing
      // any existing domain metadata
      txnState.readTableOpt
          .get()
          .getActiveDomainMetadataMap()
          .forEach(
              (domainName, domainMetadata) -> {
                if (!domainsToAdd.containsKey(domainName)) {
                  // We only need to remove the domain if it is not added (& thus overwritten)
                  // in this current transaction. We cannot add and remove the same domain in
                  // one transaction.
                  domainsToRemove.add(domainName);
                }
              });
    }

    final List<DomainMetadata> result = new ArrayList<>(domainsToAdd.values());

    if (domainsToRemove.isEmpty()) {
      // If no domain metadatas are removed we don't need to load the existing domain metadatas
      // from the snapshot (which is an expensive operation)
      computedDomainMetadatasForCommit = Optional.of(result);
      return result;
    }

    // Generate the tombstones for removed domains
    Map<String, DomainMetadata> snapshotDomainMetadataMap =
        txnState.readTableOpt.map(x -> x.getActiveDomainMetadataMap()).orElse(Collections.emptyMap());

    for (String domainName : domainsToRemove) {
      if (snapshotDomainMetadataMap.containsKey(domainName)) {
        // Note: we know domainName is not already in finalDomainMetadatas because we do not allow
        // removing and adding a domain with the same identifier in a single txn!
        DomainMetadata domainToRemove = snapshotDomainMetadataMap.get(domainName);
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
            txnState.dataPath, domainName, readSnapshot.getVersion());
      }
    }

    computedDomainMetadatasForCommit = Optional.of(result);
    return result;
  }

}
