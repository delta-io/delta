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
