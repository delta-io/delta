package io.delta.kernel.internal.transaction.domainmetadata;

import io.delta.kernel.internal.actions.DomainMetadata;
import java.util.List;

public interface DomainMetadataHandler {
  void add(String domain, String config);

  void remove(String domain);

  List<DomainMetadata> getDomainMetadatasForCommit();
}
