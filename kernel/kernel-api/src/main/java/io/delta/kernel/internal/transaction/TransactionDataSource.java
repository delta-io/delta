package io.delta.kernel.internal.transaction;

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.checksum.CRCInfo;
import io.delta.kernel.metrics.SnapshotReport;
import java.util.Map;
import java.util.Optional;

public interface TransactionDataSource {
  long getVersion();

  long getTimestamp(Engine engine);

  ScanBuilder getScanBuilder();

  Map<String, DomainMetadata> getActiveDomainMetadataMap();

  Optional<CRCInfo> getCurrentCrcInfo();

  SnapshotReport getSnapshotReport();
}
