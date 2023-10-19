package io.delta.flink.source.internal.enumerator.supplier;

import io.delta.kernel.Table;
import io.delta.kernel.client.TableClient;
import io.delta.standalone.DeltaLog;

public interface SnapshotSupplierFactory {

    SnapshotSupplier create(DeltaLog deltaLog, TableClient tableClient, Table table);
}
