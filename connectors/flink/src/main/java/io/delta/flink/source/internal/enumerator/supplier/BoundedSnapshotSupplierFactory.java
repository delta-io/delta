package io.delta.flink.source.internal.enumerator.supplier;

import io.delta.kernel.Table;
import io.delta.kernel.client.TableClient;

import io.delta.standalone.DeltaLog;

public class BoundedSnapshotSupplierFactory implements SnapshotSupplierFactory {

    @Override
    public BoundedSourceSnapshotSupplier create(
          DeltaLog deltaLog,
          TableClient tableClient,
          Table table) {
        return new BoundedSourceSnapshotSupplier(deltaLog, tableClient, table);
    }
}
