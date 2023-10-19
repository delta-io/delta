package io.delta.flink.source.internal.enumerator.supplier;

import io.delta.kernel.Table;
import io.delta.kernel.client.TableClient;
import io.delta.standalone.DeltaLog;

public class ContinuousSnapshotSupplierFactory implements SnapshotSupplierFactory {

    @Override
    public ContinuousSourceSnapshotSupplier create(DeltaLog deltaLog, TableClient tableClient, Table table) {
	return new ContinuousSourceSnapshotSupplier(deltaLog, tableClient, table);
    }
}
