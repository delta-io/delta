package io.delta.flink.source.internal.enumerator.supplier;

import io.delta.standalone.DeltaLog;

public class BoundedSnapshotSupplierFactory implements SnapshotSupplierFactory {

    @Override
    public BoundedSourceSnapshotSupplier create(DeltaLog deltaLog) {
        return new BoundedSourceSnapshotSupplier(deltaLog);
    }
}
