package io.delta.flink.source.internal.enumerator.supplier;

import io.delta.standalone.DeltaLog;

public class ContinuousSnapshotSupplierFactory implements SnapshotSupplierFactory {

    @Override
    public ContinuousSourceSnapshotSupplier create(DeltaLog deltaLog) {
        return new ContinuousSourceSnapshotSupplier(deltaLog);
    }
}
