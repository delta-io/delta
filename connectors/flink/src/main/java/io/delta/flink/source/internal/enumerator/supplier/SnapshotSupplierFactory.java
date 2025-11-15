package io.delta.flink.source.internal.enumerator.supplier;

import io.delta.standalone.DeltaLog;

public interface SnapshotSupplierFactory {

    SnapshotSupplier create(DeltaLog deltaLog);
}
