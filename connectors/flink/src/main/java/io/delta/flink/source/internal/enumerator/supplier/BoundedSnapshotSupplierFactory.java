package io.delta.flink.source.internal.enumerator.supplier;

import io.delta.standalone.DeltaLog;

import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class BoundedSnapshotSupplierFactory implements SnapshotSupplierFactory {

    @Override
    public BoundedSourceSnapshotSupplier create(DeltaLog deltaLog, Configuration configuration, Path tablePath) {
        return new BoundedSourceSnapshotSupplier(deltaLog, configuration, tablePath);
    }
}
