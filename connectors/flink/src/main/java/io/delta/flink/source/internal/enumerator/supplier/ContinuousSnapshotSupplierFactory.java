package io.delta.flink.source.internal.enumerator.supplier;

import io.delta.standalone.DeltaLog;

import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class ContinuousSnapshotSupplierFactory implements SnapshotSupplierFactory {

    @Override
    public ContinuousSourceSnapshotSupplier create(DeltaLog deltaLog, Configuration configuration, Path tablePath) {
	return new ContinuousSourceSnapshotSupplier(deltaLog, configuration, tablePath);
    }
}
