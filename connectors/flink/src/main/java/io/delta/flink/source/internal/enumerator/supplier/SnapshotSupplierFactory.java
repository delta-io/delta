package io.delta.flink.source.internal.enumerator.supplier;

import io.delta.standalone.DeltaLog;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;

public interface SnapshotSupplierFactory {

    SnapshotSupplier create(DeltaLog deltaLog, Configuration configuration, Path tablePath);
}
