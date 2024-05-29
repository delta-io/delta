package io.delta.flinkv2.sink;

import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import org.apache.flink.api.connector.sink2.Committer;

import java.io.IOException;
import java.util.Collection;

public class DeltaCommitter implements Committer<DeltaCommittable> {

    public DeltaCommitter() {

    }

    @Override
    public void commit(
            Collection<CommitRequest<DeltaCommittable>> committables)
            throws IOException, InterruptedException {

    }

    @Override
    public void close() throws Exception {

    }
}
