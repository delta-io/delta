package io.delta.flinkv2.sink;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;


public class DeltaSink implements TwoPhaseCommittingSink<RowData, DeltaCommittable>, SupportsPreCommitTopology {

    ///////////////////////
    // Public static API //
    ///////////////////////

    public static DeltaSink forRowData(final Path tablePath, final RowType rowType) {
        return new DeltaSink(tablePath, rowType);
    }

    ///////////////////
    // Member fields //
    ///////////////////

    private DeltaSink(final Path basePath, final RowType rowType) {

    }

    /////////////////
    // Public APIs //
    /////////////////

    @Override
    public PrecommittingSinkWriter<RowData, DeltaCommittable> createWriter(
            InitContext context) throws IOException {
        System.out.println("Scott > DeltaSink > createWriter");
        return new DeltaPrecommittingSinkWriter();
    }

    @Override
    public Committer<DeltaCommittable> createCommitter() throws IOException {
        System.out.println("Scott > DeltaSink > createCommitter");
        return new DeltaCommitter();
    }

    @Override
    public SimpleVersionedSerializer<DeltaCommittable> getCommittableSerializer() {
        return null;
    }
}
