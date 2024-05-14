package io.delta.flinkv2.sink;

import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.Collection;

public class DeltaPrecommittingSinkWriter implements
    TwoPhaseCommittingSink.PrecommittingSinkWriter<RowData, DeltaCommittable>  {

    @Override
    public Collection<DeltaCommittable> prepareCommit() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void write(RowData element, Context context) throws IOException, InterruptedException {

    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {

    }

    @Override
    public void close() throws Exception {

    }
}
