package io.delta.flinkv2.sink;

import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DeltaSinkWriter implements CommittingSinkWriter<RowData, DeltaCommittable> {

    private final List<RowData> buffer; // TODO: better data type?

    public DeltaSinkWriter() {
        this.buffer = new ArrayList<>();
    }

    @Override
    public void write(RowData element, Context context) throws IOException, InterruptedException {
        this.buffer.add(element);

        System.out.println(String.format("Scott > DeltaSinkWriter > write :: element=%s", element));
    }

    /**
     * Preparing the commit is the first part of a two-phase commit protocol.
     *
     * Returns the data to commit as the second step of the two-phase commit protocol.
     */
    @Override
    public Collection<DeltaCommittable> prepareCommit() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        // TODO: perhaps have the writer write (i.e. flush) the data here, as this can be called
        //       multiple times?
    }

    @Override
    public void close() throws Exception {

    }
}
