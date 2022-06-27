package org.utils;

import java.util.List;
import java.util.StringJoiner;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsoleSink extends RichSinkFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(ConsoleSink.class);

    private final RowType rowType;

    public ConsoleSink(RowType rowType) {
        Preconditions.checkNotNull(rowType);
        this.rowType = rowType;
    }

    @Override
    public void invoke(RowData row, Context context) throws Exception {

        int i = 0;
        StringJoiner joiner = new StringJoiner(", ");
        List<RowField> fields = rowType.getFields();
        for (RowField field : fields) {
            Object value = field.getType().accept(new ValueVisitor(row, i++));
            joiner.add( field.getName() + " -> [" + value + "]");
        }

        LOG.info("Delta table row content: " + joiner);
    }
}
