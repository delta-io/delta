package io.delta.flink.source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

/**
 * This class describes a Delta table update scenario for IT case test. Information from this class
 * is used by updater thread that updates Delta table with new rows during test run.
 */
public class ContinuousTestDescriptor {

    /**
     * Number of rows in test Delta table before starting adding new data.
     */
    private final int initialDataSize;

    /**
     * A {@link List} of {@link Descriptor} objcets describing every data insert into Delta table
     * that should be executed during test run.
     */
    private final List<Descriptor> updateDescriptors = new ArrayList<>();

    public ContinuousTestDescriptor(int initialDataSize) {
        this.initialDataSize = initialDataSize;
    }

    /**
     * Add batch of rows that should be inserted into a Delta table as a one table updater
     * batch.
     */
    public void add(RowType rowType, List<Row> rows) {
        updateDescriptors.add(new Descriptor(rowType, rows));
    }

    public List<Descriptor> getUpdateDescriptors() {
        return Collections.unmodifiableList(updateDescriptors);
    }

    public int getInitialDataSize() {
        return initialDataSize;
    }

    /**
     * This class represents a batch of rows that should be inserted into a Delta table.
     */
    public static class Descriptor {

        /**
         * A {@link RowType} that describes both column names and column types for table row.
         */
        private final RowType rowType;

        /**
         * A {@link List} of rows that should be inserted into Delta table.
         */
        private final List<Row> rows;

        public Descriptor(RowType rowType, List<Row> rows) {
            this.rowType = rowType;
            this.rows = rows;
        }

        public RowType getRowType() {
            return rowType;
        }

        public List<Row> getRows() {
            return Collections.unmodifiableList(rows);
        }

        public int getNumberOfNewRows() {
            return rows.size();
        }
    }

}
