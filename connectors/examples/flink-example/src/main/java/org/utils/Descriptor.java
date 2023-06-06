package org.utils;

import java.util.Collections;
import java.util.List;

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

/**
 * This class describes a Delta table update scenario for IT case test. Information from this class
 * is used by updater thread that updates Delta table with new rows during test run.
 */
public class Descriptor {

    /**
     * Path to Delta table
     */
    private final String tablePath;

    /**
     * A {@link RowType} that describes both column names and column types for table row.
     */
    private final RowType rowType;

    /**
     * A {@link List} of rows that should be inserted into Delta table.
     */
    private final List<Row> rows;

    public Descriptor(String tablePath, RowType rowType, List<Row> rows) {
        this.tablePath = tablePath;
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

    public String getTablePath() {
        return tablePath;
    }
}
