package io.delta.flink.internal.table.exceptions;

import java.util.Collection;
import java.util.StringJoiner;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.exceptions.CatalogException;

/**
 * Exception thrown when a table definition contains unsupported column types.
 *
 * <p>Currently, Delta Flink connector only supports physical columns.
 * Computed columns, metadata columns, and other special column types are not supported.
 */
public class DeltaUnsupportedColumnTypeException extends CatalogException {

    private static final long serialVersionUID = 1L;

    private final Collection<Column> unsupportedColumns;

    public DeltaUnsupportedColumnTypeException(Collection<Column> unsupportedColumns) {
        super(buildMessage(unsupportedColumns));
        this.unsupportedColumns = unsupportedColumns;
    }

    private static String buildMessage(Collection<Column> unsupportedColumns) {
        StringJoiner sj = new StringJoiner("\n");
        for (Column unsupportedColumn : unsupportedColumns) {
            sj.add(
                String.join(
                    " -> ",
                    unsupportedColumn.getName(),
                    unsupportedColumn.getClass().getSimpleName()
                )
            );
        }

        return String.format(
            "Table definition contains unsupported column types. "
                + "Currently, only physical columns are supported by Delta Flink connector.\n"
                + "Invalid columns and types:\n%s", sj);
    }

    public Collection<Column> getUnsupportedColumns() {
        return unsupportedColumns;
    }
}

