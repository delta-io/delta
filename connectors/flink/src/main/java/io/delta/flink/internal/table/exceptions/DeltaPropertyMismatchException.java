package io.delta.flink.internal.table.exceptions;

import java.util.List;
import java.util.StringJoiner;

import io.delta.flink.internal.table.CatalogExceptionHelper.MismatchedDdlOptionAndDeltaTableProperty;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;

/**
 * Exception thrown when DDL options attempt to override table properties
 * already defined in the Delta table's _delta_log.
 *
 * <p>Delta table properties are immutable once written to _delta_log.
 * DDL options can only add new properties or match existing ones, but cannot override them.
 */
public class DeltaPropertyMismatchException extends CatalogException {

    private static final long serialVersionUID = 1L;

    private final ObjectPath catalogTablePath;
    private final List<MismatchedDdlOptionAndDeltaTableProperty> invalidOptions;

    public DeltaPropertyMismatchException(
            ObjectPath catalogTablePath,
            List<MismatchedDdlOptionAndDeltaTableProperty> invalidOptions) {

        super(buildMessage(catalogTablePath, invalidOptions));

        this.catalogTablePath = catalogTablePath;
        this.invalidOptions = invalidOptions;
    }

    private static String buildMessage(
            ObjectPath catalogTablePath,
            List<MismatchedDdlOptionAndDeltaTableProperty> invalidOptions) {

        StringJoiner invalidOptionsString = new StringJoiner("\n");
        for (MismatchedDdlOptionAndDeltaTableProperty invalidOption : invalidOptions) {
            invalidOptionsString.add(
                String.join(
                    " | ",
                    invalidOption.getOptionName(),
                    invalidOption.getDdlOptionValue(),
                    invalidOption.getDeltaLogPropertyValue()
                )
            );
        }

        return String.format(
            "Invalid DDL options for table [%s]. "
                + "DDL options for Delta table connector cannot override table properties "
                + "already defined in _delta_log.\n"
                + "DDL option name | DDL option value | Delta option value \n%s",
            catalogTablePath.getFullName(),
            invalidOptionsString);
    }

    public ObjectPath getCatalogTablePath() {
        return catalogTablePath;
    }

    public List<MismatchedDdlOptionAndDeltaTableProperty> getInvalidOptions() {
        return invalidOptions;
    }
}

