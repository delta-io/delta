package io.delta.flink.internal.table;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.delta.flink.internal.table.exceptions.DeltaPropertyMismatchException;
import io.delta.flink.internal.table.exceptions.DeltaSchemaMismatchException;
import io.delta.flink.internal.table.exceptions.DeltaUnsupportedColumnTypeException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructType;

/**
 * Helper class for creating Delta-specific catalog exceptions.
 *
 * <p>This class provides factory methods for creating various types of exceptions
 * that can occur during Delta Catalog operations, including:
 * <ul>
 *   <li>{@link DeltaSchemaMismatchException} - Schema/partition mismatches</li>
 *   <li>{@link DeltaPropertyMismatchException} - Property override conflicts</li>
 *   <li>{@link DeltaUnsupportedColumnTypeException} - Unsupported column types</li>
 *   <li>{@link ValidationException} - Invalid job properties</li>
 * </ul>
 */
public final class CatalogExceptionHelper {

    private static final String INVALID_PROPERTY_TEMPLATE = " - '%s'";

    private static final String ALLOWED_SELECT_JOB_SPECIFIC_OPTIONS =
        DeltaFlinkJobSpecificOptions.SOURCE_JOB_OPTIONS.stream()
            .map(tableProperty -> String.format(INVALID_PROPERTY_TEMPLATE, tableProperty))
            .collect(Collectors.joining("\n"));

    private CatalogExceptionHelper() {}

    static DeltaSchemaMismatchException deltaLogAndDdlSchemaMismatchException(
            ObjectPath catalogTablePath,
            String deltaTablePath,
            Metadata deltaMetadata,
            StructType ddlDeltaSchema,
            List<String> ddlPartitions) {

        return new DeltaSchemaMismatchException(
            catalogTablePath,
            deltaTablePath,
            deltaMetadata,
            ddlDeltaSchema,
            ddlPartitions);
    }

    public static DeltaPropertyMismatchException mismatchedDdlOptionAndDeltaTablePropertyException(
            ObjectPath catalogTablePath,
            List<MismatchedDdlOptionAndDeltaTableProperty> invalidOptions) {

        return new DeltaPropertyMismatchException(catalogTablePath, invalidOptions);
    }

    public static DeltaUnsupportedColumnTypeException unsupportedColumnType(
            Collection<Column> unsupportedColumns) {
        return new DeltaUnsupportedColumnTypeException(unsupportedColumns);
    }

    public static CatalogException invalidDdlOptionException(InvalidDdlOptions invalidOptions) {

        String invalidTablePropertiesUsed = invalidOptions.getInvalidTableProperties().stream()
            .map(tableProperty -> String.format(INVALID_PROPERTY_TEMPLATE, tableProperty))
            .collect(Collectors.joining("\n"));

        String usedJobSpecificOptions = invalidOptions.getJobSpecificOptions().stream()
            .map(jobProperty -> String.format(INVALID_PROPERTY_TEMPLATE, jobProperty))
            .collect(Collectors.joining("\n"));

        String exceptionMessage = "DDL contains invalid properties. "
            + "DDL can have only delta table properties or arbitrary user options only.";

        if (invalidTablePropertiesUsed.length() > 0) {
            exceptionMessage = String.join(
                "\n",
                exceptionMessage,
                String.format("Invalid options used:\n%s", invalidTablePropertiesUsed)
            );
        }

        if (usedJobSpecificOptions.length() > 0) {
            exceptionMessage = String.join(
                "\n",
                exceptionMessage,
                String.format(
                    "DDL contains job-specific options. Job-specific options can be used only via "
                        + "Query hints.\nUsed job-specific options:\n%s", usedJobSpecificOptions)
            );
        }

        return new CatalogException(exceptionMessage);
    }

    public static ValidationException invalidInsertJobPropertyException(
        Collection<String> invalidOptions) {
        String insertJobSpecificOptions = invalidOptions.stream()
            .map(tableProperty -> String.format(INVALID_PROPERTY_TEMPLATE, tableProperty))
            .collect(Collectors.joining("\n"));

        String message = String.format(
            "Currently no job-specific options are allowed in INSERT SQL statements.\n"
                + "Invalid options used:\n%s",
            insertJobSpecificOptions);

        return new ValidationException(message);
    }

    public static ValidationException invalidSelectJobPropertyException(
        Collection<String> invalidOptions) {
        String selectJobSpecificOptions = invalidOptions.stream()
            .map(tableProperty -> String.format(INVALID_PROPERTY_TEMPLATE, tableProperty))
            .collect(Collectors.joining("\n"));

        String message = String.format(
            "Only job-specific options are allowed in SELECT SQL statement.\n"
                + "Invalid options used: \n%s\n"
                + "Allowed options:\n%s",
            selectJobSpecificOptions,
            ALLOWED_SELECT_JOB_SPECIFIC_OPTIONS
        );

        return new ValidationException(message);
    }

    /**
     * A container class that contains DDL and _delta_log property values for given DDL option.
     */
    public static class MismatchedDdlOptionAndDeltaTableProperty {

        private final String optionName;

        private final String ddlOptionValue;

        private final String deltaLogPropertyValue;

        public MismatchedDdlOptionAndDeltaTableProperty(
                String optionName,
                String ddlOptionValue,
                String deltaLogPropertyValue) {
            this.optionName = optionName;
            this.ddlOptionValue = ddlOptionValue;
            this.deltaLogPropertyValue = deltaLogPropertyValue;
        }

        public String getOptionName() {
            return optionName;
        }

        public String getDdlOptionValue() {
            return ddlOptionValue;
        }

        public String getDeltaLogPropertyValue() {
            return deltaLogPropertyValue;
        }
    }

    public static class InvalidDdlOptions {

        private final Set<String> jobSpecificOptions = new HashSet<>();

        private final Set<String> invalidTableProperties = new HashSet<>();

        public void addJobSpecificOption(String jobSpecificOption) {
            this.jobSpecificOptions.add(jobSpecificOption);
        }

        public void addInvalidTableProperty(String invalidTableProperty) {
            this.invalidTableProperties.add(invalidTableProperty);
        }

        public boolean hasInvalidOptions() {
            return !(jobSpecificOptions.isEmpty() && invalidTableProperties.isEmpty());
        }

        public Collection<String> getJobSpecificOptions() {
            return Collections.unmodifiableSet(this.jobSpecificOptions);
        }

        public Collection<String> getInvalidTableProperties() {
            return Collections.unmodifiableSet(this.invalidTableProperties);
        }
    }

}
