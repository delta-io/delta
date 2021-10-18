package io.delta.standalone;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * An operation that can be performed on a Delta table.
 *
 * An operation is tracked as the first line in delta logs, and powers `DESCRIBE HISTORY` for Delta
 * tables.
 */
public final class Operation {

    /**
     * Supported operation types.
     */
    public enum Name {
        /** Recorded during batch inserts. */
        WRITE("WRITE"),

        /** Recorded during streaming inserts. */
        STREAMING_UPDATE("STREAMING UPDATE"),

        /** Recorded while deleting certain partitions. */
        DELETE("DELETE"),

        /** Recorded when truncating the table. */
        TRUNCATE("TRUNCATE"),

        /** Recorded when converting a table into a Delta table. */
        CONVERT("CONVERT"),

        /** Recorded when a merge operation is committed to the table. */
        MERGE("MERGE"),

        /** Recorded when an update operation is committed to the table. */
        UPDATE("UPDATE"),

        /** Recorded when the table is created. */
        CREATE_TABLE("CREATE TABLE"),

        /** Recorded when the table is replaced. */
        REPLACE_TABLE("REPLACE TABLE"),

        /** Recorded when the table properties are set. */
        SET_TABLE_PROPERTIES("SET TBLPROPERTIES"),

        /** Recorded when the table properties are unset. */
        UNSET_TABLE_PROPERTIES("UNSET TBLPROPERTIES"),

        /** Recorded when columns are added. */
        ADD_COLUMNS("ADD COLUMNS"),

        /** Recorded when columns are changed. */
        CHANGE_COLUMN("CHANGE COLUMN"),

        /** Recorded when columns are replaced. */
        REPLACE_COLUMNS("REPLACE COLUMNS"),

        /** Recorded when the table protocol is upgraded. */
        UPGRADE_PROTOCOL("UPGRADE PROTOCOL"),

        /** Recorded when the table schema is upgraded. */
        UPGRADE_SCHEMA("UPDATE SCHEMA"),

        MANUAL_UPDATE("Manual Update");

        /** Actual value that will be recorded in the transaction log */
        private final String logStr;

        Name(String logStr) {
            this.logStr = logStr;
        }

        @Override
        public String toString() {
            return logStr;
        }
    }

    private final Name name;
    private final Map<String, Object> parameters;
    private final Map<String, String> operationMetrics;
    private final Optional<String> userMetadata;

    public Operation(Name name) {
        this(name, Collections.emptyMap(), Collections.emptyMap(), Optional.empty());
    }

    public Operation(Name name, Map<String, Object> parameters) {
        this(name, parameters, Collections.emptyMap(), Optional.empty());
    }

    public Operation(Name name, Map<String, Object> parameters, Map<String, String> operationMetrics) {
        this(name, parameters, operationMetrics, Optional.empty());
    }

    public Operation(Name name, Map<String, Object> parameters, Map<String, String> operationMetrics,
                     Optional<String> userMetadata) {
        this.name = name;
        this.parameters = parameters;
        this.operationMetrics = operationMetrics;
        this.userMetadata = userMetadata;
    }

    /**
     * @return operation name
     */
    public Name getName() {
        return name;
    }

    /**
     * @return operation parameters
     */
    public Map<String, Object> getParameters() {
        return parameters != null ? Collections.unmodifiableMap(parameters) : null;
    }

    /**
     * @return operation metrics
     */
    public Map<String, String> getOperationMetrics() {
        return operationMetrics != null ? Collections.unmodifiableMap(operationMetrics) : null;
    }

    /**
     * @return user metadata for this operation
     */
    public Optional<String> getUserMetadata() {
        return null == userMetadata ? Optional.empty() : userMetadata;
    }
}
