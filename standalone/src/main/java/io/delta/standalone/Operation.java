// TODO: copyright

package io.delta.standalone;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * An operation that can be performed on a Delta table.
 *
 * An operation is tracked as the first line in delta logs, and powers `DESCRIBE HISTORY` for Delta
 * tables.
 *
 * Operations must be constructed using one of the {@link Operation.Name} types below.
 * As well, optional {@link Metrics} values are given below.
 */
public final class Operation {

    ///////////////////////////////////////////////////////////////////////////
    // Operation Names
    ///////////////////////////////////////////////////////////////////////////

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

    ///////////////////////////////////////////////////////////////////////////
    // Operation Metrics
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Some possible operation metrics and their suggested corresponding operation types.
     * These are purely exemplary, and users may user whichever metrics best fit their application.
     */
    public static class Metrics {
        /**
         * Number of files written.
         *
         * Usually used with WRITE operation.
         */
        public static final String numFiles = "numFiles";

        /**
         * Size in bytes of the written contents.
         *
         * Usually used with WRITE, STREAMING_UPDATE operations.
         */
        public static final String numOutputBytes = "numOutputBytes";

        /**
         * Number of rows written.
         *
         * Usually used with WRITE, STREAMING_UPDATE, MERGE operations.
         */
        public static final String numOutputRows = "numOutputRows";

        /**
         * Number of files added.
         *
         * Usually used with STREAMING_UPDATE, DELETE, UPDATE operations.
         */
        public static final String numAddedFiles = "numAddedFiles";

        /**
         * Number of files removed.
         *
         * Usually used with STREAMING_UPDATE, DELETE, DELETE_PARTITIONS, TRUNCATE,
         * UPDATE operations.
         */
        public static final String numRemovedFiles = "numRemovedFiles";

        /**
         * Number of rows removed.
         *
         * Usually used with DELETE operation.
         */
        public static final String numDeletedRows = "numDeletedRows";

        /**
         * Number of rows copied in the process of deleting files.
         *
         * Usually used with DELETE, UPDATE operations.
         */
        public static final String numCopiedRows = "numCopiedRows";

        /**
         * Time taken to execute the entire operation.
         *
         * Usually used with DELETE, DELETE_PARTITIONS, TRUNCATE, MERGE, UPDATE operations.
         */
        public static final String executionTimeMs = "executionTimeMs";

        /**
         * Time taken to scan the files for matches.
         *
         * Usually used with DELETE, DELETE_PARTITIONS, MERGE, UPDATE operations.
         */
        public static final String scanTimeMs = "scanTimeMs";

        /**
         * Time taken to rewrite the matched files.
         *
         * Usually used with DELETE, DELETE_PARTITIONS, MERGE, UPDATE operations.
         */
        public static final String rewriteTimeMs = "rewriteTimeMs";

        /**
         * Number of parquet files that have been converted.
         *
         * Usually used with the CONVERT operation.
         */
        public static final String numConvertedFiles = "numConvertedFiles";

        /**
         * Number of rows in the source table.
         *
         * Usually used with the MERGE operation.
         */
        public static final String numSourceRows = "numSourceRows";

        /**
         * Number of rows inserted into the target table.
         *
         * Usually used with the MERGE operation.
         */
        public static final String numTargetRowsInserted = "numTargetRowsInserted";

        /**
         * Number of rows updated in the target table.
         *
         * Usually used with the MERGE operation.
         */
        public static final String numTargetRowsUpdated = "numTargetRowsUpdated";

        /**
         * Number of rows deleted in the target table.
         *
         * Usually used with the MERGE operation.
         */
        public static final String numTargetRowsDeleted = "numTargetRowsDeleted";

        /**
         * Number of target rows copied.
         *
         * Usually used with the MERGE operation.
         */
        public static final String numTargetRowsCopied = "numTargetRowsCopied";

        /**
         * Number files added to the sink(target).
         *
         * Usually used with the MERGE operation.
         */
        public static final String numTargetFilesAdded = "numTargetFilesAdded";

        /**
         * Number of files removed from the sink(target).
         *
         * Usually used with the MERGE operation.
         */
        public static final String numTargetFilesRemoved = "numTargetFilesRemoved";

        /**
         * Number of rows updated.
         *
         * Usually used with the UPDATE operation.
         */
        public static final String numUpdatedRows = "numUpdatedRows";
    }

    ///////////////////////////////////////////////////////////////////////////
    // Operation internals, constructors, and external APIs
    ///////////////////////////////////////////////////////////////////////////

    @Nonnull
    private final Name name;

    @Nullable
    private final Map<String, String> parameters;

    @Nullable
    private final Map<String, String> metrics;

    @Nonnull
    private final Optional<String> userMetadata;

    public Operation(@Nonnull Name name) {
        this(name, Collections.emptyMap(), Collections.emptyMap(), Optional.empty());
    }

    public Operation(@Nonnull Name name, @Nullable Map<String, String> parameters) {
        this(name, parameters, Collections.emptyMap(), Optional.empty());
    }

    public Operation(@Nonnull Name name, @Nullable Map<String, String> parameters,
                     @Nullable Map<String, String> metrics) {
        this(name, parameters, metrics, Optional.empty());
    }

    public Operation(@Nonnull Name name, @Nullable Map<String, String> parameters,
                     @Nullable Map<String, String> metrics,
                     @Nonnull Optional<String> userMetadata) {
        this.name = name;
        this.parameters = parameters;
        this.metrics = metrics;
        this.userMetadata = userMetadata;
    }

    /**
     * @return operation name
     */
    @Nonnull
    public Name getName() {
        return name;
    }

    /**
     * @return operation parameters
     */
    @Nullable
    public Map<String, String> getParameters() {
        return parameters != null ? Collections.unmodifiableMap(parameters) : null;
    }

    /**
     * @return operation metrics
     */
    @Nullable
    public Map<String, String> getMetrics() {
        return metrics != null ? Collections.unmodifiableMap(metrics) : null;
    }

    /**
     * @return user metadata for this operation
     */
    @Nonnull
    public Optional<String> getUserMetadata() {
        return userMetadata;
    }
}
