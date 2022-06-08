package io.delta.flink.source.internal.exceptions;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import io.delta.flink.source.internal.file.AddFileEnumeratorContext;
import org.apache.flink.core.fs.Path;

import io.delta.standalone.actions.Action;

/**
 * The utility class that provides a factory methods for various cases where {@link
 * DeltaSourceException} has to be thrown.
 */
public final class DeltaSourceExceptions {

    private DeltaSourceExceptions() {

    }

    /**
     * Wraps given {@link Throwable} with {@link DeltaSourceException}. The returned exception
     * object will use {@link Throwable#toString()} on provided {@code Throwable} to get its
     * exception message.
     *
     * @param tablePath       Path to Delta table for which this exception occurred.
     * @param snapshotVersion Delta table Snapshot version for which this exception occurred.
     * @param t               {@link Throwable} that should be wrapped with {@link
     *                        DeltaSourceException}
     * @return {@link DeltaSourceException} wrapping original {@link Throwable}
     */
    public static DeltaSourceException generalSourceException(
            String tablePath,
            long snapshotVersion,
            Throwable t) {
        return new DeltaSourceException(tablePath, snapshotVersion, t);
    }

    /**
     * Creates new {@link DeltaSourceException} object that can be used for {@link IOException}
     * thrown from {@link io.delta.flink.source.internal.file.AddFileEnumerator#enumerateSplits(
     *AddFileEnumeratorContext, io.delta.flink.source.internal.file.AddFileEnumerator.SplitFilter)}
     * <p>
     * <p>
     * Wraps given {@link Throwable} with {@link DeltaSourceException}. The returned exception
     * object will use defined error message for this case.
     *
     * @param context  The {@link AddFileEnumeratorContext} for which this exception occurred.
     * @param filePath The {@link Path} for Parquet file that caused this exception.
     * @param e        Wrapped {@link IOException}
     * @return {@link DeltaSourceException} wrapping original {@code IOException}
     */
    public static DeltaSourceException fileEnumerationException(
            AddFileEnumeratorContext context,
            Path filePath,
            IOException e) {
        return new DeltaSourceException(context.getTablePath(), context.getSnapshotVersion(),
            String.format("An Exception while processing Parquet Files for path %s and version %d",
                filePath, context.getSnapshotVersion()), e);
    }

    /**
     * Creates a new DeltaSourceException with a dedicated exception message for case when {@link
     * io.delta.standalone.actions.RemoveFile} and {@link io.delta.standalone.actions.AddFile}
     * actions were recorded for the same Delta table version and "ignoreChanges" option was not
     * used.
     *
     * @param tablePath       Path to Delta table for which this exception occurred.
     * @param snapshotVersion Delta table Snapshot version for which this exception occurred.
     * @return A {@link DeltaSourceException} object.
     */
    public static DeltaSourceException deltaSourceIgnoreChangesException(
            String tablePath,
            long snapshotVersion) {

        return new DeltaSourceException(
            tablePath, snapshotVersion,
            String.format("Detected a data update in the source table at version "
                + "%d. This is currently not supported. If you'd like to ignore updates, set "
                + "the option 'ignoreChanges' to 'true'. If you would like the data update to "
                + "be reflected, please restart this query with a fresh Delta checkpoint "
                + "directory.", snapshotVersion));
    }

    /**
     * Creates a new DeltaSourceException with a dedicated exception message for case when {@link
     * io.delta.standalone.actions.RemoveFile} and {@link io.delta.standalone.actions.AddFile}
     * actions were recorded for the same Delta table version and "ignoreChanges" nor
     * "ignoreDeletes" options were used.
     *
     * @param tablePath       Path to Delta table for which this exception occurred.
     * @param snapshotVersion Delta table Snapshot version for which this exception occurred.
     * @return A {@link DeltaSourceException} object.
     */
    public static DeltaSourceException deltaSourceIgnoreDeleteException(
            String tablePath,
            long snapshotVersion) {
        return new DeltaSourceException(
            tablePath, snapshotVersion,
            String.format("Detected deleted data (for example $removedFile) from streaming source "
                + "at version %d. This is currently not supported. If you'd like to ignore deletes "
                + "set the option 'ignoreDeletes' to 'true'.", snapshotVersion));
    }

    public static DeltaSourceException tableMonitorException(
            String deltaTablePath,
            Throwable error) {
        return new DeltaSourceException(
            deltaTablePath, null,
            String.format("Exception during monitoring Delta table [%s] for changes",
                deltaTablePath), error);
    }

    /**
     * Creates a new DeltaSourceException with a dedicated exception message for case when
     * unsupported {@link Action} was recorded when processing changes from Delta table.
     *
     * @param tablePath       Path to Delta Table for which this exception occurred.
     * @param snapshotVersion Delta Table Snapshot version for which this exception occurred.
     * @param action          Unsupported {@link Action} that was recorded for given snapshot
     *                        version.
     * @return A {@link DeltaSourceException} object.
     */
    public static DeltaSourceException unsupportedDeltaActionException(
            String tablePath,
            long snapshotVersion,
            Action action) {
        return new DeltaSourceException(
            tablePath, snapshotVersion,
            String.format(
                "Got an unsupported action - [%s] when processing changes"
                    + " from version [%d] for table [%s]",
                action.getClass(), snapshotVersion, tablePath));
    }

    public static DeltaSourceValidationException invalidOptionNameException(
        String tablePath,
        String invalidOption) {

        return new DeltaSourceValidationException(
            tablePath,
            Collections.singletonList(
                String.format("Invalid option [%s] used for Delta Source Connector.",
                    invalidOption)));
    }

    public static DeltaSourceException notPartitionedTableException(String columnName) {
        return new DeltaSourceException(
            String.format(
                "Attempt to get a value for partition column from unpartitioned Delta Table. "
                    + "Column name %s", columnName));
    }

    public static DeltaSourceException missingPartitionValueException(
        String partitionName,
        Collection<String> expectedPartitionColumnNames) {
        return new DeltaSourceException(
            String.format("Cannot find the partition value in Delta MetaData for column %s. "
                    + "Expected partition column names from MetaData are %s",
                partitionName, expectedPartitionColumnNames));
    }

    public static DeltaSourceValidationException optionValidationException(
            String tablePath,
            Exception e) {
        return new DeltaSourceValidationException(
            tablePath,
            Collections.singletonList(e.getClass() + " - " + e.getMessage())
        );
    }
}
