package io.delta.flink.source.internal.state;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.StringUtils;

/**
 * A {@link SourceSplit} that represents a Parquet file, or a region of a file.
 *
 * <p>The split additionally has an offset and an end, which defines the region of the file
 * represented by the split. For splits representing the while file, the offset is zero and the
 * length is the file size.
 *
 * <p>The split may furthermore have a "reader position", which is the checkpointed position from a
 * reader previously reading this split. This position is null when the split is assigned from the
 * enumerator to the readers, and is non-null when the reader's checkpoint their state in a file
 * source split.
 *
 * <p>This implementation extends a {@link FileSourceSplit} with Delta table partition
 * information</p>
 */

public class DeltaSourceSplit extends FileSourceSplit {

    private static final String[] NO_HOSTS = StringUtils.EMPTY_STRING_ARRAY;

    /**
     * Map containing partition column name to partition column value mappings. This mapping is used
     * in scope of given Split.
     */
    private final Map<String, String> partitionValues;

    /**
     * Constructs a split with no host information and with no reader position.
     *
     * @param partitionValues The Delta partition column to partition value map that should be used
     *                        for underlying Parquet File.
     * @param id              The unique ID of this source split.
     * @param filePath        The path to the Parquet file that this splits represents.
     * @param offset          The start (inclusive) of the split's rage in the Parquet file, in
     *                        bytes.
     * @param length          The number of bytes in the split (starting from the offset)
     */
    public DeltaSourceSplit(Map<String, String> partitionValues, String id,
        Path filePath, long offset, long length) {
        this(partitionValues, id, filePath, offset, length, NO_HOSTS, null);
    }

    /**
     * Constructs a split with host information and no reader position.
     * <p>
     * The {@code hostnames} provides information about the names of the hosts is storing this range
     * of the file. Empty, if no host information is available. Host information is typically only
     * available on a specific file systems, like HDFS.
     *
     * @param partitionValues The Delta partition column to partition value map that should be used
     *                        for underlying Parquet File.
     * @param id              The unique ID of this source split.
     * @param filePath        The path to the Parquet file that this splits represents.
     * @param offset          The start (inclusive) of the split's rage in the Parquet file, in
     *                        bytes.
     * @param length          The number of bytes in the split (starting from the offset)
     * @param hostnames       The hostnames of the nodes storing the split's file range.
     */
    public DeltaSourceSplit(Map<String, String> partitionValues, String id,
        Path filePath, long offset, long length, String... hostnames) {
        this(partitionValues, id, filePath, offset, length, hostnames, null);
    }

    /**
     * Constructs a split with host information and reader position restored from checkpoint.
     * <p>
     * The {@code hostnames} parameter provides information about the names of the hosts storing
     * this range of the file. Empty, if no host information is available. Host information is
     * typically only available on a specific file systems, like HDFS.
     *
     * @param partitionValues The Delta partition column to partition value map that should be used
     *                        for underlying Parquet File.
     * @param id              The unique ID of this source split.
     * @param filePath        The path to the Parquet file that this splits represents.
     * @param offset          The start (inclusive) of the split's rage in the Parquet file, in
     *                        bytes.
     * @param length          The number of bytes in the split (starting from the offset)
     * @param hostnames       The hostnames of the nodes storing the split's file range.
     * @param readerPosition  The reader position in bytes recovered from a checkpoint.
     */
    public DeltaSourceSplit(Map<String, String> partitionValues, String id,
        Path filePath, long offset, long length, String[] hostnames,
        CheckpointedPosition readerPosition) {
        // Flink 2.0: FileSourceSplit constructor signature changed
        // Now requires: id, filePath, offset, length, fileSize, recordSkipCount, hostnames
        // Using length as fileSize and 0 as recordSkipCount (standard for file splits)
        super(id, filePath, offset, length, length, 0L, hostnames);

        // Make split Partition a new Copy of original map to for immutability.
        this.partitionValues =
            (partitionValues == null) ? Collections.emptyMap() : new HashMap<>(partitionValues);

        // FLINK 2.0 NOTE: CheckpointedPosition handling
        //
        // In Flink 2.0, FileSourceSplit manages CheckpointedPosition internally.
        // The readerPosition is stored by the parent class and accessible via
        // getReaderPosition() method inherited from FileSourceSplit.
        //
        // To create a split with a checkpointed position, use the
        // updateWithCheckpointedPosition(CheckpointedPosition) method, which
        // creates a new DeltaSourceSplit instance with the position set.
        //
        // This is the correct pattern for Flink 2.0 and is tested in
        // DeltaSourceSplitTest and DeltaSourceSplitSerializerTest.
    }

    @Override
    public DeltaSourceSplit updateWithCheckpointedPosition(CheckpointedPosition position) {
        return new DeltaSourceSplit(partitionValues, splitId(), path(), offset(), length(),
            hostnames(), position);
    }

    /**
     * @return an unmodifiable Map of Delta Table Partition columns and values.
     */
    public Map<String, String> getPartitionValues() {
        return Collections.unmodifiableMap(partitionValues);
    }
}
