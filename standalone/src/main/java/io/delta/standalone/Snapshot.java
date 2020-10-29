package io.delta.standalone;

import java.util.List;

import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;

import org.apache.hadoop.fs.Path;

/**
 * An immutable snapshot of the state of the log at some delta version. Internally
 * this class manages the replay of actions stored in checkpoint or delta files.
 * <p>
 * After resolving any new actions, it recalculates the transaction state and metadata.
 */
public interface Snapshot {

    /**
     * @return all of the files present in this snapshot
     */
    List<AddFile> getAllFiles();

    /**
     * @return the metadata for this snapshot
     */
    Metadata getMetadata();

    /**
     * @return the path to the log for this snapshot
     */
    Path getPath();

    /**
     * @return the version for this snapshot
     */
    long getVersion();

    /**
     * @return the {@code DeltaLog} instance for this snapshot
     */
    DeltaLog getDeltaLog();

    /**
     * @return the timestamp of the latest commit in milliseconds. Can also be to -1 if the
     *         timestamp of the commit is unknown or the table has not been initialized (that is,
     *         the version is set to -1).
     */
    long getTimestamp();

    /**
     * @return the number of files present in this snapshot
     */
    int getNumOfFiles();

    /**
     * Creates a new {@code CloseableIterator<RowRecord>} which can iterate over data located in this
     * snapshot's {@code DeltaLog.dataPath}. Iterates file by file, row by row.
     * <p>
     * This does not iterate over snapshot metadata/log files. To do that, use {@code getAllFiles}.
     * <p>
     * Provides no iteration ordering guarantee among data files.
     *
     * @return the new iterator
     */
    CloseableIterator<RowRecord> open();
}
