package io.delta.standalone.actions;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;

/**
 * Adds a new file to the table. The path of a file acts as the primary key for
 * the entry in the set of files. When multiple {@code AddFile} actions are
 * seen with the same {@code path} only the metadata from the last one is kept.
 */
public final class AddFile {
    private final String path;
    private final Map<String, String> partitionValues;
    private final long size;
    private final long modificationTime;
    private final boolean dataChange;
    private final String stats;
    private final Map<String, String> tags;

    public AddFile(String path, Map<String, String> partitionValues, long size,
                   long modificationTime, boolean dataChange, String stats,
                   Map<String, String> tags) {
        this.path = path;
        this.partitionValues = partitionValues;
        this.size = size;
        this.modificationTime = modificationTime;
        this.dataChange = dataChange;
        this.stats = stats;
        this.tags = tags;
    }

    /**
     * @return the relative path, from the root of the table, to the data file
     *         that should be added to the table
     */
    public String getPath() {
        return path;
    }

    /**
     * @return the URI for the data file that this {@code AddFile} represents
     * @throws URISyntaxException if path violates <a href="https://www.ietf.org/rfc/rfc2396.txt">RFC 2396</a>
     */
    public URI getPathAsUri() throws URISyntaxException {
        return new URI(path);
    }

    /**
     * @return an unmodifiable {@code Map} from partition column to value for
     *         this file
     * @see <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Partition-Value-Serialization" target="_blank">Delta Protocol Partition Value Serialization</a>
     */
    public Map<String, String> getPartitionValues() {
        return Collections.unmodifiableMap(partitionValues);
    }

    /**
     * @return the size of this file in bytes
     */
    public long getSize() {
        return size;
    }

    /**
     * @return the time that this file was last modified or created, as
     *         milliseconds since the epoch
     */
    public long getModificationTime() {
        return modificationTime;
    }

    /**
     * @return whether any data was changed as a result of this file being
     *         created
     */
    public boolean isDataChange() {
        return dataChange;
    }

    /**
     * @return statistics (for example: count, min/max values for columns)
     *         about the data in this file as serialized JSON
     */
    public String getStats() {
        return stats;
    }

    /**
     * @return an unmodifiable {@code Map} of user-defined metadata for this
     *         file
     */
    public Map<String, String> getTags() {
        return Collections.unmodifiableMap(tags);
    }
}
