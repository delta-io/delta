package io.delta.standalone.actions;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class RemoveFile implements FileAction {
    private final String path;
    private final Optional<Long> deletionTimestamp;
    private final boolean dataChange;
    private final boolean extendedFileMetadata;
    private final Map<String, String> partitionValues;
    private final long size;
    private final Map<String, String> tags;

    public RemoveFile(String path, Optional<Long> deletionTimestamp, boolean dataChange,
                      boolean extendedFileMetadata, Map<String, String> partitionValues, long size,
                      Map<String, String> tags) {
        this.path = path;
        this.deletionTimestamp = deletionTimestamp;
        this.dataChange = dataChange;
        this.extendedFileMetadata = extendedFileMetadata;
        this.partitionValues = partitionValues;
        this.size = size;
        this.tags = tags;
    }

    @Override
    public String getPath() {
        return path;
    }

    public Optional<Long> getDeletionTimestamp() {
        return deletionTimestamp;
    }

    @Override
    public boolean isDataChange() {
        return dataChange;
    }

    public boolean isExtendedFileMetadata() {
        return extendedFileMetadata;
    }

    public Map<String, String> getPartitionValues() {
        return Collections.unmodifiableMap(partitionValues);
    }

    public long getSize() {
        return size;
    }

    public Map<String, String> getTags() {
        return Collections.unmodifiableMap(tags);
    }
}
