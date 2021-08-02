package io.delta.standalone.actions;

import java.util.Map;

public class AddCDCFile implements FileAction {
    private final String path;
    private final Map<String, String> partitionValues;
    private final long size;
    private final Map<String, String> tags;

    public AddCDCFile(String path, Map<String, String> partitionValues, long size, Map<String, String> tags) {
        this.path = path;
        this.partitionValues = partitionValues;
        this.size = size;
        this.tags = tags;
    }

    @Override
    public String getPath() {
        return path;
    }

    public Map<String, String> getPartitionValues() {
        return partitionValues;
    }

    public long getSize() {
        return size;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public boolean isDataChange() {
        return false;
    }
}
