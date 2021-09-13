package io.delta.standalone.actions;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoveFile removeFile = (RemoveFile) o;
        return Objects.equals(path, removeFile.path) &&
                Objects.equals(deletionTimestamp, removeFile.deletionTimestamp) &&
                Objects.equals(dataChange, removeFile.dataChange) &&
                Objects.equals(extendedFileMetadata, removeFile.extendedFileMetadata) &&
                Objects.equals(partitionValues, removeFile.partitionValues) &&
                Objects.equals(size, removeFile.size) &&
                Objects.equals(tags, removeFile.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, deletionTimestamp, dataChange, extendedFileMetadata,
                partitionValues, size, tags);
    }

    /**
     * @return a new {@code RemoveFile.Builder}
     */
    public static Builder builder(String path) {
        return new Builder(path);
    }

    /**
     * Builder class for RemoveFile. Enables construction of RemoveFile object with default values.
     */
    public static class Builder {
        // required RemoveFile fields
        private final String path;
        // optional RemoveFile fields
        private Optional<Long> deletionTimestamp = Optional.empty();
        private boolean dataChange = true;
        private boolean extendedFileMetadata = false;
        private Map<String, String> partitionValues;
        private long size = 0;
        private Map<String, String> tags;

        public Builder(String path) {
            this.path = path;
        }

        public Builder deletionTimestamp(Long deletionTimestamp) {
            this.deletionTimestamp = Optional.of(deletionTimestamp);
            return this;
        }

        public Builder dataChange(boolean dataChange) {
            this.dataChange = dataChange;
            return this;
        }

        public Builder extendedFileMetadata(boolean extendedFileMetadata) {
            this.extendedFileMetadata = extendedFileMetadata;
            return this;
        }

        public Builder partitionValues(Map<String, String> partitionValues) {
            this.partitionValues = partitionValues;
            return this;
        }

        public Builder size(long size) {
            this.size = size;
            return this;
        }

        public Builder tags(Map<String, String> tags) {
            this.tags = tags;
            return this;
        }

        /**
         * @return a new {@code RemoveFile} with the same properties as {@code this}
         */
        public RemoveFile build() {
            RemoveFile removeFile = new RemoveFile(
                    this.path,
                    this.deletionTimestamp,
                    this.dataChange,
                    this.extendedFileMetadata,
                    this.partitionValues,
                    this.size,
                    this.tags);
            return removeFile;
        }
    }
}
