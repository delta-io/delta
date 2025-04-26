package io.delta.table;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.snapshot.LogSegment;
import org.apache.iceberg.*;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.mapping.NameMapping;

import java.util.List;
import java.util.Map;

public class DeltaSnapshot implements Snapshot, HistoryEntry {

    private final SnapshotImpl wrapped;
    private final Metadata metadata;
    private final LogSegment log;
    private final Engine deltaEngine;
    private final String location;

    private NameMapping mapping = null;

    private Schema schema = null;

    private PartitionSpec spec = null;

    private boolean hasMissingFieldIds = true;

    public io.delta.kernel.Snapshot deltaSnapshot() {
        return wrapped;
    }

    public PartitionSpec spec() {
        if (spec == null) {
            this.spec =
                    DeltaPartitionUtil.convert(
                            schema(), DeltaFileUtil.asStringList(metadata.getPartitionColumns()));
        }

        return spec;
    }

    NameMapping nameMapping() {
        if (null == mapping) {
            String nameMappingStr = metadata.getConfiguration().get(DeltaTable.NAME_MAPPING_KEY);
            if (nameMappingStr != null) {
                this.mapping = NameMappingParser.fromJson(nameMappingStr);
            }
        }

        return mapping;
    }

    Metadata metadata() {
        return metadata;
    }

    int lastAssignedFieldId() {
        Map<String, String> config = metadata.getConfiguration();
        String lastAssignedIdString = config.get(DeltaTable.LAST_ASSIGNED_ID_KEY);
        if (lastAssignedIdString != null) {
            return Integer.parseInt(lastAssignedIdString);
        } else {
            return 0;
        }
    }

    public Schema schema() {
        if (null == schema) {
            // use the current version as the schema ID to ensure uniqueness
            int schemaId = Math.toIntExact(log.version);
            int lastAssignedId = lastAssignedFieldId();
            Pair<NameMapping, Integer> updatedMapping =
                    DeltaTypeUtil.updateNameMapping(metadata.getSchema(), nameMapping(), lastAssignedId);

            // if the updated mapping contains new IDs, then some field IDs were missing
            this.hasMissingFieldIds = updatedMapping.second() != lastAssignedId;

            // convert the schema with the updated mapping so that the snapshot can be read
            this.schema =
                    new Schema(
                            schemaId,
                            DeltaTypeUtil.convert(metadata.getSchema(), updatedMapping.first()).fields());
        }

        return schema;
    }

    DeltaSnapshot(String location, io.delta.kernel.Snapshot snapshot, Engine deltaEngine) {
        this.location = location;
        this.wrapped = (SnapshotImpl) snapshot;
        this.metadata = wrapped.getMetadata();
        this.log = wrapped.getLogSegment();
        this.deltaEngine = deltaEngine;
    }

    @Override
    public long sequenceNumber() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public long snapshotId() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Long parentId() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public long timestampMillis() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public List<ManifestFile> allManifests(FileIO io) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public List<ManifestFile> dataManifests(FileIO io) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public List<ManifestFile> deleteManifests(FileIO io) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public String operation() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Map<String, String> summary() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Iterable<DataFile> addedDataFiles(FileIO io) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Iterable<DataFile> removedDataFiles(FileIO io) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public String manifestListLocation() {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
