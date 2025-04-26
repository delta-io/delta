package io.delta.table;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.snapshot.LogSegment;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.io.FileIO;

import java.util.List;
import java.util.Map;

public class DeltaSnapshot implements Snapshot, HistoryEntry {

    private final SnapshotImpl wrapped;
    private final Metadata metadata;
    private final LogSegment log;
    private final Engine deltaEngine;
    private final String location;

    Metadata metadata() {
        return metadata;
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
