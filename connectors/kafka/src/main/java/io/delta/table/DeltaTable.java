package io.delta.table;

import org.apache.iceberg.*;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.List;
import java.util.Map;

public class DeltaTable implements Table {

    public DeltaTable(TableIdentifier ident, Configuration conf, String deltaTableLocation) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void refresh() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public TableScan newScan() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Schema schema() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Map<Integer, Schema> schemas() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public PartitionSpec spec() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Map<Integer, PartitionSpec> specs() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public SortOrder sortOrder() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Map<Integer, SortOrder> sortOrders() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Map<String, String> properties() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public String location() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Snapshot currentSnapshot() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Iterable<Snapshot> snapshots() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public List<HistoryEntry> history() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public UpdateSchema updateSchema() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public UpdatePartitionSpec updateSpec() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public UpdateProperties updateProperties() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ReplaceSortOrder replaceSortOrder() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public UpdateLocation updateLocation() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public AppendFiles newAppend() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public RewriteFiles newRewrite() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public RewriteManifests rewriteManifests() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public OverwriteFiles newOverwrite() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public RowDelta newRowDelta() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ReplacePartitions newReplacePartitions() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DeleteFiles newDelete() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ExpireSnapshots expireSnapshots() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ManageSnapshots manageSnapshots() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Transaction newTransaction() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public FileIO io() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public EncryptionManager encryption() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public LocationProvider locationProvider() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public List<StatisticsFile> statisticsFiles() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Map<String, SnapshotRef> refs() {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
