package io.delta.table;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableMap;
import io.delta.kernel.internal.actions.Metadata;
import org.apache.iceberg.*;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.DateTimeUtil;

import java.util.List;
import java.util.Map;


public class DeltaTable implements org.apache.iceberg.Table {

    private final TableIdentifier ident;
    private final String deltaTableLocation;
    private final Table deltaTable;
    private final Engine deltaEngine;
    private final HadoopFileIO io;
    private final LoadingCache<Long, DeltaSnapshot> snapshots;

    private DeltaSnapshot currentVersion = null;

    private long currentVersionId = 0;

    public DeltaTable(TableIdentifier ident, Configuration conf, String deltaTableLocation) {
        this.ident = ident;
        this.deltaTableLocation = deltaTableLocation;
        this.deltaEngine = DefaultEngine.create(conf);
        this.deltaTable = Table.forPath(deltaEngine, deltaTableLocation);
        this.io = new HadoopFileIO(conf);
        this.snapshots =
                Caffeine.newBuilder()
                        .build(
                                version -> new DeltaSnapshot(
                                        location(),
                                        deltaTable.getSnapshotAsOfVersion(deltaEngine, version), deltaEngine));

        refresh();
    }

    @Override
    public void refresh() {
        this.currentVersionId = deltaTable.getLatestSnapshot(deltaEngine).getVersion(deltaEngine);
        this.currentVersion = snapshots.get(currentVersionId);
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

    private Map<String, String> asProperties(Metadata metadata) {
        // TODO: construct NameMapping from the schema
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

        builder.putAll(metadata.getConfiguration());
        builder.put("format", "delta/" + metadata.getFormat().getProvider());
        builder.put(TableProperties.UUID, metadata.getId());
        metadata.getDescription().ifPresent(desc -> builder.put("comment", desc));
        metadata
                .getCreatedTime()
                .ifPresent(ts -> builder.put("created-at", DateTimeUtil.formatTimestampMillis(ts)));

        return builder.build();
    }

    @Override
    public Map<String, String> properties() {
        return asProperties(currentVersion.metadata());
    }

    @Override
    public String location() {
        return deltaTableLocation;
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
