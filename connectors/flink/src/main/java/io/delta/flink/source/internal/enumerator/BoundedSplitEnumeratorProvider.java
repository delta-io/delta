package io.delta.flink.source.internal.enumerator;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import io.delta.flink.internal.options.DeltaConnectorConfiguration;
import io.delta.flink.internal.options.PartitionFilterOptionTypeConverter;
import io.delta.flink.source.internal.enumerator.processor.SnapshotProcessor;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.source.internal.DeltaSourceOptions.LOADED_SCHEMA_SNAPSHOT_VERSION;
import static io.delta.flink.source.internal.DeltaSourceOptions.PARTITION_FILTERS;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;

/**
 * An implementation of {@link SplitEnumeratorProvider} that creates a {@code
 * BoundedSplitEnumerator} used for {@link Boundedness#BOUNDED} mode.
 */
public class BoundedSplitEnumeratorProvider implements SplitEnumeratorProvider {
    private static final Logger LOG = LoggerFactory.getLogger(BoundedSplitEnumeratorProvider.class);

    private final FileSplitAssigner.Provider splitAssignerProvider;

    private final AddFileEnumerator.Provider<DeltaSourceSplit> fileEnumeratorProvider;

    /**
     * @param splitAssignerProvider  an instance of {@link FileSplitAssigner.Provider} that will be
     *                               used for building a {@code BoundedSplitEnumerator} by factory
     *                               methods.
     * @param fileEnumeratorProvider an instance of {@link AddFileEnumerator.Provider} that will be
     *                               used for building a {@code BoundedSplitEnumerator} by factory
     *                               methods.
     */
    public BoundedSplitEnumeratorProvider(
            FileSplitAssigner.Provider splitAssignerProvider,
            AddFileEnumerator.Provider<DeltaSourceSplit> fileEnumeratorProvider) {
        this.splitAssignerProvider = splitAssignerProvider;
        this.fileEnumeratorProvider = fileEnumeratorProvider;
    }

    @Override
    public BoundedDeltaSourceSplitEnumerator createInitialStateEnumerator(
            Path deltaTablePath, Configuration configuration,
            SplitEnumeratorContext<DeltaSourceSplit> enumContext,
            DeltaConnectorConfiguration sourceConfiguration) {

        DeltaLog deltaLog =
            DeltaLog.forTable(configuration, SourceUtils.pathToString(deltaTablePath));

        // Getting the same snapshot that was used for schema discovery in Source Builder.
        // With this we are making sure that what we read from Delta will have the same schema
        // that was discovered in Source builder.
        Snapshot initSnapshot = deltaLog.getSnapshotForVersionAsOf(
            sourceConfiguration.getValue(LOADED_SCHEMA_SNAPSHOT_VERSION));

        Map<String, Set<String>> partitionsToFilter =
                PartitionFilterOptionTypeConverter.parseStringToMap(
                        sourceConfiguration.getValue(PARTITION_FILTERS));
        LOG.info("Initialized provider with partitions to filter: {}",
                partitionsToFilter);

        SnapshotProcessor snapshotProcessor =
            new SnapshotProcessor(deltaTablePath, initSnapshot,
                fileEnumeratorProvider.create(), Collections.emptySet());

        return new BoundedDeltaSourceSplitEnumerator(
            deltaTablePath, snapshotProcessor, splitAssignerProvider.create(Collections.emptyList()),
            enumContext);
    }

    @Override
    public BoundedDeltaSourceSplitEnumerator createEnumeratorForCheckpoint(
            DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint,
            Configuration configuration,
            SplitEnumeratorContext<DeltaSourceSplit> enumContext,
            DeltaConnectorConfiguration sourceConfiguration) {

        DeltaLog deltaLog = DeltaLog.forTable(configuration,
            SourceUtils.pathToString(checkpoint.getDeltaTablePath()));

        Map<String, Set<String>> partitionsToFilter =
                PartitionFilterOptionTypeConverter.parseStringToMap(
                        sourceConfiguration.getValue(PARTITION_FILTERS));
        LOG.info("Created provider from checkpoint with partitions to filter: {}",
                partitionsToFilter);

        SnapshotProcessor snapshotProcessor =
            new SnapshotProcessor(checkpoint.getDeltaTablePath(),
                    deltaLog.getSnapshotForVersionAsOf(checkpoint.getSnapshotVersion()),
                    fileEnumeratorProvider.create(), checkpoint.getAlreadyProcessedPaths(),
                    partitionsToFilter);

        return new BoundedDeltaSourceSplitEnumerator(
            checkpoint.getDeltaTablePath(), snapshotProcessor,
            splitAssignerProvider.create(Collections.emptyList()), enumContext);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }
}
