package io.delta.flink.source.internal.enumerator;

import java.util.Collection;
import java.util.Collections;
import static java.util.Collections.emptyList;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.enumerator.monitor.TableMonitor;
import io.delta.flink.source.internal.enumerator.processor.ActionProcessor;
import io.delta.flink.source.internal.enumerator.processor.ChangesProcessor;
import io.delta.flink.source.internal.enumerator.processor.ContinuousTableProcessor;
import io.delta.flink.source.internal.enumerator.processor.SnapshotAndChangesTableProcessor;
import io.delta.flink.source.internal.enumerator.processor.SnapshotProcessor;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import static io.delta.flink.source.internal.DeltaSourceOptions.LOADED_SCHEMA_SNAPSHOT_VERSION;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_TIMESTAMP;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_VERSION;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;

/**
 * An implementation of {@link SplitEnumeratorProvider} that creates a {@code
 * ContinuousSplitEnumerator} used for {@link Boundedness#CONTINUOUS_UNBOUNDED} mode.
 */
public class ContinuousSplitEnumeratorProvider implements SplitEnumeratorProvider {

    private final FileSplitAssigner.Provider splitAssignerProvider;

    private final AddFileEnumerator.Provider<DeltaSourceSplit> fileEnumeratorProvider;

    /**
     * @param splitAssignerProvider  an instance of {@link FileSplitAssigner.Provider} that will be
     *                               used for building a {@code ContinuousSplitEnumerator} by
     *                               factory methods.
     * @param fileEnumeratorProvider an instance of {@link AddFileEnumerator.Provider} that will be
     *                               used for building a {@code ContinuousSplitEnumerator} by
     *                               factory methods.
     */
    public ContinuousSplitEnumeratorProvider(
        FileSplitAssigner.Provider splitAssignerProvider,
        AddFileEnumerator.Provider<DeltaSourceSplit> fileEnumeratorProvider) {
        this.splitAssignerProvider = splitAssignerProvider;
        this.fileEnumeratorProvider = fileEnumeratorProvider;
    }

    @Override
    public ContinuousDeltaSourceSplitEnumerator createInitialStateEnumerator(
            Path deltaTablePath, Configuration configuration,
            SplitEnumeratorContext<DeltaSourceSplit> enumContext,
            DeltaSourceConfiguration sourceConfiguration) {

        DeltaLog deltaLog =
            DeltaLog.forTable(configuration, SourceUtils.pathToString(deltaTablePath));

        // Getting the same snapshot that was used for schema discovery in Source Builder.
        // With this we are making sure that what we read from Delta will have the same schema
        // that was discovered in Source builder.
        Snapshot initSnapshot = deltaLog.getSnapshotForVersionAsOf(
            sourceConfiguration.getValue(LOADED_SCHEMA_SNAPSHOT_VERSION));

        ContinuousTableProcessor tableProcessor =
            createTableProcessor(
                deltaTablePath, enumContext, sourceConfiguration, deltaLog, initSnapshot);

        return new ContinuousDeltaSourceSplitEnumerator(
            deltaTablePath, tableProcessor, splitAssignerProvider.create(emptyList()), enumContext);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ContinuousDeltaSourceSplitEnumerator createEnumeratorForCheckpoint(
            DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint,
            Configuration configuration,
            SplitEnumeratorContext<DeltaSourceSplit> enumContext,
            DeltaSourceConfiguration sourceConfiguration) {

        ContinuousTableProcessor tableProcessor =
            createTableProcessorFromCheckpoint(checkpoint, configuration, enumContext,
                sourceConfiguration);

        Collection<FileSourceSplit> checkpointSplits =
            (Collection<FileSourceSplit>) (Collection<?>) checkpoint.getSplits();

        return new ContinuousDeltaSourceSplitEnumerator(
            checkpoint.getDeltaTablePath(), tableProcessor, splitAssignerProvider.create(
            checkpointSplits), enumContext);
    }

    /**
     * @return A {@link ContinuousTableProcessor} instance using
     * {@link DeltaEnumeratorStateCheckpoint}
     * data.
     * <p>
     * @implNote If {@link DeltaSourceOptions#STARTING_VERSION} or {@link
     * DeltaSourceOptions#STARTING_TIMESTAMP} options were defined or if Enumerator already
     * processed initial Snapshot, the returned ContinuousTableProcessor instance will process only
     * changes applied to monitored Delta table.
     */
    private ContinuousTableProcessor createTableProcessorFromCheckpoint(
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration) {
        long snapshotVersion = checkpoint.getSnapshotVersion();

        Path deltaTablePath = checkpoint.getDeltaTablePath();
        DeltaLog deltaLog =
            DeltaLog.forTable(configuration, SourceUtils.pathToString(deltaTablePath));

        if (checkpoint.isMonitoringForChanges()) {
            return createChangesProcessor(deltaTablePath, enumContext, sourceConfiguration,
                deltaLog, snapshotVersion);
        } else {
            return
                createSnapshotAndChangesProcessor(deltaTablePath, enumContext, sourceConfiguration,
                    deltaLog, deltaLog.getSnapshotForVersionAsOf(snapshotVersion));
        }
    }

    /**
     * @return A {@link ContinuousTableProcessor} instance.
     * <p>
     * @implNote If {@link DeltaSourceOptions#STARTING_VERSION} or {@link
     * DeltaSourceOptions#STARTING_TIMESTAMP} options were defined the returned
     * ContinuousTableProcessor instance will process only changes applied to monitored Delta
     * table.
     */
    private ContinuousTableProcessor createTableProcessor(
        Path deltaTablePath, SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration, DeltaLog deltaLog, Snapshot snapshot) {

        if (isChangeStreamOnly(sourceConfiguration)) {
            return
                createChangesProcessor(deltaTablePath, enumContext, sourceConfiguration, deltaLog,
                    snapshot.getVersion());
        } else {
            return
                createSnapshotAndChangesProcessor(deltaTablePath, enumContext, sourceConfiguration,
                    deltaLog, snapshot);
        }
    }

    private ChangesProcessor createChangesProcessor(
        Path deltaTablePath, SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration, DeltaLog deltaLog,
        long monitorSnapshotVersion) {

        ActionProcessor actionProcessor = new ActionProcessor(
            sourceConfiguration.getValue(DeltaSourceOptions.IGNORE_CHANGES),
            sourceConfiguration.getValue(DeltaSourceOptions.IGNORE_DELETES));

        TableMonitor tableMonitor =
            new TableMonitor(deltaLog, monitorSnapshotVersion, sourceConfiguration.getValue(
                DeltaSourceOptions.UPDATE_CHECK_INTERVAL), actionProcessor);

        return new ChangesProcessor(
            deltaTablePath, tableMonitor, enumContext, fileEnumeratorProvider.create(),
            sourceConfiguration);
    }

    private ContinuousTableProcessor createSnapshotAndChangesProcessor(Path deltaTablePath,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration, DeltaLog deltaLog, Snapshot snapshot) {

        // Since this is the processor for both snapshot and changes, the version for which we
        // should start monitoring for changes is snapshot.version + 1. We don't want to get
        // changes from snapshot.version.
        ChangesProcessor changesProcessor =
            createChangesProcessor(deltaTablePath, enumContext, sourceConfiguration, deltaLog,
                snapshot.getVersion() + 1);

        SnapshotProcessor snapshotProcessor =
            new SnapshotProcessor(deltaTablePath, snapshot, fileEnumeratorProvider.create(),
                Collections.emptySet());

        return new SnapshotAndChangesTableProcessor(snapshotProcessor, changesProcessor);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    private boolean isChangeStreamOnly(DeltaSourceConfiguration sourceConfiguration) {
        return
            sourceConfiguration.hasOption(STARTING_VERSION) ||
                sourceConfiguration.hasOption(STARTING_TIMESTAMP);
    }
}
