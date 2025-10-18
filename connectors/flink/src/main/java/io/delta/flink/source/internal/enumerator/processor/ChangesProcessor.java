package io.delta.flink.source.internal.enumerator.processor;

import java.util.List;
import java.util.function.Consumer;

import io.delta.flink.internal.options.DeltaConnectorConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.enumerator.monitor.ChangesPerVersion;
import io.delta.flink.source.internal.enumerator.monitor.TableMonitor;
import io.delta.flink.source.internal.enumerator.monitor.TableMonitorResult;
import io.delta.flink.source.internal.exceptions.DeltaSourceException;
import io.delta.flink.source.internal.exceptions.DeltaSourceExceptions;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpointBuilder;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;

/**
 * This implementation of {@link TableProcessor} process only Delta table changes starting from
 * specified {@link io.delta.standalone.Snapshot} version. This implementation does not read {@code
 * Snapshot} content.
 *
 * <p>
 * The {@code Snapshot} version is specified by {@link TableMonitor} used when creating an instance
 * of {@code ChangesProcessor}.
 */
public class ChangesProcessor extends TableProcessorBase implements ContinuousTableProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ChangesProcessor.class);

    /**
     * The {@link TableMonitor} instance used to monitor Delta table for changes.
     */
    private final TableMonitor tableMonitor;

    /**
     * A {@link SplitEnumeratorContext} used for this {@code ChangesProcessor}.
     */
    private final SplitEnumeratorContext<DeltaSourceSplit> enumContext;

    /**
     * An interval value in milliseconds to periodically check the Delta table for new changes.
     */
    private final long checkInterval;

    /**
     * A delay value in milliseconds for first check of Delta table for new changes.
     */
    private final long initialDelay;

    /**
     * A {@link Snapshot} version that this processor used as a starting version to get changes from
     * Delta table.
     * <p>
     * This value will be updated while processing every version from {@link TableMonitorResult}.
     */
    private long currentSnapshotVersion;

    public ChangesProcessor(
        Path deltaTablePath, TableMonitor tableMonitor,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        DeltaConnectorConfiguration sourceConfiguration) {
        super(deltaTablePath, fileEnumerator);
        this.tableMonitor = tableMonitor;
        this.enumContext = enumContext;
        this.currentSnapshotVersion = this.tableMonitor.getMonitorVersion();

        this.checkInterval = sourceConfiguration.getValue(DeltaSourceOptions.UPDATE_CHECK_INTERVAL);
        this.initialDelay =
            sourceConfiguration.getValue(DeltaSourceOptions.UPDATE_CHECK_INITIAL_DELAY);
    }

    /**
     * Starts processing changes that were added to Delta table starting from version specified by
     * {@link #currentSnapshotVersion} field by converting them to {@link DeltaSourceSplit}
     * objects.
     *
     * @param processCallback A {@link Consumer} callback that will be called after processing all
     *                        {@link io.delta.standalone.actions.Action} and converting them to
     *                        {@link DeltaSourceSplit}. This callback will be executed for every new
     *                        discovered Delta table version.
     */
    @Override
    public void process(Consumer<List<DeltaSourceSplit>> processCallback) {
        //monitor for changes
        enumContext.callAsync(
            tableMonitor, // executed sequentially by ScheduledPool Thread.
            (tableMonitorResult, throwable) -> processDiscoveredVersions(tableMonitorResult,
                processCallback, throwable), // executed by Flink's Source-Coordinator Thread.
            initialDelay, checkInterval);
    }

    /**
     * @return A {@link Snapshot} version that this processor used as a starting version to get
     * changes from Delta table. The method can return different values for every method call.
     */
    @Override
    public long getSnapshotVersion() {
        return this.currentSnapshotVersion;
    }

    @Override
    public DeltaEnumeratorStateCheckpointBuilder<DeltaSourceSplit> snapshotState(
        DeltaEnumeratorStateCheckpointBuilder<DeltaSourceSplit> checkpointBuilder) {
        return checkpointBuilder.withMonitoringForChanges(isMonitoringForChanges());
    }

    /**
     * @return return always true indicating that this processor process only changes.
     */
    @Override
    public boolean isMonitoringForChanges() {
        return true;
    }

    /**
     * Process all versions discovered by {@link TableMonitor} in the latest Table check.
     *
     * @param monitorTableResult Result of {@link TableMonitor} table check.
     * @param processCallback    A callback that should be called while processing Delta table
     *                           changes.
     * @param error              An error that was returned by the monitoring thread. Can be null.
     */
    private void processDiscoveredVersions(
        TableMonitorResult monitorTableResult, Consumer<List<DeltaSourceSplit>> processCallback,
        Throwable error) {
        if (error != null) {
            LOG.error("Failed to enumerate files", error);
            if (error instanceof DeltaSourceException) {
                throw (DeltaSourceException) error;
            }

            throw DeltaSourceExceptions.tableMonitorException(
                SourceUtils.pathToString(deltaTablePath), error);
        }

        monitorTableResult.getChanges()
            .forEach(changesPerVersion -> processVersion(processCallback, changesPerVersion));
    }

    /**
     * Process changes from individual Delta table version.
     *
     * @param processCallback   A callback that should be called while processing Delta table
     *                          changes.
     * @param changesPerVersion The {@link ChangesPerVersion} object containing {@link Action}s for
     *                          given {@link ChangesPerVersion#getSnapshotVersion()} version.
     */
    private void processVersion(
        Consumer<List<DeltaSourceSplit>> processCallback,
        ChangesPerVersion<AddFile> changesPerVersion) {

        // This may look like TableMonitor#monitorVersion field. However, TableMonitor's field
        // will be updated on a different thread than this method here is executed. So to avoid
        // any race conditions and visibility issues caused by updating and reading field from two
        // threads, we are using data from TableMonitorResult.
        // From ChangesProcessor perspective we only need to know what is the next version that
        // we used as deltaLog.getChanges(version, boolean) and this will be this here.
        this.currentSnapshotVersion = changesPerVersion.getSnapshotVersion() + 1;

        List<DeltaSourceSplit> splits = prepareSplits(changesPerVersion, (path) -> true);
        processCallback.accept(splits);
    }
}
