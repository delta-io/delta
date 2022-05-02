package io.delta.flink.source.internal.enumerator.monitor;

import java.util.List;

import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;

/**
 * The result object for {@link TableMonitor#call()} method. It contains Lists of {@link Action} per
 * {@link io.delta.standalone.Snapshot} versions for monitored Delta table.
 */
public class TableMonitorResult {

    /**
     * An ordered list of {@link ChangesPerVersion}. Elements of this list represents Delta table
     * changes per version in ASC version order.
     */
    private final List<ChangesPerVersion<AddFile>> changesPerVersion;

    public TableMonitorResult(List<ChangesPerVersion<AddFile>> changesPerVersion) {
        this.changesPerVersion = changesPerVersion;
    }

    public List<ChangesPerVersion<AddFile>> getChanges() {
        return changesPerVersion;
    }

}
