package io.delta.kernel.internal.metrics;

import java.util.Optional;
import java.util.UUID;

import io.delta.kernel.metrics.DeltaOperationReport;
import static java.util.Objects.requireNonNull;

/** Basic POJO implementation of {@link DeltaOperationReport} */
public abstract class DeltaOperationReportImpl implements DeltaOperationReport {

    private final String tablePath;
    private final UUID reportUUID;
    private final Optional<Exception> exception;

    protected DeltaOperationReportImpl(
        String tablePath,
        Optional<Exception> exception) {
        this.tablePath = requireNonNull(tablePath);
        this.reportUUID = UUID.randomUUID();
        this.exception = requireNonNull(exception);
    }

    @Override
    public String getTablePath() {
        return tablePath;
    }

    @Override
    public UUID getReportUUID() {
        return reportUUID;
    }

    @Override
    public Optional<Exception> getException() {
        return exception;
    }
}
