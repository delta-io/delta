package io.delta.flink.source.internal.enumerator.processor;

/**
 * Extension of {@link TableProcessor} for
 * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
 * mode where Delta table changes should be also processed.
 */
public interface ContinuousTableProcessor extends TableProcessor {

    /**
     * @return Indicates whether {@link ContinuousTableProcessor} started processing Delta table
     * changes.
     */
    boolean isMonitoringForChanges();
}
