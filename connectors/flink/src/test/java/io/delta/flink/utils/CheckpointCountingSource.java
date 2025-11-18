package io.delta.flink.utils;

import java.io.Serializable;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;

import static io.delta.flink.sink.utils.DeltaSinkTestUtils.TEST_ROW_TYPE;

/**
 * Flink 2.0 Source API v2 implementation of CheckpointCountingSource.
 * <p>
 * Each of the source operators outputs records in given number of checkpoints. Number of records
 * per checkpoint is constant between checkpoints, and defined by user. When all records are
 * emitted, the source waits for two more checkpoints until it finishes.
 * <p>
 * Migrated from Flink 1.x SourceFunction API to Flink 2.0 Source API v2.
 */
public class CheckpointCountingSource
    implements Source<RowData, CheckpointCountingSplit, CheckpointCountingSplitState>,
    ResultTypeQueryable<RowData> {

    private final int numberOfCheckpoints;
    private final int recordsPerCheckpoint;
    private final RowProducer rowProducer;

    public CheckpointCountingSource(int recordsPerCheckpoint, int numberOfCheckpoints) {
        this(recordsPerCheckpoint, numberOfCheckpoints, new DefaultRowProducer());
    }

    public CheckpointCountingSource(
            int recordsPerCheckpoint,
            int numberOfCheckpoints,
            RowProducer rowProducer) {
        this.numberOfCheckpoints = numberOfCheckpoints;
        this.recordsPerCheckpoint = recordsPerCheckpoint;
        this.rowProducer = rowProducer;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, CheckpointCountingSplit> createReader(
            SourceReaderContext readerContext) {
        return new CheckpointCountingSourceReader(
            readerContext,
            numberOfCheckpoints,
            recordsPerCheckpoint,
            rowProducer);
    }

    @Override
    public SplitEnumerator<CheckpointCountingSplit, CheckpointCountingSplitState> createEnumerator(
            SplitEnumeratorContext<CheckpointCountingSplit> enumContext) {
        return new CheckpointCountingSplitEnumerator(
            enumContext,
            numberOfCheckpoints,
            recordsPerCheckpoint);
    }

    @Override
    public SplitEnumerator<CheckpointCountingSplit, CheckpointCountingSplitState> restoreEnumerator(
            SplitEnumeratorContext<CheckpointCountingSplit> enumContext,
            CheckpointCountingSplitState checkpoint) {
        return new CheckpointCountingSplitEnumerator(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<CheckpointCountingSplit> getSplitSerializer() {
        return new CheckpointCountingSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<CheckpointCountingSplitState> getEnumeratorCheckpointSerializer() {
        return new CheckpointCountingSplitStateSerializer();
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return rowProducer.getProducedType();
    }

    /**
     * Interface for producing RowData records.
     */
    public interface RowProducer extends Serializable {
        RowData createRow(int value);

        TypeInformation<RowData> getProducedType();
    }

    /**
     * Default implementation of RowProducer.
     */
    private static class DefaultRowProducer implements RowProducer {

        @Override
        public RowData createRow(int value) {
            return io.delta.flink.sink.utils.DeltaSinkTestUtils.TEST_ROW_TYPE_CONVERTER.toInternal(
                org.apache.flink.types.Row.of(
                    String.valueOf(value),
                    String.valueOf((value + value)),
                    value
                )
            );
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            LogicalType[] fieldTypes = TEST_ROW_TYPE.getFields().stream()
                .map(RowField::getType).toArray(LogicalType[]::new);
            String[] fieldNames = TEST_ROW_TYPE.getFieldNames().toArray(new String[0]);
            return InternalTypeInfo.of(RowType.of(fieldTypes, fieldNames));
        }
    }
}
