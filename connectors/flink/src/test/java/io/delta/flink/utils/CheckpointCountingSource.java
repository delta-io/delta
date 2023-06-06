package io.delta.flink.utils;

import java.io.Serializable;
import java.util.Collections;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.sink.utils.DeltaSinkTestUtils.TEST_ROW_TYPE;

/**
 * Each of the source operators outputs records in given number of checkpoints. Number of records
 * per checkpoint is constant between checkpoints, and defined by user. When all records are
 * emitted, the source waits for two more checkpoints until it finishes.
 * <p>
 * All credits for this implementation goes to <b>Grzegorz Kolakowski<b> who implemented the
 * original version of this class for end2end tests. This class was copied from his Pull Request
 * here.
 */
public class CheckpointCountingSource extends RichParallelSourceFunction<RowData>
    implements CheckpointListener, CheckpointedFunction, ResultTypeQueryable<RowData> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckpointCountingSource.class);

    private final int numberOfCheckpoints;

    private final int recordsPerCheckpoint;

    private final RowProducer rowProducer;

    private ListState<Integer> nextValueState;

    private int nextValue;

    private volatile boolean isCanceled;

    private volatile boolean waitingForCheckpoint;

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
    public void initializeState(FunctionInitializationContext context) throws Exception {
        nextValueState = context.getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("nextValue", Integer.class));

        if (nextValueState.get() != null && nextValueState.get().iterator().hasNext()) {
            nextValue = nextValueState.get().iterator().next();
        }
        waitingForCheckpoint = false;
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        LOGGER.info("Run subtask={}; attempt={}.",
            getRuntimeContext().getIndexOfThisSubtask(),
            getRuntimeContext().getAttemptNumber());

        sendRecordsUntil(numberOfCheckpoints, ctx);
        idleUntilNextCheckpoint(ctx);
        LOGGER.info("Source task done; subtask={}.",
            getRuntimeContext().getIndexOfThisSubtask());
    }

    private void sendRecordsUntil(int targetCheckpoints, SourceContext<RowData> ctx)
        throws InterruptedException {
        while (!isCanceled && nextValue < targetCheckpoints * recordsPerCheckpoint) {
            synchronized (ctx.getCheckpointLock()) {
                emitRecordsBatch(recordsPerCheckpoint, ctx);
                waitingForCheckpoint = true;
            }
            LOGGER.info("Waiting for checkpoint to complete; subtask={}.",
                getRuntimeContext().getIndexOfThisSubtask());
            while (waitingForCheckpoint) {
                Thread.sleep(1);
            }
        }
    }

    private void emitRecordsBatch(int batchSize, SourceContext<RowData> ctx) {
        nextValue = rowProducer.emitRecordsBatch(nextValue, ctx, batchSize);
        LOGGER.info("Emitted {} records (total {}); subtask={}.", batchSize, nextValue,
            getRuntimeContext().getIndexOfThisSubtask());
    }

    private void idleUntilNextCheckpoint(SourceContext<RowData> ctx) throws InterruptedException {
        if (!isCanceled) {
            // Idle until the next checkpoint completes to avoid any premature job termination and
            // race conditions.
            LOGGER.info("Waiting for an additional checkpoint to complete; subtask={}.",
                getRuntimeContext().getIndexOfThisSubtask());
            synchronized (ctx.getCheckpointLock()) {
                waitingForCheckpoint = true;
            }
            while (waitingForCheckpoint) {
                Thread.sleep(1L);
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        nextValueState.update(Collections.singletonList(nextValue));
        LOGGER.info("state snapshot done; checkpointId={}; subtask={}.",
            context.getCheckpointId(),
            getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        waitingForCheckpoint = false;
        LOGGER.info("Checkpoint {} complete; subtask={}.", checkpointId,
            getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        LOGGER.info("Checkpoint {} aborted; subtask={}.", checkpointId,
            getRuntimeContext().getIndexOfThisSubtask());
        CheckpointListener.super.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void cancel() {
        isCanceled = true;
        waitingForCheckpoint = false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return rowProducer.getProducedType();
    }

    public interface RowProducer extends Serializable {

        int emitRecordsBatch(int nextValue, SourceContext<RowData> ctx, int batchSize);

        TypeInformation<RowData> getProducedType();
    }

    private static class DefaultRowProducer implements RowProducer {

        @Override
        public int emitRecordsBatch(int nextValue, SourceContext<RowData> ctx, int batchSize) {
            for (int i = 0; i < batchSize; ++i) {
                RowData row = DeltaSinkTestUtils.TEST_ROW_TYPE_CONVERTER.toInternal(
                    Row.of(
                        String.valueOf(nextValue),
                        String.valueOf((nextValue + nextValue)),
                        nextValue
                    )
                );
                ctx.collect(row);
                nextValue++;
            }
            return nextValue;
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
