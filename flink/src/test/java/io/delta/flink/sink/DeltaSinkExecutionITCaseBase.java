package io.delta.flink.sink;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import io.delta.flink.sink.internal.DeltaSinkInternal;
import io.delta.flink.sink.internal.committables.DeltaCommittable;
import io.delta.flink.sink.internal.committables.DeltaGlobalCommittable;
import io.delta.flink.sink.internal.committer.DeltaGlobalCommitter;
import io.delta.flink.sink.internal.writer.DeltaWriterBucketState;
import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public abstract class DeltaSinkExecutionITCaseBase {

    protected String initSourceFolder(boolean isPartitioned, String deltaTablePath) {
        try {
            if (isPartitioned) {
                DeltaTestUtils.initTestForPartitionedTable(deltaTablePath);
            } else {
                DeltaTestUtils.initTestForNonPartitionedTable(deltaTablePath);
            }

            return deltaTablePath;
        } catch (IOException e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    protected abstract static class FailoverDeltaSinkBase<IN>
        implements Sink<IN, DeltaCommittable, DeltaWriterBucketState, DeltaGlobalCommittable> {

        protected final DeltaSinkInternal<IN> decoratedSink;

        protected FailoverDeltaSinkBase(DeltaSinkInternal<IN> decoratedSink) {
            this.decoratedSink = decoratedSink;
        }

        @Override
        public SinkWriter<IN, DeltaCommittable, DeltaWriterBucketState> createWriter(
            InitContext initContext, List<DeltaWriterBucketState> list) throws IOException {
            return this.decoratedSink.createWriter(initContext, list);
        }

        @Override
        public Optional<SimpleVersionedSerializer<DeltaWriterBucketState>>
            getWriterStateSerializer() {

            return this.decoratedSink.getWriterStateSerializer();
        }

        @Override
        public Optional<Committer<DeltaCommittable>> createCommitter() throws IOException {
            return this.decoratedSink.createCommitter();
        }
        @Override
        public Optional<SimpleVersionedSerializer<DeltaCommittable>> getCommittableSerializer() {
            return this.decoratedSink.getCommittableSerializer();
        }

        @Override
        public Optional<SimpleVersionedSerializer<DeltaGlobalCommittable>>
            getGlobalCommittableSerializer() {
            return this.decoratedSink.getGlobalCommittableSerializer();
        }
    }

    protected abstract static class FailoverDeltaGlobalCommitterBase
        implements GlobalCommitter<DeltaCommittable, DeltaGlobalCommittable> {

        protected final DeltaGlobalCommitter decoratedGlobalCommitter;

        protected FailoverDeltaGlobalCommitterBase(DeltaGlobalCommitter decoratedGlobalCommitter) {
            this.decoratedGlobalCommitter = decoratedGlobalCommitter;
        }

        @Override
        public List<DeltaGlobalCommittable> filterRecoveredCommittables(
            List<DeltaGlobalCommittable> list) throws IOException {
            return this.decoratedGlobalCommitter.filterRecoveredCommittables(list);
        }

        @Override
        public DeltaGlobalCommittable combine(List<DeltaCommittable> list) throws IOException {
            return this.decoratedGlobalCommitter.combine(list);
        }

        @Override
        public void endOfInput() throws IOException, InterruptedException {
            this.decoratedGlobalCommitter.endOfInput();
        }

        @Override
        public void close() throws Exception {
            this.decoratedGlobalCommitter.close();
        }
    }

    protected enum GlobalCommitterExceptionMode {
        BEFORE_COMMIT,
        AFTER_COMMIT,
        NONE
    }
}
