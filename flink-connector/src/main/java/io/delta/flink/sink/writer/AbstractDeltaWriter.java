package io.delta.flink.sink.writer;

import io.delta.flink.sink.committables.AbstractDeltaCommittable;
import org.apache.flink.api.connector.sink.SinkWriter;

/**
 * Marker interface for internal class, not meant for public use.
 */
public interface AbstractDeltaWriter<IN>
        extends SinkWriter<IN, AbstractDeltaCommittable, AbstractDeltaWriterBucketState> { }
