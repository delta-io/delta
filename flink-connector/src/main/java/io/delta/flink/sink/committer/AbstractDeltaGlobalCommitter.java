package io.delta.flink.sink.committer;


import io.delta.flink.sink.committables.AbstractDeltaCommittable;
import io.delta.flink.sink.committables.AbstractDeltaGlobalCommittable;
import org.apache.flink.api.connector.sink.GlobalCommitter;

/**
 * Marker interface for internal class, not meant for public use.
 */
public interface AbstractDeltaGlobalCommitter
        extends GlobalCommitter<AbstractDeltaCommittable, AbstractDeltaGlobalCommittable> { }
