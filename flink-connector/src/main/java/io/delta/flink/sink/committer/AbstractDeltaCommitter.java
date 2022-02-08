package io.delta.flink.sink.committer;


import io.delta.flink.sink.committables.AbstractDeltaCommittable;
import org.apache.flink.api.connector.sink.Committer;

/**
 * Marker interface for internal class, not meant for public use.
 */
public interface AbstractDeltaCommitter extends Committer<AbstractDeltaCommittable> { }
