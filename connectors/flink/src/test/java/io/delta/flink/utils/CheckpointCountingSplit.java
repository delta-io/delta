package io.delta.flink.utils;

import java.io.Serializable;
import java.util.Objects;

import org.apache.flink.api.connector.source.SourceSplit;

/**
 * Represents a split for CheckpointCountingSource.
 * Each split is assigned to a specific subtask.
 */
public class CheckpointCountingSplit implements SourceSplit, Serializable {

    private final String splitId;
    private final int subtaskId;
    private final int nextValue;

    public CheckpointCountingSplit(String splitId, int subtaskId) {
        this(splitId, subtaskId, 0);
    }

    public CheckpointCountingSplit(String splitId, int subtaskId, int nextValue) {
        this.splitId = splitId;
        this.subtaskId = subtaskId;
        this.nextValue = nextValue;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    public int getNextValue() {
        return nextValue;
    }

    public CheckpointCountingSplit withNextValue(int nextValue) {
        return new CheckpointCountingSplit(splitId, subtaskId, nextValue);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CheckpointCountingSplit that = (CheckpointCountingSplit) o;
        return subtaskId == that.subtaskId && nextValue == that.nextValue
            && Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId, subtaskId, nextValue);
    }

    @Override
    public String toString() {
        return "CheckpointCountingSplit{"
            + "splitId='" + splitId + '\''
            + ", subtaskId=" + subtaskId
            + ", nextValue=" + nextValue
            + '}';
    }
}

