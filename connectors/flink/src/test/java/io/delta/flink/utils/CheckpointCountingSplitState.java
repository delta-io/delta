package io.delta.flink.utils;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Checkpoint state for CheckpointCountingSplitEnumerator.
 */
public class CheckpointCountingSplitState implements Serializable {

    private final List<CheckpointCountingSplit> remainingSplits;

    public CheckpointCountingSplitState(List<CheckpointCountingSplit> remainingSplits) {
        this.remainingSplits = remainingSplits;
    }

    public List<CheckpointCountingSplit> getRemainingSplits() {
        return remainingSplits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CheckpointCountingSplitState that = (CheckpointCountingSplitState) o;
        return Objects.equals(remainingSplits, that.remainingSplits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(remainingSplits);
    }

    @Override
    public String toString() {
        return "CheckpointCountingSplitState{" + "remainingSplits=" + remainingSplits + '}';
    }
}

