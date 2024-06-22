package io.delta.flink.source.internal.state;

import java.util.Collections;
import java.util.Map;

import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.Path;
import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertTrue;

public class DeltaSourceSplitTest {

    @Test
    public void shouldAllowForNullPartitions() {
        DeltaSourceSplit split = prepareSplit(null);
        assertTrue(split.getPartitionValues().isEmpty());
    }

    @Test
    public void shouldAcceptPartitions() {
        DeltaSourceSplit split = prepareSplitWithPartition();
        assertThat(split.getPartitionValues().size(), equalTo(1));
    }

    @Test
    public void shouldUpdateWithPosition() {
        DeltaSourceSplit split = prepareSplitWithPartition();
        assertThat(split.getReaderPosition().orElse(null), equalTo(null));
        assertThat(split.getPartitionValues().size(), equalTo(1));

        CheckpointedPosition checkpointedPosition = new CheckpointedPosition(100, 1000);

        DeltaSourceSplit updatedSplit = split.updateWithCheckpointedPosition(checkpointedPosition);
        assertThat(updatedSplit.getReaderPosition().orElse(null), equalTo(checkpointedPosition));
        assertThat(updatedSplit.getPartitionValues().size(), equalTo(1));
    }

    private DeltaSourceSplit prepareSplitWithPartition() {
        Map<String, String> partitions = Collections.singletonMap("col1", "val1");
        return prepareSplit(partitions);
    }


    private DeltaSourceSplit prepareSplit(Map<String, String> partitions) {
        return new DeltaSourceSplit(partitions, "id", new Path(), 0, 0);
    }
}
