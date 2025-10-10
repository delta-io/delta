package io.delta.flink.source.internal.enumerator.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static java.util.Arrays.asList;

import io.delta.flink.internal.options.PartitionFilterOptionTypeConverter;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.core.fs.Path;
import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.when;

import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;

public class SnapshotProcessorTest {

    private final List<DeltaSourceSplit> resultFiles = new ArrayList<>();

    private Map<String, Set<String>> partitionsToFilter = new HashMap<>();
    private final Map<String, String> partitionSpec1 = new HashMap<>();
    private final Map<String, String> partitionSpec2 = new HashMap<>();
    private Map<String, String> partitionSpec3 = new HashMap<>();

    protected Snapshot snapshot = org.mockito.Mockito.mock(Snapshot.class);

    @Test
    public void shouldFilterOutFilesOutsideOfSpecifiedPartitions() {
        SnapshotProcessor processor = init("part1=p1v1;part2=p2v1");
        processor.process(this::addFiles);

        assertThat(resultFiles.size(), equalTo(2));
        resultFiles.forEach(split -> {
            assertThat(split.getPartitionValues().get("part1"), equalTo("p1v1"));
            assertThat(split.getPartitionValues().get("part2"), equalTo("p2v1"));
        });
    }

    @Test
    public void shouldFilterOutFilesOutsideOfSpecifiedPartitionsWithInStatement() {
        SnapshotProcessor processor = init("part1 in (p1v1, p1v2);part2=p2v2");
        processor.process(this::addFiles);

        assertThat(resultFiles.size(), equalTo(1));
        resultFiles.forEach(split -> {
            assertThat(split.getPartitionValues().get("part1"), equalTo("p1v2"));
            assertThat(split.getPartitionValues().get("part2"), equalTo("p2v2"));
        });
    }

    @Test
    public void shouldProcessAllPartitions() {

        SnapshotProcessor processor = init("");
        processor.process(this::addFiles);

        assertThat(resultFiles.size(), equalTo(4));
    }

    private SnapshotProcessor init(String partitionFilterStr) {
        resultFiles.clear();
        partitionSpec1.clear();
        partitionSpec2.clear();
        partitionSpec3.clear();
        partitionsToFilter.clear();

        partitionsToFilter =
                PartitionFilterOptionTypeConverter.parseStringToMap(partitionFilterStr);
        partitionSpec1.put("part1", "p1v1");
        partitionSpec1.put("part2", "p2v1");
        partitionSpec2.put("part1", "p1v2");
        partitionSpec2.put("part2", "p2v2");
        partitionSpec3.put("part1", "p1v3");
        partitionSpec3.put("part2", "p2v3");
        List<AddFile> files = asList(
                new AddFile("s3://some/path/part1=p1v1/part2=p2v1/file1.parquet",
                        partitionSpec1, 100, 0L, true,
                        "", new HashMap<>()),
                new AddFile("s3://some/path/part1=p1v1/part2=p2v1/file2.parquet",
                        partitionSpec1, 100, 0L, true,
                        "", new HashMap<>()),
                new AddFile("s3://some/path/part1=p1v2/part2=p2v2/file3.parquet",
                        partitionSpec2, 100, 0L, true,
                        "", new HashMap<>()),
                new AddFile("s3://some/path/part1=p1v3/part2=p2v3/file1.parquet",
                        partitionSpec3, 100, 0L, true,
                        "", new HashMap<>()));

        when(snapshot.getAllFiles()).thenReturn(files);
        when(snapshot.getVersion()).thenReturn(1L);

        return new SnapshotProcessor(
                new Path("s3://some/path/"),
                snapshot,
                (AddFileEnumerator) (context, splitFilter) -> {
                    ArrayList<DeltaSourceSplit> splitsToReturn =
                            new ArrayList<>(context.getAddFiles().size());

                    for (AddFile addFile : context.getAddFiles()) {
                        Path path = new Path(addFile.getPath());
                        splitsToReturn.add(new DeltaSourceSplit(
                                addFile.getPartitionValues(),
                                "test",
                                path, 0L, 0L));
                    }

                    return splitsToReturn;
                },
                new ArrayList<>(),
                partitionsToFilter
        );
    }


    private void addFiles(List<DeltaSourceSplit> files) {
        this.resultFiles.addAll(files);
    }

    public Map<String, String> getPartitionSpec3() {
        return partitionSpec3;
    }

    public void setPartitionSpec3(Map<String, String> partitionSpec3) {
        this.partitionSpec3 = partitionSpec3;
    }
}
