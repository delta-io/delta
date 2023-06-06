package io.delta.flink.source.internal.file;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.delta.standalone.actions.AddFile;

@RunWith(MockitoJUnitRunner.class)
public class DeltaFileEnumeratorTest {

    private static final String TABLE_PATH = "hdfs://host.path.com/to/table/";

    private static final long SNAPSHOT_VERSION = 1;

    private static final Map<String, String> DELTA_PARTITIONS =
        Collections.singletonMap("col1", "val1");

    @Mock
    private Path pathMockOne;

    @Mock
    private Path pathMockTwo;

    @Mock
    private FileSystem fileSystemOne;

    @Mock
    private FileSystem fileSystemTwo;

    @Mock
    private FileStatus fileStatusOne;

    @Mock
    private FileStatus fileStatusTwo;

    @Mock
    private BlockLocation blockOne;

    @Mock
    private BlockLocation blockTwo;

    private DeltaFileEnumerator fileEnumerator;

    private AddFileEnumeratorContext context;

    private List<AddFile> addFiles;

    @Before
    public void setUp() throws IOException {
        this.fileEnumerator = spy(new DeltaFileEnumerator());

        addFiles = buildAddFiles();

        when(pathMockOne.getFileSystem()).thenReturn(fileSystemOne);
        when(pathMockTwo.getFileSystem()).thenReturn(fileSystemTwo);

        // Mocking File System. The acquireFilePath logic is unit tested in
        // DeltaFileEnumeratorAcquireFilePathTest class.
        mockFileSystem();

        context = new AddFileEnumeratorContext(TABLE_PATH, addFiles, SNAPSHOT_VERSION);
    }

    @Test
    public void shouldCreateSplitWithoutBlocks() throws IOException {
        // Both files are returning no File Blocks. The fileStatusOne returns an empty array of
        // blocks and fileStatusTwo returns  null. The null means file or region (start + length)
        // is not existing, null will be returned.

        when(fileSystemOne.getFileBlockLocations(fileStatusOne, 0, 10)).thenReturn(
            new BlockLocation[0]);
        when(fileSystemTwo.getFileBlockLocations(fileStatusTwo, 0, 10)).thenReturn(null);

        List<DeltaSourceSplit> splits =
            fileEnumerator.enumerateSplits(context, (Path path) -> true);

        assertThat(splits.size(), equalTo(addFiles.size()));
        assertThat(splits.get(0).getPartitionValues(), equalTo(DELTA_PARTITIONS));
        assertThat(splits.get(1).getPartitionValues(), equalTo(DELTA_PARTITIONS));

        assertThat(splits.get(0).path(), equalTo(pathMockOne));
        assertThat(splits.get(1).path(), equalTo(pathMockTwo));

        assertThat("Splits do not have unique Ids",
            splits.stream().map(FileSourceSplit::splitId).collect(
                Collectors.toSet()).size(), equalTo(splits.size()));
    }

    @Test
    public void shouldCreateSplitWithBlocks() throws IOException {
        // We are creating two blocks (blockOne and blockTwo) for second file (fileStatusTwo).
        // Sum of block Length should be equal to file length.
        // In this example blockOne.length = 5 and blockTwo.length = 5, and file length is 10.

        when(blockOne.getOffset()).thenReturn(0L);
        when(blockOne.getLength()).thenReturn(5L);

        when(blockTwo.getOffset()).thenReturn(5L);
        when(blockTwo.getLength()).thenReturn(5L);

        when(blockOne.getHosts()).thenReturn(new String[]{"hostOne"});
        when(blockTwo.getHosts()).thenReturn(new String[]{"hostTwo"});

        when(fileSystemOne.getFileBlockLocations(fileStatusOne, 0, 10)).thenReturn(
            new BlockLocation[]{blockOne, blockTwo});

        when(fileSystemTwo.getFileBlockLocations(fileStatusTwo, 0, 10)).thenReturn(null);

        List<DeltaSourceSplit> splits =
            fileEnumerator.enumerateSplits(context, (Path path) -> true);

        // One File is splittable, so we should have 3 DeltaSourceSplits from 2 AddFiles.
        assertThat(splits.size(), equalTo(3));
        assertThat(splits.get(0).getPartitionValues(), equalTo(DELTA_PARTITIONS));
        assertThat(splits.get(1).getPartitionValues(), equalTo(DELTA_PARTITIONS));
        assertThat(splits.get(2).getPartitionValues(), equalTo(DELTA_PARTITIONS));

        assertThat(splits.get(0).hostnames(), equalTo(new String[]{"hostOne"}));
        assertThat(splits.get(1).hostnames(), equalTo(new String[]{"hostTwo"}));
        assertThat(splits.get(2).hostnames(), equalTo(new String[0]));

        assertThat(splits.get(0).path(), equalTo(pathMockOne));
        assertThat(splits.get(1).path(), equalTo(pathMockOne));
        assertThat(splits.get(2).path(), equalTo(pathMockTwo));

        assertThat("Splits do not have unique Ids",
            splits.stream().map(FileSourceSplit::splitId).collect(
                Collectors.toSet()).size(), equalTo(splits.size()));
    }

    @Test
    public void shouldHandleInvalidBlocks() throws IOException {
        // This test checks the block length sum condition. If sum of the blocks' length is not
        // equal to file length,
        // no block should be created and file should be converted to one split.

        // In this case we want to use only one AddFile.
        List<AddFile> addFiles = Collections.singletonList(this.addFiles.get(1));

        context = new AddFileEnumeratorContext(TABLE_PATH, addFiles, SNAPSHOT_VERSION);

        // We are creating two blocks (blockOne and blockTwo) for this file (fileStatusTwo).
        // Sum of block Length should be equal to file length.
        // In this example blockOne.length = 10 and blockTwo.length = 10, and file length is also
        // 10.
        // The sum of block's length will not be equal to getFileBlockLocations method's length
        // argument, hence file will be processed as one and not separate blocks.
        when(blockOne.getLength()).thenReturn(10L);
        when(blockTwo.getLength()).thenReturn(10L);

        when(fileSystemTwo.getFileBlockLocations(fileStatusTwo, 0, 10)).thenReturn(
            new BlockLocation[]{blockOne, blockTwo});

        List<DeltaSourceSplit> splits =
            fileEnumerator.enumerateSplits(context, (Path path) -> true);

        // File has invalid blocks, so it will be ignored, and we will process file as one.
        assertThat(splits.size(), equalTo(1));
        assertThat(splits.get(0).getPartitionValues(), equalTo(DELTA_PARTITIONS));

        assertThat(splits.get(0).hostnames(), equalTo(new String[0]));

        assertThat(splits.get(0).path(), equalTo(pathMockTwo));

        assertThat("Splits do not have unique Ids",
            splits.stream().map(FileSourceSplit::splitId).collect(
                Collectors.toSet()).size(), equalTo(splits.size()));
    }

    @Test
    public void shouldFilterAddFiles() throws IOException {

        Set<Path> processedPaths = new HashSet<>();

        when(fileSystemOne.getFileBlockLocations(fileStatusOne, 0, 10)).thenReturn(
            new BlockLocation[0]);
        when(fileSystemTwo.getFileBlockLocations(fileStatusTwo, 0, 10)).thenReturn(null);

        assertThat(fileEnumerator.enumerateSplits(context, processedPaths::add).size(), equalTo(2));
        assertThat("Splits Should not be produced from filtered files.",
            fileEnumerator.enumerateSplits(context, processedPaths::add).size(), equalTo(0));
    }

    @Test
    public void shouldGenerateUniqueIds() {

        String firstId = "";
        String lastId = "";

        int maxValue = 10_000;

        // Using Set to know that we have unique values.
        LinkedHashSet<String> splitIds = new LinkedHashSet<>(maxValue);
        for (int i = 0; i < maxValue; i++) {
            String id = fileEnumerator.getNextId();
            splitIds.add(id);

            if (i == 0) {
                firstId = id;
            } else {
                lastId = id;
            }
        }

        assertThat(splitIds.size(), equalTo(maxValue));
        assertThat(firstId, equalTo("0000000001"));
        assertThat(lastId, equalTo("0000010000"));

        System.gc();
    }

    private void mockFileSystem() throws IOException {
        when(fileEnumerator.acquireFilePath(TABLE_PATH, addFiles.get(0))).thenReturn(
            pathMockOne);
        when(fileEnumerator.acquireFilePath(TABLE_PATH, addFiles.get(1))).thenReturn(
            pathMockTwo);

        when(fileSystemOne.getFileStatus(pathMockOne)).thenReturn(fileStatusOne);
        when(fileSystemTwo.getFileStatus(pathMockTwo)).thenReturn(fileStatusTwo);

        when(fileStatusOne.getPath()).thenReturn(pathMockOne);
        when(fileStatusOne.getLen()).thenReturn(10L);

        when(fileStatusTwo.getPath()).thenReturn(pathMockTwo);
        when(fileStatusTwo.getLen()).thenReturn(10L);
    }

    private List<AddFile> buildAddFiles() {
        AddFile addFileOne =
            new AddFile("dataOne.parquet", DELTA_PARTITIONS, 10,
                System.currentTimeMillis(), true, "", Collections.emptyMap());
        AddFile addFileTwo =
            new AddFile(TABLE_PATH + "dataTwo.parquet", DELTA_PARTITIONS, 10,
                System.currentTimeMillis(), true, "", Collections.emptyMap());
        return Arrays.asList(addFileOne, addFileTwo);
    }
}

