package io.delta.flink.source.internal.file;

import java.util.Arrays;
import java.util.Collection;

import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.core.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.delta.standalone.actions.AddFile;

@RunWith(Parameterized.class)
public class DeltaFileEnumeratorAcquireFilePathTest {

    private final String tablePath;

    private final String addFilePath;

    private final String expectedPath;

    protected DeltaFileEnumerator fileEnumerator;

    public DeltaFileEnumeratorAcquireFilePathTest(String tablePath, String addFilePath,
        String expectedPath) {
        this.tablePath = tablePath;
        this.addFilePath = addFilePath;
        this.expectedPath = expectedPath;
    }

    @Parameters(name = "{index}: Table Path = [{0}], AddFile Path = [{1}]")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
            {"hdfs://host.path.com/to/table", "data.parquet",
                "hdfs://host.path.com/to/table/data.parquet"},
            {"hdfs://host.path.com/to/table/", "data.parquet",
                "hdfs://host.path.com/to/table/data.parquet"},
            {"hdfs://host.path.com/to/table", "hdfs://host.path.com/to/table/data.parquet",
                "hdfs://host.path.com/to/table/data.parquet"}
        });
    }

    @Before
    public void setUp() {
        this.fileEnumerator = new DeltaFileEnumerator();
    }

    @Test
    public void shouldAcquireFilePath() {
        AddFile addFile = mock(AddFile.class);
        when(addFile.getPath()).thenReturn(addFilePath);

        Path filePath = fileEnumerator.acquireFilePath(tablePath, addFile);
        assertThat(SourceUtils.pathToString(filePath), equalTo(expectedPath));
    }
}
