package io.delta.flink.source.internal.utils;

import org.apache.flink.core.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SourceUtilsTest {

    @ParameterizedTest(name = "{index}: Input Path = [{0}]")
    @CsvSource(value = {
        "/some/path/file.txt;/some/path/file.txt",
        "some/path/file.txt;some/path/file.txt",
        "/some/path/./file.txt;/some/path/file.txt",
        "/some/path/./file.txt;/some/path/file.txt",
        "../some/path/./file.txt;../some/path/file.txt",
        "././some/path/./file.txt;some/path/file.txt",
        "C:/some/path/./file.txt;/C:/some/path/file.txt",
        "s3:/some/path/./file.txt;s3:/some/path/file.txt",
        "hdfs://hosts.com:8020/user/it1;hdfs://hosts.com:8020/user/it1",
        "file.txt;file.txt"
        },
        delimiter = ';')
    public void shouldConvertPathToString(String input, String expected) {
        String actual = SourceUtils.pathToString(new Path(input));

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void shouldThrowOnNullParam() {
        assertThrows(IllegalArgumentException.class, () -> SourceUtils.pathToString(null));
    }

}
