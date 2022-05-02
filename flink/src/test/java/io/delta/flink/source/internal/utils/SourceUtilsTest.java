package io.delta.flink.source.internal.utils;

import java.util.Arrays;
import java.util.Collection;

import org.apache.flink.core.fs.Path;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

@RunWith(Enclosed.class)
public class SourceUtilsTest {

    @RunWith(Parameterized.class)
    public static class ParametrizedTests {

        private final String input;
        private final String expected;

        public ParametrizedTests(String input, String expected) {
            this.input = input;
            this.expected = expected;
        }

        @Parameters(name = "{index}: Input Path = [{0}]")
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                {"/some/path/file.txt", "/some/path/file.txt"},
                {"some/path/file.txt", "some/path/file.txt"},
                {"/some/path/./file.txt", "/some/path/file.txt"},
                {"../some/path/./file.txt", "../some/path/file.txt"},
                {"././some/path/./file.txt", "some/path/file.txt"},
                {"C:/some/path/./file.txt", "/C:/some/path/file.txt"},
                {"s3:/some/path/./file.txt", "s3:/some/path/file.txt"},
                {"hdfs://hosts.com:8020/user/it1", "hdfs://hosts.com:8020/user/it1"},
                {"file.txt", "file.txt"}
            });
        }

        @Test
        public void shouldConvertPathToString() {
            String actual = SourceUtils.pathToString(new Path(input));

            assertThat(actual, equalTo(expected));
        }
    }

    public static class NonParametrizedTests {

        @Test(expected = IllegalArgumentException.class)
        public void shouldThrowOnNullParam() {
            SourceUtils.pathToString(null);
        }

    }
}
