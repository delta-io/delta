package io.delta.flink.source.internal.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@RunWith(Enclosed.class)
public class TransitiveOptionalTest {

    private static List<String> convertInput(String input) {
        return Arrays.stream(input.split(","))
            .map(String::trim).map(s -> {
                if (s.equals("null")) {
                    return null;
                } else {
                    return s;
                }
            }).collect(Collectors.toList());
    }

    @RunWith(Parameterized.class)
    public static class ParametrizedTests {

        private final String input;
        private final String expected;
        private final int expectedOrCount;

        public ParametrizedTests(String input, String expected, int expectedOrCount) {
            this.input = input;
            this.expected = expected;
            this.expectedOrCount = expectedOrCount;
        }

        @Parameters(name = "{index}: Input Values = [{0}]")
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                {"1, null, null", "1", 0}, {"1, 2, 3", "1", 0}, {"null, 2, null", "2", 1},
                {"null, null, 3", "3", 2}
            });
        }

        @Test
        public void shouldChainAndStopOnFirstNonNull() {
            List<String> inputValues = convertInput(input);

            // The actualOrCount is an AtomicInteger to make Java think that this is a final
            // immutable object. In fact, we do want to increment the counter on every "or" call.
            AtomicInteger actualOrCount = new AtomicInteger(0);

            String finalValue = TransitiveOptional.ofNullable(inputValues.get(0))
                .or(() -> {
                    actualOrCount.incrementAndGet();
                    return TransitiveOptional.ofNullable(inputValues.get(1));
                })
                .or(() -> {
                    actualOrCount.incrementAndGet();
                    return TransitiveOptional.ofNullable(inputValues.get(2));
                })
                .get();

            assertThat(finalValue, equalTo(expected));
            assertThat(actualOrCount.get(), equalTo(expectedOrCount));
        }
    }

    public static class NonParametrizedTests {

        @Test
        public void shouldCreateAndGetValue() {
            TransitiveOptional<String> optional = TransitiveOptional.of("val1");
            assertThat(optional.get(), equalTo("val1"));
        }

        @Test(expected = NoSuchElementException.class)
        public void shouldCreateNullableAndThrow() {
            TransitiveOptional<String> optional = TransitiveOptional.ofNullable(null);
            optional.get();
        }

        @Test(expected = NoSuchElementException.class)
        public void shouldCreateEmptyAndThrow() {
            TransitiveOptional<String> optional = TransitiveOptional.empty();
            optional.get();
        }
    }
}
