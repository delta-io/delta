package io.delta.flink.source.internal.builder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static io.delta.flink.source.internal.utils.TestOptions.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultOptionTypeConverterTest {

    private OptionTypeConverter<?> typeConverter;

    @BeforeEach
    public void setUp() {
        typeConverter = new DefaultOptionTypeConverter();
    }

    @Test
    public void shouldConvertToString() {

        String expectedValue = "1";

        assertAll(() -> {
                assertThat(typeConverter.convertType(STRING_OPTION, 1), equalTo(expectedValue));
                assertThat(typeConverter.convertType(STRING_OPTION, 1L), equalTo(expectedValue));
                assertThat(typeConverter.convertType(STRING_OPTION, true), equalTo("true"));
                assertThat(typeConverter.convertType(STRING_OPTION, "1"), equalTo(expectedValue));
            }
        );
    }

    @Test
    public void shouldConvertToInteger() {

        int expectedValue = 1;

        assertAll(() -> {
                assertThat(typeConverter.convertType(INT_OPTION, 1), equalTo(expectedValue));
                assertThat(typeConverter.convertType(INT_OPTION, 1L), equalTo(expectedValue));
                assertThat(typeConverter.convertType(INT_OPTION, "1"), equalTo(expectedValue));
                assertThrows(NumberFormatException.class,
                    () -> typeConverter.convertType(INT_OPTION, true));
            }
        );
    }

    @Test
    public void shouldConvertToLong() {

        long expectedValue = 1L;

        assertAll(() -> {
                assertThat(typeConverter.convertType(LONG_OPTION, 1), equalTo(expectedValue));
                assertThat(typeConverter.convertType(LONG_OPTION, 1L), equalTo(expectedValue));
                assertThat(typeConverter.convertType(LONG_OPTION, "1"), equalTo(expectedValue));
                assertThrows(NumberFormatException.class,
                    () -> typeConverter.convertType(LONG_OPTION, true));
            }
        );
    }

    @Test
    public void shouldConvertToBoolean() {

        assertAll(() -> {
                assertThat(typeConverter.convertType(BOOLEAN_OPTION, 1), equalTo(false));
                assertThat(typeConverter.convertType(BOOLEAN_OPTION, 1L), equalTo(false));
                assertThat(typeConverter.convertType(BOOLEAN_OPTION, "1"), equalTo(false));
                assertThat(typeConverter.convertType(BOOLEAN_OPTION, "0"), equalTo(false));
                assertThat(typeConverter.convertType(BOOLEAN_OPTION, "true"), equalTo(true));
                assertThat(typeConverter.convertType(BOOLEAN_OPTION, "false"), equalTo(false));
                assertThat(typeConverter.convertType(BOOLEAN_OPTION, true), equalTo(true));
                assertThat(typeConverter.convertType(BOOLEAN_OPTION, false), equalTo(false));
            }
        );
    }
}
