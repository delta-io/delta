package io.delta.flink.internal.options;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.delta.flink.internal.options.TestOptions.STRING_OPTION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PartitionFilterOptionTypeConverterTest {

    private OptionTypeConverter<?> typeConverter;

    @BeforeEach
    public void setUp() {
        typeConverter = new PartitionFilterOptionTypeConverter();
    }

    @Test
    public void shouldConvertToMap() {

        Map<String, Set<String>> expectedValue = new HashMap<>();
        Set<String> values1 = new HashSet<>();
        values1.add("value1");
        values1.add("value2");
        expectedValue.put("key1", values1);
        Set<String> values2 = new HashSet<>();
        values2.add("value3");
        expectedValue.put("key2", values2);

        String input = "key1 in (value1, value2);key2=value3";

        assertAll(() -> {
            assertThat(typeConverter.convertType(STRING_OPTION, input), equalTo(input));
            assertThat(PartitionFilterOptionTypeConverter.parseStringToMap(input),
                    equalTo(expectedValue));
            assertThat(typeConverter.convertType(STRING_OPTION, ""),
                    equalTo(""));
            assertThat(PartitionFilterOptionTypeConverter.parseStringToMap(""),
                    equalTo(new HashMap<>()));
            assertThrows(IllegalArgumentException.class,
                    () -> typeConverter.convertType(STRING_OPTION, "invalid_format"));
        });
    }
}
