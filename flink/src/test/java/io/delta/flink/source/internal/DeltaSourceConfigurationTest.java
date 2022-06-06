package io.delta.flink.source.internal;

import org.junit.Before;
import org.junit.Test;
import static io.delta.flink.source.internal.utils.TestOptions.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.nullValue;

public class DeltaSourceConfigurationTest {

    private DeltaSourceConfiguration configuration;

    @Before
    public void setUp() {
        configuration = new DeltaSourceConfiguration();
    }

    @Test
    public void shouldAddOption() {
        String stringValue = "StringValue";
        long longValue = Long.MIN_VALUE;
        int intValue = Integer.MIN_VALUE;
        boolean booleanValue = true;

        configuration.addOption(LONG_OPTION, longValue);
        configuration.addOption(INT_OPTION, intValue);
        configuration.addOption(STRING_OPTION, stringValue);
        configuration.addOption(BOOLEAN_OPTION, booleanValue);

        assertThat(configuration.hasOption(LONG_OPTION), equalTo(true));
        assertThat(configuration.hasOption(INT_OPTION), equalTo(true));
        assertThat(configuration.hasOption(STRING_OPTION), equalTo(true));
        assertThat(configuration.hasOption(BOOLEAN_OPTION), equalTo(true));

        assertThat(configuration.getValue(LONG_OPTION), equalTo(longValue));
        assertThat(configuration.getValue(INT_OPTION), equalTo(intValue));
        assertThat(configuration.getValue(STRING_OPTION), equalTo(stringValue));
        assertThat(configuration.getValue(BOOLEAN_OPTION), equalTo(booleanValue));
    }

    @Test
    public void shouldGetDefaultValue() {
        assertThat(configuration.hasOption(LONG_OPTION), equalTo(false));
        assertThat(configuration.hasOption(INT_OPTION), equalTo(false));
        assertThat(configuration.hasOption(STRING_OPTION), equalTo(false));
        assertThat(configuration.hasOption(BOOLEAN_OPTION), equalTo(false));

        assertThat(configuration.getValue(LONG_OPTION), equalTo(LONG_OPTION.defaultValue()));
        assertThat(configuration.getValue(INT_OPTION), equalTo(INT_OPTION.defaultValue()));
        assertThat(configuration.getValue(STRING_OPTION), equalTo(STRING_OPTION.defaultValue()));
        assertThat(configuration.getValue(BOOLEAN_OPTION), equalTo(BOOLEAN_OPTION.defaultValue()));
    }

    @Test
    public void shouldHandleNoDefaultValue() {
        assertThat(configuration.hasOption(NO_DEFAULT_VALUE), equalTo(false));
        assertThat(configuration.getValue(NO_DEFAULT_VALUE), nullValue());
    }
}
