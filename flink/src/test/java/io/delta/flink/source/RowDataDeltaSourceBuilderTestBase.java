package io.delta.flink.source;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.builder.DeltaConfigOption;
import io.delta.flink.source.internal.builder.DeltaSourceBuilderBase;
import io.delta.flink.source.internal.exceptions.DeltaSourceValidationException;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.janino.util.Producer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

public abstract class RowDataDeltaSourceBuilderTestBase {

    protected static final Logger LOG =
        LoggerFactory.getLogger(RowDataBoundedDeltaSourceBuilderTest.class);

    protected static final String TABLE_PATH = "s3://some/path/";

    @Mock
    protected DeltaLog deltaLog;

    @Mock
    protected Snapshot headSnapshot;

    @Mock
    protected Metadata metadata;

    protected MockedStatic<DeltaLog> deltaLogStatic;

    public void closeDeltaLogStatic() {
        if (deltaLogStatic != null) {
            deltaLogStatic.close();
        }
    }

    /**
     * @return A Stream of arguments for parametrized test such that every element contains:
     * <ul>
     *     <li>An array of column names.</li>
     *     <li>An array of types for requested column name.</li>
     *     <li>
     *         Expected number of validation errors for given combination of column names and types.
     *     </li>
     * </ul>
     */
    protected static Stream<Arguments> columnArrays() {
        return Stream.of(
            // Validation error due to blank column name.
            Arguments.of(new String[]{"col1", " "}, 1),

            // Validation error due to empty column name.
            Arguments.of(new String[]{"col1", ""}, 1),

            // Validation error due to null element in column name array.
            Arguments.of(new String[]{"col1", null, "col3"}, 1),

            // Validation error due to null reference to column name array.
            Arguments.of(null, 1)
        );
    }

    /**
     * Test for column name and colum type arrays.
     *
     * @param columnNames        An array with column names.
     * @param expectedErrorCount Number of expected validation errors for given combination of
     *                           column names and types.
     */
    @ParameterizedTest
    @MethodSource("columnArrays")
    public void testColumnArrays(String[] columnNames, int expectedErrorCount) {

        Optional<Exception> validation = testValidation(
            () -> getBuilderForColumns(columnNames).build()
        );

        DeltaSourceValidationException exception =
            (DeltaSourceValidationException) validation.orElseThrow(
                () -> new AssertionError(
                    "Builder should throw exception on invalid column names and column types "
                        + "arrays."));

        assertThat(exception.getValidationMessages().size(), equalTo(expectedErrorCount));
    }

    @Test
    public void testNullArgumentsValidation() {

        Optional<Exception> validation = testValidation(() -> getBuilderWithNulls().build());

        DeltaSourceValidationException exception =
            (DeltaSourceValidationException) validation.orElseThrow(
                () -> new AssertionError("Builder should throw exception on null arguments."));

        assertThat(exception.getValidationMessages().size(), equalTo(2));
    }

    @Test
    public void testMutualExclusiveOptions() {
        // using dedicated builder methods
        Optional<Exception> validation = testValidation(
            () -> getBuilderWithMutuallyExcludedOptions().build()
        );

        DeltaSourceValidationException exception =
            (DeltaSourceValidationException) validation.orElseThrow(
                () -> new AssertionError(
                    "Builder should throw exception when using mutually exclusive options."));

        assertThat(exception.getValidationMessages().size(), equalTo(1));
    }

    @Test
    public void testMutualExcludedGenericOptions() {
        // using dedicated builder methods
        Optional<Exception> validation = testValidation(
            () -> getBuilderWithGenericMutuallyExcludedOptions().build()
        );

        DeltaSourceValidationException exception =
            (DeltaSourceValidationException) validation.orElseThrow(
                () -> new AssertionError(
                    "Builder should throw exception when using mutually exclusive options."));

        assertThat(exception.getValidationMessages().size(), equalTo(1));
    }

    @Test
    public void testNullMandatoryFieldsAndExcludedOption() {

        Optional<Exception> validation = testValidation(
            () -> getBuilderWithNullMandatoryFieldsAndExcludedOption().build()
        );

        DeltaSourceValidationException exception =
            (DeltaSourceValidationException) validation.orElseThrow(
                () -> new AssertionError("Builder should throw validation exception."));

        assertThat(exception.getValidationMessages().size(), equalTo(2));
    }

    @Test
    public void shouldThrowWhenUsingNotExistingOption() {
        DeltaSourceValidationException exception =
            assertThrows(DeltaSourceValidationException.class,
                () -> getBuilderAllColumns().option("SomeOption", "SomeValue"));

        LOG.info("Option Validation Exception: ", exception);
        assertThat(
            "Unexpected message in reported DeltaSourceValidationException.",
            exception
                .getValidationMessages()
                .stream()
                .allMatch(message -> message.contains(
                    "Invalid option [SomeOption] used for Delta Source Connector")),
            equalTo(true)
        );
    }

    @Test
    public void shouldThrowWhenSettingInternalOption() {

        DeltaSourceValidationException exception =
            assertThrows(DeltaSourceValidationException.class,
                () -> getBuilderWithOption(
                    DeltaSourceOptions.LOADED_SCHEMA_SNAPSHOT_VERSION, 10L));

        assertThat(exception.getMessage().contains("Invalid option"), equalTo(true));
    }

    @Test
    public void shouldThrowWhenInapplicableOptionUsed() {
        assertAll(() -> {
            for (DeltaSourceBuilderBase<?, ?> builder : initBuildersWithInapplicableOptions()) {
                assertThrows(DeltaSourceValidationException.class, builder::build,
                    "Builder should throw when inapplicable option was used. Config: "
                        + builder.getSourceConfiguration());
            }
        });
    }

    @Test
    public void testGetSourceConfigurationImmutability() {

        DeltaSourceBuilderBase<?, ?> builder = getBuilderAllColumns();
        builder.option(DeltaSourceOptions.STARTING_VERSION.key(), 10);

        DeltaSourceConfiguration originalConfiguration = builder.getSourceConfiguration();

        // making sure that "startingVersion" option was added and configuration has no
        // "updateCheckIntervalMillis" and "updateCheckDelayMillis" options set.
        // Those will be used for next step.
        assertAll(() -> {
                assertThat(
                    originalConfiguration.hasOption(DeltaSourceOptions.STARTING_VERSION),
                    equalTo(true)
                );
                assertThat(
                    originalConfiguration.hasOption(DeltaSourceOptions.UPDATE_CHECK_INTERVAL),
                    equalTo(false)
                );
                assertThat(
                    originalConfiguration.hasOption(DeltaSourceOptions.UPDATE_CHECK_INITIAL_DELAY),
                    equalTo(false));
            }
        );

        // Add "updateCheckIntervalMillis" option to builder and check if previous configuration
        // was updated. It shouldn't because builder.getSourceConfiguration should return a copy of
        // builder's configuration.
        builder.option(DeltaSourceOptions.UPDATE_CHECK_INTERVAL.key(), 1000);
        assertAll(() -> {
                assertThat(
                    builder.getSourceConfiguration()
                        .hasOption(DeltaSourceOptions.UPDATE_CHECK_INTERVAL),
                    equalTo(true)
                );
                assertThat(
                    "Updates on builder's configuration should not be visible in previously "
                        + "returned configuration via builder.getSourceConfiguration",
                    originalConfiguration.hasOption(DeltaSourceOptions.UPDATE_CHECK_INTERVAL),
                    equalTo(false)
                );
            }
        );

        // Update originalConfiguration and check if that mutates builder's configuration,
        // it shouldn't.
        originalConfiguration.addOption(DeltaSourceOptions.UPDATE_CHECK_INITIAL_DELAY, 1410L);

        assertAll(() -> {
                assertThat(
                    originalConfiguration.hasOption(DeltaSourceOptions.UPDATE_CHECK_INITIAL_DELAY),
                    equalTo(true));
                assertThat(
                    "Updates on returned configuration should not change builder's inner "
                        + "configuration",
                    builder.getSourceConfiguration()
                        .hasOption(DeltaSourceOptions.UPDATE_CHECK_INITIAL_DELAY),
                    equalTo(false));
            }
        );
    }

    /**
     * @return A collection of Delta source builders where each has inapplicable option set.
     * <p>
     * Inapplicable option is an option that is not suited for given
     * {@link DeltaSourceBuilderBase} implementation. For example incompatible
     * {@link org.apache.flink.api.connector.source.Boundedness} mode.
     */
    protected abstract Collection<? extends DeltaSourceBuilderBase<?,?>>
        initBuildersWithInapplicableOptions();

    /**
     * Creates a Delta source builder with option set via DeltaSourceBuilderBase#option(key, value)
     * method.
     * @param optionName {@link DeltaConfigOption} to set.
     * @param value value for option.
     */
    protected abstract DeltaSourceBuilderBase<?, ?> getBuilderWithOption(
        DeltaConfigOption<?> optionName,
        Object value
    );

    /**
     * @return A Delta source builder implementation with null values for mandatory fields.
     */
    protected abstract DeltaSourceBuilderBase<?, ?> getBuilderWithNulls();

    /**
     * Creates a Delta source builder for given array of columnNames that are passed to
     * {@link DeltaSourceBuilderBase#columnNames(List)} method.
     * @param columnNames Column names that should be read from Delta table by created source.
     */
    protected abstract DeltaSourceBuilderBase<?, ?> getBuilderForColumns(String[] columnNames);

    /**
     * @return most basic builder configuration, no options, no columns defined.
     */
    protected abstract DeltaSourceBuilderBase<?, ?> getBuilderAllColumns();

    /**
     * @return Delta source builder that uses invalid combination od mutually excluded options set
     * via builder's dedicated methods such as 'startVersion(...)' or 'startingTimeStamp(...).
     */
    protected abstract DeltaSourceBuilderBase<?, ?> getBuilderWithMutuallyExcludedOptions();

    /**
     * @return Delta source builder that uses invalid combination od mutually excluded options set
     * via builder's generic 'option(key, value)' methods such as 'option("startVersion", 10)'.
     */
    protected abstract DeltaSourceBuilderBase<?, ?> getBuilderWithGenericMutuallyExcludedOptions();

    /**
     * @return Builder that has null values for mandatory fields and used mutually excluded options.
     */
    protected abstract DeltaSourceBuilderBase<?, ?>
        getBuilderWithNullMandatoryFieldsAndExcludedOption();

    protected Optional<Exception> testValidation(Producer<DeltaSource<?>> builder) {
        try {
            builder.produce();
        } catch (Exception e) {
            LOG.info("Caught exception during builder validation tests", e);
            return Optional.of(e);
        }
        return Optional.empty();
    }

    protected void mockDeltaTableForSchema(StructField[] fields) {
        deltaLogStatic = Mockito.mockStatic(DeltaLog.class);
        deltaLogStatic.when(() -> DeltaLog.forTable(any(Configuration.class), anyString()))
            .thenReturn(this.deltaLog);

        when(headSnapshot.getMetadata()).thenReturn(metadata);
        when(metadata.getSchema())
            .thenReturn(
                new StructType(fields)
            );
    }

    protected  <T> DeltaSourceBuilderBase<?, ?> setOptionOnBuilder(String optionName, T value,
            DeltaSourceBuilderBase<?, ?> builder) {
        if (value instanceof String) {
            return (DeltaSourceBuilderBase<?, ?>) builder.option(optionName, (String) value);
        }

        if (value instanceof Integer) {
            return (DeltaSourceBuilderBase<?, ?>) builder.option(optionName, (Integer) value);
        }

        if (value instanceof Long) {
            return (DeltaSourceBuilderBase<?, ?>) builder.option(optionName, (Long) value);
        }

        if (value instanceof Boolean) {
            return (DeltaSourceBuilderBase<?, ?>) builder.option(optionName, (Boolean) value);
        }

        throw new IllegalArgumentException(
            "Used unsupported value type for Builder optionName - " + value.getClass());
    }

}
