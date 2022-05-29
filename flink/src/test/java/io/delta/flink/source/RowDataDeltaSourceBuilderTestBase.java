package io.delta.flink.source;

import java.util.Optional;
import java.util.stream.Stream;

import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.builder.DeltaSourceBuilderBase;
import io.delta.flink.source.internal.exceptions.DeltaSourceValidationException;
import org.apache.flink.configuration.ConfigOption;
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
    public void shouldThrowWhenSettingInternalOption() {
        DeltaSourceOptions.LOADED_SCHEMA_SNAPSHOT_VERSION.key();

        DeltaSourceValidationException exception =
            assertThrows(DeltaSourceValidationException.class,
                () -> getBuilderWithOption(
                    DeltaSourceOptions.LOADED_SCHEMA_SNAPSHOT_VERSION, 10L));

        assertThat(exception.getMessage().contains("Invalid option"), equalTo(true));
    }

    protected abstract <T> DeltaSourceBuilderBase<?, ?> getBuilderWithOption(
        ConfigOption<T> option,
        T value
    );

    protected abstract DeltaSourceBuilderBase<?, ?> getBuilderWithNulls();

    protected abstract DeltaSourceBuilderBase<?, ?> getBuilderForColumns(String[] columnNames);

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

    protected  <T> DeltaSourceBuilderBase<?, ?> setOptionOnBuilder(
            ConfigOption<T> option,
            T value,
            DeltaSourceBuilderBase<?, ?> builder) {
        if (value instanceof String) {
            return (DeltaSourceBuilderBase<?, ?>) builder.option(option.key(), (String) value);
        }

        if (value instanceof Integer) {
            return (DeltaSourceBuilderBase<?, ?>) builder.option(option.key(), (Integer) value);
        }

        if (value instanceof Long) {
            return (DeltaSourceBuilderBase<?, ?>) builder.option(option.key(), (Long) value);
        }

        if (value instanceof Boolean) {
            return (DeltaSourceBuilderBase<?, ?>) builder.option(option.key(), (Boolean) value);
        }

        throw new IllegalArgumentException(
            "Used unsupported value type for Builder option - " + value.getClass());
    }

}
