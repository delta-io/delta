package io.delta.flink.source;

import java.util.Optional;
import java.util.stream.Stream;

import io.delta.flink.source.internal.builder.DeltaSourceBuilderBase;
import io.delta.flink.source.internal.exceptions.DeltaSourceValidationException;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.codehaus.janino.util.Producer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public abstract class RowDataDeltaSourceBuilderTestBase {

    protected static final Logger LOG =
        LoggerFactory.getLogger(RowDataBoundedDeltaSourceBuilderTest.class);

    protected static final String[] COLUMN_NAMES = {"name", "surname", "age"};

    protected static final String TABLE_PATH = "s3://some/path/";

    protected static final LogicalType[] COLUMN_TYPES =
        {new CharType(), new CharType(), new IntType()};

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
            // Validation error due to different size of column names and column types array.
            Arguments.of(
                new String[]{"col1", "col2"},
                new LogicalType[]{new CharType(), new CharType(), new CharType()},
                1),

            // Validation error due to different size of column names and column types array.
            Arguments.of(
                new String[]{"col1", "col2", "col3"},
                new LogicalType[]{new CharType(), new CharType()},
                1),

            // Validation error due to null element in column name array.
            Arguments.of(
                new String[]{"col1", null, "col3"},
                new LogicalType[]{new CharType(), new CharType(), new CharType()},
                1),

            // Validation error due to null element in column type array.
            Arguments.of(
                new String[]{"col1", "col2", "col3"},
                new LogicalType[]{new CharType(), null, new CharType()},
                1),

            // Expecting two validation errors due to null element in column name array and array
            // length mismatch for column names and types.
            Arguments.of(
                new String[]{"col1", null, "col3"},
                new LogicalType[]{new CharType(), new CharType()},
                2),

            // Expecting two validation errors due to null element in column type array and array
            // length mismatch for column names and types.
            Arguments.of(
                new String[]{"col1", "col3"},
                new LogicalType[]{new CharType(), null, new CharType()},
                2),

            // Expecting two validation errors due to null reference to column name array and
            // null element in column type array.
            Arguments.of(
                null,
                new LogicalType[]{new CharType(), null, new CharType()},
                2),

            // Validation error due to null reference to column name array.
            Arguments.of(
                null,
                new LogicalType[]{new CharType(), new CharType()},
                1),

            // Validation error due to null reference to column type array.
            Arguments.of(new String[]{"col1", "col3"}, null, 1),

            // Validation error due to null reference to column type array and null element in
            // column name array.
            Arguments.of(new String[]{"col1", null}, null, 2)
        );
    }

    /**
     * Test for column name and colum type arrays.
     *
     * @param columnNames        An array with column names.
     * @param columnTypes        An array with column types.
     * @param expectedErrorCount Number of expected validation errors for given combination of
     *                           column names and types.
     */
    @ParameterizedTest
    @MethodSource("columnArrays")
    public void testColumnArrays(
            String[] columnNames,
            LogicalType[] columnTypes,
            int expectedErrorCount) {

        Optional<Exception> validation = testValidation(
            () -> getBuilderForColumns(columnNames, columnTypes).build()
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

        // expected number is 5 because Hadoop is used and validated by Format and Source builders.
        assertThat(exception.getValidationMessages().size(), equalTo(5));
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

        // expected number is 5 because Hadoop is used and validated by Format and Source builders.
        assertThat(exception.getValidationMessages().size(), equalTo(4));
    }

    protected abstract DeltaSourceBuilderBase<?, ?> getBuilderWithNulls();

    protected abstract DeltaSourceBuilderBase<?, ?> getBuilderForColumns(
            String[] columnNames,
            LogicalType[] columnTypes);


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

}
