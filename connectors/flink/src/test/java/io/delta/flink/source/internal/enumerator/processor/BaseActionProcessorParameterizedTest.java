package io.delta.flink.source.internal.enumerator.processor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.delta.flink.source.internal.enumerator.monitor.ChangesPerVersion;
import io.delta.flink.source.internal.exceptions.DeltaSourceException;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;

public abstract class BaseActionProcessorParameterizedTest {

    protected static final int SIZE = 100;

    protected static final long SNAPSHOT_VERSION = 10;

    protected static final String TABLE_PATH = "s3://some/path/";

    protected static final Map<String, String> PARTITIONS = Collections.emptyMap();

    protected static final Map<String, String> TAGS = Collections.emptyMap();

    protected static final String PATH = TABLE_PATH + "file.parquet";

    protected static final AddFile ADD_FILE =
        new AddFile(PATH, PARTITIONS, SIZE, System.currentTimeMillis(), true, "", TAGS);

    protected static final RemoveFile REMOVE_FILE =
        ADD_FILE.remove(true);

    protected static final AddFile ADD_FILE_NO_CHANGE =
        new AddFile(PATH, PARTITIONS, 100, System.currentTimeMillis(), false, "", TAGS);

    protected static final RemoveFile REMOVE_FILE_NO_CHANGE =
        ADD_FILE_NO_CHANGE.remove(false);

    protected ChangesPerVersion<Action> changesToProcess;

    protected void testProcessor(List<Action> inputActions, Object expectedResults,
        ActionProcessor processor) {
        boolean gotDeltaException = false;

        // GIVEN
        changesToProcess = prepareChangesToProcess(inputActions);

        // WHEN
        ChangesPerVersion<AddFile> actualResult = null;
        try {
            actualResult = processor.processActions(changesToProcess);
        } catch (DeltaSourceException e) {
            gotDeltaException = true;
        }

        // THEN
        assertResult(actualResult, expectedResults, gotDeltaException);
    }

    /**
     * This is a common method to assert results from {@link ActionProcessor} parametrized tests.
     * This method assert whether returned values are same as expected including empty result, when
     * {@link ActionProcessor} did not returned any data or when an exception was thrown.
     *
     * @param actualResult      Delta {@link AddFile} objects returned by {@link
     *                          ActionProcessor#processActions(ChangesPerVersion)} method.
     * @param expectedResults   Expected result for given test. Can be A collection od AddFile
     *                          objects or Exception.
     * @param gotDeltaException flag indicating that
     * {@link ActionProcessor#processActions(ChangesPerVersion)}
     *                          method thrown an exception during a test.
     */
    @SuppressWarnings("unchecked")
    protected void assertResult(
            ChangesPerVersion<AddFile> actualResult,
            Object expectedResults,
            boolean gotDeltaException) {

        // Case when the Exception is the expected result.
        if (DeltaSourceException.class.equals(expectedResults)) {
            assertThat("An exception was expected from ActionProcessor", gotDeltaException,
                equalTo(true));
        } else {
            List<AddFile> castedExpectedResults = (List<AddFile>) expectedResults;
            assertThat(actualResult.getChanges().size(), equalTo(castedExpectedResults.size()));
            assertThat(
                hasItems(castedExpectedResults.toArray()).matches(actualResult.getChanges()),
                equalTo(true));
        }
    }

    protected ChangesPerVersion<Action> prepareChangesToProcess(List<Action> actions) {
        return new ChangesPerVersion<>(TABLE_PATH, SNAPSHOT_VERSION, actions);
    }
}
