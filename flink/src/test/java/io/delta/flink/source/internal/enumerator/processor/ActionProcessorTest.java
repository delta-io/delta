package io.delta.flink.source.internal.enumerator.processor;

import java.util.List;
import java.util.stream.Stream;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import io.delta.flink.source.internal.enumerator.monitor.ChangesPerVersion;
import io.delta.flink.source.internal.exceptions.DeltaSourceException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;

public class ActionProcessorTest extends ActionProcessorParameterizedTestBase {

    ///////////////////////////////////////////////////////////////////////////////
    // test case & arguments for ignoreChanges = false and ignoreDeletes = false //
    ///////////////////////////////////////////////////////////////////////////////
    @ParameterizedTest(name = "{index}: Actions = {0}")
    @MethodSource("arguments_notIgnoreChangesAndNotIgnoreDeletes")
    public void notIgnoreChangesAndNotIgnoreDeletes(
        List<Action> inputActions,
        Object expectedResults) {

        ActionProcessor processor = new ActionProcessor(false, false);

        testProcessor(inputActions, expectedResults, processor);
    }

    /**
     * Stream of {@link Arguments} elements for test case where ignoreChanges and ignoreDeletes are
     * set to false. In this configuration {@link ActionProcessor} should not allow for version with
     * {@link RemoveFile} actions that have
     * {@link io.delta.standalone.actions.FileAction#isDataChange()}
     * set to true. In that case, an exception should be thrown. Actions with {@link
     * io.delta.standalone.actions.FileAction#isDataChange()} set to false will be ignored and will
     * not trigger any exception.
     *
     * @return Stream of test {@link Arguments} elements. Arguments.of(testParameter,
     * testExpectedResult)
     */
    private static Stream<Arguments> arguments_notIgnoreChangesAndNotIgnoreDeletes() {
        return Stream.of(
            Arguments.of(singletonList(ADD_FILE), singletonList(ADD_FILE)),
            Arguments.of(singletonList(ADD_FILE_NO_CHANGE), emptyList()),

            // An exception is expected for version with Remove action and isDataChange == true
            Arguments.of(singletonList(REMOVE_FILE), DeltaSourceException.class),
            Arguments.of(singletonList(REMOVE_FILE_NO_CHANGE), emptyList()),

            // An exception is expected for version with Remove action and isDataChange == true
            Arguments.of(asList(ADD_FILE, REMOVE_FILE), DeltaSourceException.class),
            Arguments.of(asList(ADD_FILE, REMOVE_FILE_NO_CHANGE), singletonList(ADD_FILE)),

            // An exception is expected for version with Remove action and isDataChange == true
            Arguments.of(asList(ADD_FILE_NO_CHANGE, REMOVE_FILE), DeltaSourceException.class),
            Arguments.of(asList(ADD_FILE_NO_CHANGE, REMOVE_FILE_NO_CHANGE), emptyList())
        );
    }

    //////////////////////////////////////////////////////////////////////////////
    // test case & arguments for ignoreChanges = false and ignoreDeletes = true //
    //////////////////////////////////////////////////////////////////////////////
    @ParameterizedTest(name = "{index}: Actions = {0}")
    @MethodSource("arguments_notIgnoreChangesAndIgnoreDeletes")
    public void notIgnoreChangesAndIgnoreDeletes(
        List<Action> inputActions,
        Object expectedResults) {

        ActionProcessor processor = new ActionProcessor(false, true);

        testProcessor(inputActions, expectedResults, processor);
    }

    /**
     * Stream of {@link Arguments} elements for test case where ignoreChanges = false $
     * ignoreDeletes = true. In this configuration {@link ActionProcessor} should not throw an
     * exception when processing versions containing only {@link RemoveFile} actions regardless of
     * {@link io.delta.standalone.actions.FileAction#isDataChange()} flag.
     * <p>
     * An exception is expected when processing a version containing mix of {@link AddFile} and
     * {@link RemoveFile} actions with isDataChange flag set to true.
     *
     * @return Stream of test {@link Arguments} elements. Arguments.of(testParameter,
     * testExpectedResult)
     */
    private static Stream<Arguments> arguments_notIgnoreChangesAndIgnoreDeletes() {
        return Stream.of(
            Arguments.of(singletonList(ADD_FILE), singletonList(ADD_FILE)),
            Arguments.of(singletonList(ADD_FILE_NO_CHANGE), emptyList()),

            // Allowing version with only Remove Action regardless of isDataChange flag.
            Arguments.of(singletonList(REMOVE_FILE), emptyList()),
            Arguments.of(singletonList(REMOVE_FILE_NO_CHANGE), emptyList()),

            // An exception is expected for version with Add and Remove actions
            // with isDataChange == true
            Arguments.of(asList(ADD_FILE, REMOVE_FILE), DeltaSourceException.class),
            Arguments.of(asList(ADD_FILE, REMOVE_FILE_NO_CHANGE), singletonList(ADD_FILE)),
            Arguments.of(asList(ADD_FILE_NO_CHANGE, REMOVE_FILE), emptyList()),
            Arguments.of(asList(ADD_FILE_NO_CHANGE, REMOVE_FILE_NO_CHANGE), emptyList())
        );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    // test case & arguments for ignoreChanges = true and ignoreDeletes = true/false //
    ///////////////////////////////////////////////////////////////////////////////////
    @ParameterizedTest(name = "{index}: Actions = {0}")
    @MethodSource("arguments_ignoreChanges")
    public void ignoreChangesAndIgnoreDeletes(List<Action> inputActions, Object expectedResults) {

        ActionProcessor processor = new ActionProcessor(true, true);

        testProcessor(inputActions, expectedResults, processor);
    }

    @ParameterizedTest(name = "{index}: Actions = {0}")
    @MethodSource("arguments_ignoreChanges")
    public void ignoreChangesAndNotIgnoreDeletes(
        List<Action> inputActions,
        Object expectedResults) {

        ActionProcessor processor = new ActionProcessor(true, false);

        testProcessor(inputActions, expectedResults, processor);
    }

    /**
     * Stream of {@link Arguments} elements for test case where ignoreChanges = true In this
     * configuration, {@link ActionProcessor} allows for any combination of Add and Remove file
     * actions, regardless of isDataChange flag value. No exception is expected.
     *
     * @return Stream of test {@link Arguments} elements. Arguments.of(testParameter,
     * testExpectedResult)
     */
    private static Stream<Arguments> arguments_ignoreChanges() {
        return Stream.of(
            Arguments.of(singletonList(ADD_FILE), singletonList(ADD_FILE)),
            Arguments.of(singletonList(ADD_FILE_NO_CHANGE), emptyList()),
            Arguments.of(singletonList(REMOVE_FILE), emptyList()),
            Arguments.of(singletonList(REMOVE_FILE_NO_CHANGE), emptyList()),

            Arguments.of(asList(ADD_FILE, REMOVE_FILE), singletonList(ADD_FILE)),
            Arguments.of(asList(ADD_FILE, REMOVE_FILE_NO_CHANGE), singletonList(ADD_FILE)),
            Arguments.of(asList(ADD_FILE_NO_CHANGE, REMOVE_FILE), emptyList()),
            Arguments.of(asList(ADD_FILE_NO_CHANGE, REMOVE_FILE_NO_CHANGE), emptyList())
        );
    }

    //////////////////////
    // Other test cases //
    //////////////////////

    @Test
    public void shouldThrowIfInvalidActionInVersion() {

        // GIVEN
        ActionProcessor processor = new ActionProcessor(false, false);
        List<Action> actionsToProcess = asList(ADD_FILE, REMOVE_FILE, ADD_FILE);
        DeltaSourceException caughtException = null;

        // WHEN
        try {
            processor.processActions(prepareChangesToProcess(actionsToProcess));
        } catch (DeltaSourceException e) {
            caughtException = e;
        }

        // THEN
        assertThat(caughtException, notNullValue());
        assertThat(caughtException.getSnapshotVersion().orElse(null), equalTo(SNAPSHOT_VERSION));
        assertThat(caughtException.getTablePath().orElse(null), equalTo(TABLE_PATH));
    }

    @Test
    public void shouldFilterOutRemoveIfIgnoreChangesFlag() {

        // GIVEN
        ActionProcessor processor = new ActionProcessor(true, false);
        List<Action> actionsToProcess = asList(ADD_FILE, REMOVE_FILE, ADD_FILE);

        processor.processActions(prepareChangesToProcess(actionsToProcess));

        // WHEN
        ChangesPerVersion<AddFile> actualResult =
            processor.processActions(prepareChangesToProcess(actionsToProcess));

        // THEN
        assertThat(actualResult.getChanges().size(), equalTo(actionsToProcess.size() - 1));
        assertThat(actualResult.getSnapshotVersion(), equalTo(SNAPSHOT_VERSION));
        assertThat(
            hasItems(new Action[]{ADD_FILE, ADD_FILE}).matches(
                actualResult.getChanges()),
            equalTo(true));
    }

    protected ChangesPerVersion<Action> prepareChangesToProcess(List<Action> actions) {
        return new ChangesPerVersion<>(TABLE_PATH, SNAPSHOT_VERSION, actions);
    }
}
