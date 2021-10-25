// TODO: copyright

package io.delta.standalone.actions;

/**
 * A marker interface for all Actions that can be applied to a Delta Table.
 * Each action represents a single change to the state of a Delta table.
 *
 * You can use the following code to extract the concrete type of an {@link Action}.
 * <pre>{@code
 *   List<Action> actions = ... // {@link io.delta.standalone.DeltaLog.getChanges} is one way to get such actions
 *   actions.forEach(x -> {
 *       if (x instanceof AddFile) {
 *          AddFile addFile = (AddFile) x;
 *          ...
 *       } else if (x instanceof AddCDCFile) {
 *          AddCDCFile addCDCFile = (AddCDCFile)x;
 *          ...
 *       } else if ...
 *   });
 * }</pre>
 */
public interface Action {

    /** The maximum version of the protocol that this version of Delta Standalone understands. */
    int readerVersion = 1;
    int writerVersion = 2;
}
