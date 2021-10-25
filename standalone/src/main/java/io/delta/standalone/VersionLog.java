// TODO: copyright

package io.delta.standalone;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

import io.delta.standalone.actions.Action;

/**
 * {@link VersionLog} is the representation of all actions (changes) to the Delta Table
 * at a specific table version.
 */
public class VersionLog {
    private final long version;

    @Nonnull
    private final List<Action> actions;

    public VersionLog(long version, @Nonnull List<Action> actions) {
        this.version = version;
        this.actions = actions;
    }

    /**
     * @return the table version at which these actions occured
     */
    public long getVersion() {
        return version;
    }

    /**
     * @return an unmodifiable {@code List} of the actions for this table version
     */
    @Nonnull
    public List<Action> getActions() {
        return Collections.unmodifiableList(actions);
    }
}
