package io.delta.flink.internal.options;

import java.util.Collection;
import java.util.Collections;

import org.apache.flink.core.fs.Path;

/**
 * Exception throw during validation of Delta connector options.
 */
public class DeltaOptionValidationException extends RuntimeException {

    /**
     * Path to Delta table for which exception was thrown. Can be null if exception was thrown on
     * missing path to Delta table.
     */
    private final String tablePath;

    /**
     * Collection with all validation error messages that were recorded for this exception.
     */
    private final Collection<String> validationMessages;

    public DeltaOptionValidationException(Path tablePath, Collection<String> validationMessages) {
        this(String.valueOf(tablePath), validationMessages);
    }

    public DeltaOptionValidationException(
            String tablePathString,
            Collection<String> validationMessages) {
        this.tablePath = tablePathString;
        this.validationMessages =
            (validationMessages == null) ? Collections.emptyList() : validationMessages;

    }

    @Override
    public String getMessage() {

        String validationMessages = String.join(System.lineSeparator(), this.validationMessages);

        return "Invalid Delta connector definition detected."
            + System.lineSeparator()
            + "The reported issues are:"
            + System.lineSeparator()
            + validationMessages;
    }

    /** Table path for this exception. */
    public String getTablePath() {
        return tablePath;
    }

    /** Detailed validation messages for the cause of this exception. */
    public Collection<String> getValidationMessages() {
        return Collections.unmodifiableCollection(this.validationMessages);
    }

}
