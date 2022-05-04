package io.delta.flink.source.internal.exceptions;

import java.util.Collection;
import java.util.Collections;

/**
 * Exception throw during validation of Delta source builder. It contains all validation error
 * messages that occurred during this validation.
 */
public class DeltaSourceValidationException extends RuntimeException {

    /**
     * Path to Delta table for which exception was thrown. Can be null if exception was thrown on
     * missing path to Delta table.
     */
    private final String tablePath;

    /**
     * Collection with all validation error messages that were recorded for this exception.
     */
    private final Collection<String> validationMessages;

    public DeltaSourceValidationException(String tablePath, Collection<String> validationMessages) {
        this.tablePath = String.valueOf(tablePath);
        this.validationMessages =
            (validationMessages == null) ? Collections.emptyList() : validationMessages;
    }

    @Override
    public String getMessage() {

        String validationMessages = String.join(System.lineSeparator(), this.validationMessages);

        return "Invalid Delta Source definition detected."
            + System.lineSeparator()
            + "The reported issues are:"
            + System.lineSeparator()
            + validationMessages;
    }

    public String getTablePath() {
        return tablePath;
    }

    public Collection<String> getValidationMessages() {
        return Collections.unmodifiableCollection(this.validationMessages);
    }
}
