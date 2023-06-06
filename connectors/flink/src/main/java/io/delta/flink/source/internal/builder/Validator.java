package io.delta.flink.source.internal.builder;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * This class provides a methods to check validation conditions and store validation error
 * messages.
 */
public class Validator {

    /**
     * A {@link Set} with validation messages that this instance of {@link Validator} recorded.
     */
    private final Set<String> validationMessages = new HashSet<>();


    /**
     * Ensures that the given object reference is not null. Upon violation, the provided
     * errorMessage is recorded in {@link Validator} state.
     *
     * @param reference    The object reference.
     * @param errorMessage The message that should be recorded as a validation error message for
     *                     this condition.
     */
    public Validator checkNotNull(Object reference, String errorMessage) {
        if (reference == null) {
            validationMessages.add(String.valueOf(errorMessage));
        }
        return this;
    }

    /**
     * Checks the given boolean condition, when condition is not met (evaluates to {@code false})
     * the provided error message is recorded in {@link Validator} state.
     *
     * @param condition    The condition to check
     * @param errorMessage The message that should be recorded as a validation error message for
     *                     this condition.
     */
    public Validator checkArgument(boolean condition, String errorMessage) {
        if (!condition) {
            validationMessages.add(String.valueOf(errorMessage));
        }
        return this;
    }

    /**
     * @return An unmodifiable set of validation messages recorded by this {@link Validator}
     * instance.
     */
    public Set<String> getValidationMessages() {
        return Collections.unmodifiableSet(validationMessages);
    }

    /**
     * @return true if any validation message was recorded, otherwise returns false.
     */
    public boolean containsMessages() {
        return !this.validationMessages.isEmpty();
    }
}
