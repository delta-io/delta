/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.internal.util;

/**
 * Static convenience methods that help a method or constructor check whether it was invoked
 * correctly (that is, whether its preconditions were met).
 */
public class Preconditions {
    private Preconditions() {}

    /**
     * Precondition-style validation that throws {@link IllegalArgumentException}.
     *
     * @param isValid {@code true} if valid, {@code false} if an exception should be thrown
     * @throws IllegalArgumentException if {@code isValid} is false
     */
    public static void checkArgument(boolean isValid)
            throws IllegalArgumentException {
        if (!isValid) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Precondition-style validation that throws {@link IllegalArgumentException}.
     *
     * @param isValid {@code true} if valid, {@code false} if an exception should be thrown
     * @param message A String message for the exception.
     * @throws IllegalArgumentException if {@code isValid} is false
     */
    public static void checkArgument(boolean isValid, String message)
            throws IllegalArgumentException {
        if (!isValid) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Precondition-style validation that throws {@link IllegalArgumentException}.
     *
     * @param isValid {@code true} if valid, {@code false} if an exception should be thrown
     * @param message A String message for the exception.
     * @param args    Objects used to fill in {@code %s} placeholders in the message
     * @throws IllegalArgumentException if {@code isValid} is false
     */
    public static void checkArgument(boolean isValid, String message, Object... args)
            throws IllegalArgumentException {
        if (!isValid) {
            throw new IllegalArgumentException(
                    String.format(String.valueOf(message), args));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling instance.
     *
     * @param expression a boolean expression
     * @param errorMessage the exception message to use if the check fails
     * @throws IllegalStateException if {@code expression} is false
     */
    public static void checkState(boolean expression, String errorMessage) {
        if (!expression) {
            throw new IllegalStateException(String.valueOf(errorMessage));
        }
    }
}
