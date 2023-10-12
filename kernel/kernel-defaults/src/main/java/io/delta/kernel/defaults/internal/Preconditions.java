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
package io.delta.kernel.defaults.internal;

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
}
