/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.exceptions;


import io.delta.kernel.annotation.Evolving;

/**
 * Thrown when the protocol of the Delta table has changed between the time of transaction start
 * and the time of commit.
 *
 * @since 3.2.0
 */
@Evolving
public class ProtocolChangedException extends ConcurrentWriteException {
    private static final String helpfulMsgForNewTables = " This happens when multiple writers " +
            "are writing to an empty directory. Creating the table ahead of time will avoid this " +
            "conflict.";

    public ProtocolChangedException(long attemptVersion) {
        super(String.format("Transaction has encountered a conflict and can not be committed. " +
                "Query needs to be re-executed using the latest version of the table.%s",
                attemptVersion == 0 ? helpfulMsgForNewTables : ""));
    }
}
