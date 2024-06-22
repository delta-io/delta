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

import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.engine.Engine;

/**
 * Thrown when concurrent transaction both attempt to update the table with same transaction
 * identifier set through {@link TransactionBuilder#withTransactionId(Engine, String, long)}
 * (String)}.
 * <p>
 * Incremental processing systems (e.g., streaming systems) that track progress using their own
 * application-specific versions need to record what progress has been made, in order to avoid
 * duplicating data in the face of failures and retries during writes. For more information refer to
 * the Delta protocol section <a
 * href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#transaction-identifiers">
 * Transaction Identifiers</a>
 *
 * @since 3.2.0
 */
@Evolving
public class ConcurrentTransactionException extends ConcurrentWriteException {
    private static final String message = "This error occurs when multiple updates are " +
            "using the same transaction identifier to write into this table.\n" +
            "Application ID: %s, Attempted version: %s, Latest version in table: %s";

    public ConcurrentTransactionException(String appId, long txnVersion, long lastUpdated) {
        super(String.format(message, appId, txnVersion, lastUpdated));
    }
}
