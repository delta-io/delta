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
package io.delta.kernel;

import java.util.ConcurrentModificationException;

import io.delta.kernel.annotation.Evolving;

/**
 * Thrown when concurrent transaction both attempt to update the same idempotent transaction.
 *
 * @since 3.2.0
 */
@Evolving
public class ConcurrentTransactionException extends ConcurrentModificationException {
    private static final String message = "This error occurs when multiple updates are " +
            "using the same transaction identifier to write into this table.";

    public ConcurrentTransactionException(String appId, long txnVersion) {
        super(message +
                " Application ID: " + appId + ", Transaction Version: " + txnVersion);
    }
}
