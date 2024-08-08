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

/**
 * An operation that can be performed on a Delta table.
 * <p>
 * An operation is tracked as the first line in commit info action inside the Delta Log
 * It also shows up when {@code DESCRIBE HISTORY} on the table is executed.
 */
public enum Operation {

    /**
     * Recorded when the table is created.
     */
    CREATE_TABLE("CREATE TABLE"),

    /**
     * Recorded during batch inserts.
     */
    WRITE("WRITE"),

    /**
     * Recorded during streaming inserts.
     */
    STREAMING_UPDATE("STREAMING UPDATE"),

    UPGRADE_SCHEMA("UPGRADE SCHEMA"),
    COMPACT("COMPACT"),
    VACUUM("UPGRADE SCHEMA"),

    /**
     * For any operation that doesn't fit the above categories.
     */
    MANUAL_UPDATE("Manual Update");

    /**
     * Actual value that will be recorded in the transaction log
     */
    private final String description;

    Operation(String description) {
        this.description = description;
    }

    /**
     * Returns the string that will be recorded in the transaction log.
     */
    public String getDescription() {
        return description;
    }
}
