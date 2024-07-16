/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.kernel.commit.actions;

import java.util.List;
import java.util.Map;

/**
 * Interface for metadata actions in Delta. The metadata defines the metadata
 * of the table.
 */
public interface AbstractMetadata {

    /**
     * A unique table identifier.
     */
    String getId();

    /**
     * User-specified table identifier.
     */
    String getName();

    /**
     * User-specified table description.
     */
    String getDescription();

    /** The table provider format. */
    String getProvider();

    /** The format options */
    Map<String, String> getFormatOptions();

    /**
     * The table schema in string representation.
     */
    String getSchemaString();

    /**
     * List of partition columns.
     */
    List<String> getPartitionColumns();

    /**
     * The table properties defined on the table.
     */
    Map<String, String> getConfiguration();

    /**
     * Timestamp for the creation of this metadata.
     */
    Long getCreatedTime();
}
