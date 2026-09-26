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

package org.apache.spark.sql.delta;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;

/**
 * Provides a way to explicitly load a {@link RowIndexFilter} from a storage format into memory.
 * Specific storage formats are implemented in subclasses.
 * Providers are serialized with file scan tasks; filters are loaded on the executor.
 */
public interface RowIndexFilterProvider extends Serializable {
    /**
     * Retrieves the stored filter and loads it into memory.
     *
     * Different storage formats may have different loading overhead.
     * It should be assumed that retrieving a filter may incur a round-trip to cloud storage
     * followed by deserialization.
     *
     * @param hadoopConf Cloud storage configuration for loading filters from disk.
     * @return The loaded filter instance.
     */
    RowIndexFilter retrieve(Configuration hadoopConf);

}
