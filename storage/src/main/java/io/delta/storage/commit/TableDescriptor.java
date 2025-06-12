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

package io.delta.storage.commit;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.fs.Path;

/**
 * Container for all the info to uniquely identify the table
 */
public class TableDescriptor {

    private Path logPath;
    private Optional<TableIdentifier> tableIdentifier;

    private Map<String, String> tableConf;

    public TableDescriptor(Path logPath, Optional<TableIdentifier> tableIdentifier, Map<String, String> tableConf) {
        this.logPath = logPath;
        this.tableIdentifier = tableIdentifier;
        this.tableConf = tableConf;
    }

    public Optional<TableIdentifier> getTableIdentifier() {
        return tableIdentifier;
    }

    public Path getLogPath() {
        return logPath;
    }

    public Map<String, String> getTableConf() {
        return tableConf;
    }
}
