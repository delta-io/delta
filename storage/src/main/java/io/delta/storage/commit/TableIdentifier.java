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

/**
 * Identifier for a table.
 */
public class TableIdentifier {

    // The name of the table.
    private String name;

    // The namespace of the table. e.g. <catalog> / <schema>
    private String[] namespace;

    public TableIdentifier(String[] namespace, String name) {
        this.namespace = namespace;
        this.name = name;
    }

    public TableIdentifier(String firstName, String... rest) {
        String[] namespace = new String[rest.length];
        String name;
        if (rest.length > 0) {
            name = rest[rest.length-1];
            namespace[0] = firstName;
            System.arraycopy(rest, 0, namespace, 1, rest.length-1);
        } else {
            name = firstName;
        }
        this.namespace = namespace;
        this.name = name;
    }

    /**
     * Returns the namespace of the table.
     */
    public String[] getNamespace() {
        return namespace;
    }

    /**
     * Returns the name of the table.
     */
    public String getName() {
        return name;
    }
}
