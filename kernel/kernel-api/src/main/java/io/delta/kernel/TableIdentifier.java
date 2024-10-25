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

package io.delta.kernel;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.annotation.Evolving;

/**
 * Identifier for a table. e.g. $catalog / $schema / $tableName
 *
 * @since 3.3
 */
@Evolving
public class TableIdentifier {
  /** The namespace of the table. */
  private final String[] namespace;

  /** The name of the table. */
  private final String name;

  public TableIdentifier(String[] namespace, String name) {
    checkArgument(namespace != null && namespace.length > 0, "namespace cannot be null or empty");
    this.namespace = namespace;
    this.name = requireNonNull(name, "name is null");
  }

  /** @return the namespace of the table. e.g. $catalog / $schema */
  public String[] getNamespace() {
    return namespace;
  }

  /** @return the name of the table. */
  public String getName() {
    return name;
  }
}
