/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.ddl;

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.Map;
import org.apache.spark.sql.types.StructType;

/**
 * Catalog publication payload for CREATE TABLE.
 *
 * <p>Derived from the committed snapshot — not from raw user input. This ensures that the catalog
 * registration reflects the actual v0 commit (including Kernel-added defaults like protocol
 * features).
 */
public class CreateTableCatalogPublication {
  private final StructType schema;
  private final Map<String, String> properties;
  private final String location;

  public CreateTableCatalogPublication(
      StructType schema, Map<String, String> properties, String location) {
    this.schema = requireNonNull(schema, "schema is null");
    this.properties = Collections.unmodifiableMap(requireNonNull(properties, "properties is null"));
    this.location = requireNonNull(location, "location is null");
  }

  public StructType getSchema() {
    return schema;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public String getLocation() {
    return location;
  }
}
