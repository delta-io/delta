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

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

/** Plain data bag for a CREATE TABLE operation — just the raw inputs, nothing else. */
public final class CreateTableContext {
  public final Identifier ident;
  public final StructType schema;
  public final Transform[] partitions;
  public final Map<String, String> properties;
  public final Configuration hadoopConf;
  public final boolean isUnityCatalog;

  public CreateTableContext(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties,
      Configuration hadoopConf,
      boolean isUnityCatalog) {
    this.ident = requireNonNull(ident, "ident is null");
    this.schema = requireNonNull(schema, "schema is null");
    this.partitions = requireNonNull(partitions, "partitions is null");
    this.properties = requireNonNull(properties, "properties is null");
    this.hadoopConf = requireNonNull(hadoopConf, "hadoopConf is null");
    this.isUnityCatalog = isUnityCatalog;
  }
}
