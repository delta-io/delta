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
package io.delta.spark.internal.v2.catalog;

import io.delta.kernel.unitycatalog.UnityCatalogUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.CatalogV2Util;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.delta.util.CatalogTableUtils;
import org.apache.spark.sql.types.StructType;

/** Builds the catalog registration payload from a committed CREATE TABLE version-0 snapshot. */
public final class CreateTableCatalogPublisher {

  public CreateTableCatalogPublication buildCatalogPublication(
      Map<String, String> originalProperties, CommittedCreateTableOperation committed) {
    Map<String, String> registrationProperties =
        CreateTablePropertySupport.filterCredentialProperties(originalProperties);
    if (CatalogTableUtils.isUnityCatalogManagedTableFromProperties(originalProperties)) {
      registrationProperties.putIfAbsent(
          TableCatalog.PROP_LOCATION, committed.getPostCommitSnapshot().getPath());
      registrationProperties.putIfAbsent(TableCatalog.PROP_IS_MANAGED_LOCATION, "true");
    }
    registrationProperties.putAll(
        UnityCatalogUtils.getPropertiesForCreate(
            committed.getEngine(), committed.getPostCommitSnapshot()));
    CreateTablePropertySupport.normalizeUcTableIdProperty(registrationProperties);

    StructType sparkSchema =
        SchemaUtils.convertKernelSchemaToSparkSchema(committed.getPostCommitSnapshot().getSchema());
    return new CreateTableCatalogPublication(
        CatalogV2Util.structTypeToV2Columns(sparkSchema),
        committed.getPostCommitSnapshot().getPartitionColumnNames().stream()
            .map(Expressions::identity)
            .toArray(org.apache.spark.sql.connector.expressions.Transform[]::new),
        registrationProperties);
  }
}
