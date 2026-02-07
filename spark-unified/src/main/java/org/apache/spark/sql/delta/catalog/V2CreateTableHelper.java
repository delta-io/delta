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
package org.apache.spark.sql.delta.catalog;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.catalyst.catalog.CatalogUtils;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.sources.DeltaSourceUtils;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Option$;
import scala.Tuple2;
import scala.collection.immutable.$colon$colon;
import scala.collection.immutable.Map$;
import scala.collection.immutable.Nil$;

/**
 * Helpers for implementing metadata-only CREATE TABLE in the unified DeltaCatalog STRICT path.
 *
 * <p>This isolates Spark-catalyst table descriptor construction and SessionCatalog registration so
 * the Java catalog routing code stays small and readable.
 */
final class V2CreateTableHelper {

  private V2CreateTableHelper() {}

  static Tuple2<CatalogTable, String> buildCatalogTableSpec(
      SparkSession spark,
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) {
    Objects.requireNonNull(spark, "spark is null");
    Objects.requireNonNull(ident, "ident is null");
    Objects.requireNonNull(schema, "schema is null");
    Objects.requireNonNull(partitions, "partitions is null");
    Objects.requireNonNull(properties, "properties is null");

    String[] namespace = ident.namespace();
    String db =
        namespace != null && namespace.length > 0
            ? namespace[namespace.length - 1]
            : spark.sessionState().catalog().getCurrentDatabase();

    TableIdentifier tableIdent = new TableIdentifier(ident.name(), Option$.MODULE$.apply(db));
    if (spark.sessionState().catalog().tableExists(tableIdent)) {
      TableAlreadyExistsException tae = new TableAlreadyExistsException(ident);
      V2CreateTableHelper.<RuntimeException>sneakyThrow(tae);
    }

    String location = properties.get(TableCatalog.PROP_LOCATION);
    boolean isManagedLocation =
        "true".equalsIgnoreCase(properties.get(TableCatalog.PROP_IS_MANAGED_LOCATION));

    CatalogTableType tableType =
        (location == null || location.isEmpty() || isManagedLocation)
            ? CatalogTableType.MANAGED()
            : CatalogTableType.EXTERNAL();

    URI locationUri =
        location != null
            ? CatalogUtils.stringToURI(location)
            : spark.sessionState().catalog().defaultTablePath(tableIdent);

    scala.collection.immutable.Map<String, String> tableProperties = filterTableProperties(properties);
    Option<String> commentOpt = Option$.MODULE$.apply(properties.get(TableCatalog.PROP_COMMENT));

    scala.collection.immutable.Seq<String> partitionColumnNames =
        toPartitionColumnNames(partitions);

    // Provider is always delta for this code path (validated by DeltaCatalog routing).
    Option<String> providerOpt = Option$.MODULE$.apply(DeltaSourceUtils.ALT_NAME());

    CatalogStorageFormat storage =
        CatalogStorageFormat.empty()
            .copy(
                Option$.MODULE$.apply(locationUri),
                Option$.MODULE$.empty(),
                Option$.MODULE$.empty(),
                Option$.MODULE$.empty(),
                CatalogStorageFormat.empty().compressed(),
                CatalogStorageFormat.empty().properties());

    CatalogTable tableDesc =
        new CatalogTable(
            tableIdent,
            tableType,
            storage,
            schema,
            providerOpt,
            partitionColumnNames,
            CatalogTable.apply$default$7(),
            CatalogTable.apply$default$8(),
            CatalogTable.apply$default$9(),
            CatalogTable.apply$default$10(),
            CatalogTable.apply$default$11(),
            tableProperties,
            CatalogTable.apply$default$13(),
            CatalogTable.apply$default$14(),
            commentOpt,
            CatalogTable.apply$default$16(),
            CatalogTable.apply$default$17(),
            CatalogTable.apply$default$18(),
            CatalogTable.apply$default$19(),
            CatalogTable.apply$default$20(),
            CatalogTable.apply$default$21());

    return new Tuple2<>(tableDesc, locationUri.toString());
  }

  static void registerTable(SparkSession spark, CatalogTable tableDesc) {
    Objects.requireNonNull(spark, "spark is null");
    Objects.requireNonNull(tableDesc, "tableDesc is null");

    if (CatalogTableType.MANAGED().equals(tableDesc.tableType())) {
      // The Kernel commit creates the table directory before we register the table in Spark's
      // catalog, so location validation would fail for managed tables.
      spark.sessionState().catalog().createTable(tableDesc, false, false);
    } else {
      spark.sessionState().catalog().createTable(tableDesc, false, true);
    }
  }

  private static scala.collection.immutable.Map<String, String> filterTableProperties(
      Map<String, String> properties) {
    scala.collection.immutable.Map<String, String> result = emptyScalaMap();
    for (Map.Entry<String, String> e : properties.entrySet()) {
      String key = e.getKey();
      if (key == null) {
        continue;
      }
      switch (key) {
        case TableCatalog.PROP_LOCATION:
        case TableCatalog.PROP_PROVIDER:
        case TableCatalog.PROP_COMMENT:
        case TableCatalog.PROP_OWNER:
        case TableCatalog.PROP_EXTERNAL:
        case "path":
        case "option.path":
          continue;
        default:
          break;
      }
      result = result.$plus(new scala.Tuple2<>(key, e.getValue()));
    }
    return result;
  }

  private static scala.collection.immutable.Seq<String> toPartitionColumnNames(Transform[] partitions) {
    List<String> cols = new ArrayList<>(partitions.length);
    for (Transform transform : partitions) {
      if (!(transform instanceof IdentityTransform)) {
        throw new UnsupportedOperationException(
            "Partitioning by expressions is not supported: " + transform.name());
      }
      NamedReference[] refs = transform.references();
      if (refs == null || refs.length != 1) {
        throw new IllegalArgumentException("Invalid partition transform: " + transform.name());
      }
      String[] fieldNames = refs[0].fieldNames();
      if (fieldNames == null || fieldNames.length != 1) {
        throw new UnsupportedOperationException(
            "Partition columns must be top-level columns: " + refs[0].describe());
      }
      cols.add(fieldNames[0]);
    }

    scala.collection.immutable.List<String> list =
        (scala.collection.immutable.List<String>) (scala.collection.immutable.List) Nil$.MODULE$;
    for (int i = cols.size() - 1; i >= 0; i--) {
      list = new $colon$colon<>(cols.get(i), list);
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  private static scala.collection.immutable.Map<String, String> emptyScalaMap() {
    return (scala.collection.immutable.Map<String, String>)
        (scala.collection.immutable.Map) Map$.MODULE$.empty();
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void sneakyThrow(Throwable throwable) throws E {
    throw (E) throwable;
  }
}

