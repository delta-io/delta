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

package org.apache.spark.sql.delta.catalog;

import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.spark.catalog.SparkTable;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.delta.DeltaTableIdentifier;
import org.apache.spark.sql.delta.DeltaTableUtils;
import scala.Option;
import scala.collection.immutable.Map$;

/**
 * Delta Catalog implementation that can delegate to both V1 and V2 implementations. This class sits
 * in delta-spark (combined) module and can access: - V1: org.apache.spark.sql.delta.* (full version
 * with DeltaLog) - V2: io.delta.kernel.spark.*
 */
public class DeltaCatalog extends AbstractDeltaCatalog {

  @Override
  public Table loadTable(Identifier identifier) {
    try {
      // Load table from delegate catalog directly
      Table delegateTable = ((TableCatalog) delegate).loadTable(identifier);
      // If delegate table is a V1Table and it's a Delta table, check if we should use SparkTable
      if (delegateTable instanceof V1Table) {
        V1Table v1Table = (V1Table) delegateTable;
        if (DeltaTableUtils.isDeltaTable(v1Table.catalogTable())) {
          // Check if schema contains VOID/NullType - if so, use DeltaTableV2 to avoid Kernel issues
          if (containsVoidType(v1Table.catalogTable().schema())) {
            return org.apache.spark.sql.delta.catalog.DeltaTableV2$.MODULE$.apply(
                SparkSession.active(),
                new Path(v1Table.catalogTable().location()),
                Option.apply(v1Table.catalogTable()),
                Option.apply(identifier.toString()),
                Map$.MODULE$.<String, String>empty(),
                Option.empty());
          }
          try {
          return new SparkTable(identifier, v1Table.catalogTable());
          } catch (TableNotFoundException ignored) {
            throw new NoSuchTableException(identifier);
          }
        }
      }
      // Otherwise return the delegate table as-is
      return delegateTable;
    } catch (NoSuchTableException e) {
      // Handle path-based tables
      if (isPathIdentifier(identifier)) {
        return newDeltaPathTable(identifier);
      } else if (isIcebergPathIdentifier(identifier)) {
        return newIcebergPathTable(identifier);
      } else {
        // *** KEY FIX: Directly rethrow NoSuchTableException as unchecked ***
        // This allows saveAsTable to catch it properly
        // Use uncheckedThrow to bypass Java's checked exception requirement
        throw uncheckedThrow(e);
      }
    } catch (AnalysisException e) {
      // Handle other AnalysisException subtypes
      if (e instanceof NoSuchNamespaceException || e instanceof NoSuchDatabaseException) {
        if (isPathIdentifier(identifier)) {
          return newDeltaPathTable(identifier);
        } else if (isIcebergPathIdentifier(identifier)) {
          return newIcebergPathTable(identifier);
        }
      } else if (DeltaTableIdentifier.gluePermissionError(e) && isPathIdentifier(identifier)) {
        return newDeltaPathTable(identifier);
      }
      // For other AnalysisException, wrap in RuntimeException
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Check if the schema contains VOID/NullType fields.
   * Tables with VOID type should use V1 implementation to avoid Kernel parsing issues.
   */
  private boolean containsVoidType(org.apache.spark.sql.types.StructType schema) {
    if (schema == null) {
      return false;
    }
    for (org.apache.spark.sql.types.StructField field : schema.fields()) {
      if (containsVoidTypeInDataType(field.dataType())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Recursively check if a DataType contains VOID/NullType.
   */
  private boolean containsVoidTypeInDataType(org.apache.spark.sql.types.DataType dataType) {
    if (dataType instanceof org.apache.spark.sql.types.NullType) {
      return true;
    }
    if (dataType instanceof org.apache.spark.sql.types.StructType) {
      org.apache.spark.sql.types.StructType structType = 
          (org.apache.spark.sql.types.StructType) dataType;
      for (org.apache.spark.sql.types.StructField field : structType.fields()) {
        if (containsVoidTypeInDataType(field.dataType())) {
          return true;
        }
      }
    } else if (dataType instanceof org.apache.spark.sql.types.ArrayType) {
      org.apache.spark.sql.types.ArrayType arrayType = 
          (org.apache.spark.sql.types.ArrayType) dataType;
      return containsVoidTypeInDataType(arrayType.elementType());
    } else if (dataType instanceof org.apache.spark.sql.types.MapType) {
      org.apache.spark.sql.types.MapType mapType = 
          (org.apache.spark.sql.types.MapType) dataType;
      return containsVoidTypeInDataType(mapType.keyType()) || 
             containsVoidTypeInDataType(mapType.valueType());
    }
    return false;
  }

  @Override
  public Table loadTableForAlterTable(Identifier identifier) {
    return super.loadTable(identifier);
  }


  /**
   * Utility method to throw checked exceptions as unchecked.
   * This is a workaround for Java's checked exception requirement when overriding Scala methods.
   */
  @SuppressWarnings("unchecked")
  private static <E extends Throwable> RuntimeException uncheckedThrow(Throwable e) throws E {
    throw (E) e;
  }
}
