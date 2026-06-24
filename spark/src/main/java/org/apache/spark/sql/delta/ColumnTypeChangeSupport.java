/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import org.apache.spark.sql.delta.constraints.Constraints$;
import org.apache.spark.sql.delta.schema.SchemaUtils$;
import org.apache.spark.sql.delta.sources.DeltaSQLConf$;
import org.apache.spark.sql.delta.v2.interop.AbstractMetadata;
import org.apache.spark.sql.delta.v2.interop.AbstractProtocol;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.AtomicType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import scala.Option;
import scala.collection.immutable.List;

/**
 * Connector-agnostic checks for whether the data type of a column or nested field can be changed
 * on a Delta table.
 */
public final class ColumnTypeChangeSupport {

  private ColumnTypeChangeSupport() {}

  /** Returns whether the type of the field at {@code fieldNames} can be changed to {@code newType}. */
  public static boolean supportsTypeChange(
      SparkSession spark,
      AbstractProtocol protocol,
      AbstractMetadata metadata,
      List<String> fieldNames,
      DataType newType) {


    SQLConf conf = spark.sessionState().conf();
    if (!TypeWidening$.MODULE$.isEnabled(protocol, metadata)) {
      return false;
    }
    Option<StructField> fromField =
        SchemaUtils$.MODULE$.findNestedFieldIgnoreCase(metadata.schema(), fieldNames, true);
    if (fromField.isEmpty()
        || !(fromField.get().dataType() instanceof AtomicType)
        || !(newType instanceof AtomicType)) {
      return false;
    }
    AtomicType fromType = (AtomicType) fromField.get().dataType();
    AtomicType toType = (AtomicType) newType;
    return isSupportedWidening(conf, metadata, fromType, toType)
        && !hasBlockingDependency(spark, protocol, metadata, fieldNames);
  }

  private static boolean isSupportedWidening(
      SQLConf conf, AbstractMetadata metadata, AtomicType fromType, AtomicType toType) {
    boolean uniformIcebergCompatibleOnly =
        UniversalFormat$.MODULE$.icebergEnabled(metadata.configuration());
    String mode = conf.getConf(DeltaSQLConf$.MODULE$.DELTA_ALLOW_AUTOMATIC_WIDENING());
    switch (mode) {
      case "ALWAYS":
        return TypeWidening$.MODULE$.isTypeChangeSupported(
            fromType, toType, uniformIcebergCompatibleOnly);
      case "SAME_FAMILY_TYPE":
        return TypeWidening$.MODULE$.isTypeChangeSupportedForSchemaEvolution(
            fromType, toType, uniformIcebergCompatibleOnly);
      default: // NEVER
        return false;
    }
  }

  /**
   * Whether a column fits any condition that prevents changing its type, e.g it's referenced by a
   * generated column expression or a CHECK constraint that may become malformed.
   */
  private static boolean hasBlockingDependency(
      SparkSession spark,
      AbstractProtocol protocol,
      AbstractMetadata metadata,
      List<String> fieldNames) {

    boolean dependentGeneratedColumn =
        !SchemaUtils$.MODULE$
            .findDependentGeneratedColumns(spark, fieldNames, protocol, metadata.schema())
            .isEmpty();
    boolean dependentConstraint =
        !Constraints$.MODULE$.findDependentConstraints(spark, fieldNames, metadata).isEmpty();
    return dependentGeneratedColumn || dependentConstraint;
  }
}
