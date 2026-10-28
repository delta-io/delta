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
package org.apache.spark.sql.delta.v2.utils;

import java.io.Serializable;
import java.time.ZoneId;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.Cast$;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.delta.DeltaColumnMapping;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Option;

/**
 * Converts serialized partition values of one partition schema to their Spark internal values as
 * the V1 connector does: with a non-ANSI cast, so a value that does not parse as the column type
 * reads as null instead of failing the query, whatever the session ANSI setting.
 *
 * <p>Builds each column's cast once, so callers converting many files should create one caster and
 * reuse it. Not thread-safe: a non-ANSI cast from string to an integral type parses every value
 * into one mutable holder, so threads must not share a caster.
 */
public final class PartitionValueCaster implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String[] physicalNames;
  /** The cast of each column's serialized value, or null for string columns. */
  private final Cast[] casts;

  public PartitionValueCaster(StructType partitionSchema, ZoneId zoneId) {
    final StructField[] fields = partitionSchema.fields();
    physicalNames = new String[fields.length];
    casts = new Cast[fields.length];
    final BoundReference serializedValue =
        new BoundReference(0, DataTypes.StringType, /* nullable= */ true);
    for (int index = 0; index < fields.length; index++) {
      physicalNames[index] = DeltaColumnMapping.getPhysicalName(fields[index]);
      if (!(fields[index].dataType() instanceof StringType)) {
        casts[index] =
            Cast$.MODULE$.apply(
                serializedValue,
                fields[index].dataType(),
                Option.apply(zoneId.getId()),
                /* ansiEnabled= */ false);
      }
    }
  }

  int numFields() {
    return physicalNames.length;
  }

  /** The key of column {@code ordinal} in a file's partition-value map. */
  String physicalName(int ordinal) {
    return physicalNames[ordinal];
  }

  /** Converts the serialized {@code value} (null for a null partition) of column ordinal. */
  Object cast(int ordinal, String value) {
    if (value == null) {
      return null;
    }
    final UTF8String serialized = UTF8String.fromString(value);
    if (casts[ordinal] == null) {
      return serialized;
    }
    return casts[ordinal].eval(new GenericInternalRow(new Object[] {serialized}));
  }
}
