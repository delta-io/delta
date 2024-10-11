/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.delta.table;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import java.util.List;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;

/** Utility methods for working with Delta types. */
public class DeltaTypeUtil {
  private DeltaTypeUtil() {}

  /**
   * Convert a Delta table schema to Iceberg.
   *
   * <p>Note: this will assign missing field IDs during conversion.
   *
   * @param struct a Delta table's StructType
   * @return an Iceberg StructType
   */
  public static Types.StructType convert(StructType struct) {
    Pair<NameMapping, Integer> mapping = createNameMapping(struct, highestFieldId(struct));
    return DeltaTypeVisitor.visit(struct, new DeltaTypeToType(mapping.first())).asStructType();
  }

  /**
   * Convert a Delta table schema to Iceberg.
   *
   * <p>The {@link NameMapping} is applied to assign any missing field IDs.
   *
   * @param struct a Delta table's StructType
   * @param mapping a {@link NameMapping} that maps missing fields (such as map keys) to IDs
   * @return an Iceberg StructType
   */
  public static Types.StructType convert(StructType struct, NameMapping mapping) {
    return DeltaTypeVisitor.visit(struct, new DeltaTypeToType(mapping)).asStructType();
  }

  public static Pair<NameMapping, Integer> createNameMapping(
      StructType struct, int lastAssignedId) {
    CreateNameMapping visitor = new CreateNameMapping(lastAssignedId);
    NameMapping result = NameMapping.of(DeltaTypeVisitor.visit(struct, visitor).fields());
    int newLastAssignedId = visitor.lastAssignedId();

    return Pair.of(result, newLastAssignedId);
  }

  public static Pair<NameMapping, Integer> updateNameMapping(
      StructType struct, NameMapping mapping, int lastAssignedId) {
    CreateNameMapping visitor = new CreateNameMapping(mapping, lastAssignedId);
    NameMapping result = NameMapping.of(DeltaTypeVisitor.visit(struct, visitor).fields());
    int newLastAssignedId = visitor.lastAssignedId();

    return Pair.of(result, newLastAssignedId);
  }

  public static DataType convert(Type.PrimitiveType type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return BooleanType.BOOLEAN;
      case INTEGER:
        return IntegerType.INTEGER;
      case LONG:
        return LongType.LONG;
      case FLOAT:
        return FloatType.FLOAT;
      case DOUBLE:
        return DoubleType.DOUBLE;
      case DATE:
        return DateType.DATE;
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return TimestampType.TIMESTAMP;
        } else {
          return TimestampNTZType.TIMESTAMP_NTZ;
        }
      case STRING:
      case UUID:
        return StringType.STRING;
      case BINARY:
      case FIXED:
        return BinaryType.BINARY;
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) type;
        return new DecimalType(decimal.precision(), decimal.scale());
    }

    throw new UnsupportedOperationException("Cannot convert type: " + type);
  }

  /**
   * Finds the highest assigned field ID in the Delta type.
   *
   * @param struct a Delta struct
   * @return the highest field ID for any field in the type
   */
  private static int highestFieldId(StructType struct) {
    return DeltaTypeVisitor.visit(struct, HighestFieldId.INSTANCE);
  }

  private static class HighestFieldId extends DeltaTypeVisitor<Integer> {
    private static final String FIELD_ID_KEY = "delta.columnMapping.id";
    private static final HighestFieldId INSTANCE = new HighestFieldId();

    private HighestFieldId() {}

    @Override
    public Integer struct(StructType struct, List<Integer> fieldResults) {
      int maxId = 0;
      for (int fieldId : fieldResults) {
        maxId = Math.max(maxId, fieldId);
      }

      return maxId;
    }

    @Override
    public Integer field(StructField field, Integer typeResult) {
      FieldMetadata metadata = field.getMetadata();
      Object deltaId = metadata.get(FIELD_ID_KEY);
      if (deltaId instanceof Number) {
        return Math.max(((Number) deltaId).intValue(), typeResult);
      } else {
        return typeResult;
      }
    }

    @Override
    public Integer array(ArrayType array, Integer elementResult) {
      return elementResult;
    }

    @Override
    public Integer map(MapType map, Integer keyResult, Integer valueResult) {
      return Math.max(keyResult, valueResult);
    }

    @Override
    public Integer atomic(DataType atomic) {
      return 0;
    }
  }
}
