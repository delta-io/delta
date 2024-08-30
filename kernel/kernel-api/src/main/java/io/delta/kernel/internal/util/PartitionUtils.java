/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.util;

import static io.delta.kernel.expressions.AlwaysFalse.ALWAYS_FALSE;
import static io.delta.kernel.expressions.AlwaysTrue.ALWAYS_TRUE;
import static io.delta.kernel.internal.DeltaErrors.wrapEngineException;
import static io.delta.kernel.internal.util.InternalUtils.toLowerCaseSet;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.SchemaUtils.casePreservingPartitionColNames;
import static java.util.Arrays.asList;

import io.delta.kernel.data.*;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.expressions.*;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PartitionUtils {
  private static final DateTimeFormatter PARTITION_TIMESTAMP_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

  private PartitionUtils() {}

  /**
   * Utility method to remove the given columns (as {@code columnsToRemove}) from the given {@code
   * physicalSchema}.
   *
   * @param physicalSchema
   * @param logicalSchema To create a logical name to physical name map. Partition column names are
   *     in logical space and we need to identify the equivalent physical column name.
   * @param columnsToRemove
   * @return
   */
  public static StructType physicalSchemaWithoutPartitionColumns(
      StructType logicalSchema, StructType physicalSchema, Set<String> columnsToRemove) {
    if (columnsToRemove == null || columnsToRemove.size() == 0) {
      return physicalSchema;
    }

    // Partition columns are top-level only
    Map<String, String> physicalToLogical =
        new HashMap<String, String>() {
          {
            IntStream.range(0, logicalSchema.length())
                .mapToObj(i -> new Tuple2<>(logicalSchema.at(i), physicalSchema.at(i)))
                .forEach(tuple2 -> put(tuple2._2.getName(), tuple2._1.getName()));
          }
        };

    return new StructType(
        physicalSchema.fields().stream()
            .filter(field -> !columnsToRemove.contains(physicalToLogical.get(field.getName())))
            .collect(Collectors.toList()));
  }

  public static ColumnarBatch withPartitionColumns(
      ExpressionHandler expressionHandler,
      ColumnarBatch dataBatch,
      Map<String, String> partitionValues,
      StructType schemaWithPartitionCols) {
    if (partitionValues == null || partitionValues.size() == 0) {
      // no partition column vectors to attach to.
      return dataBatch;
    }

    for (int colIdx = 0; colIdx < schemaWithPartitionCols.length(); colIdx++) {
      StructField structField = schemaWithPartitionCols.at(colIdx);

      if (partitionValues.containsKey(structField.getName())) {
        // Create a partition vector

        ColumnarBatch finalDataBatch = dataBatch;
        ExpressionEvaluator evaluator =
            wrapEngineException(
                () ->
                    expressionHandler.getEvaluator(
                        finalDataBatch.getSchema(),
                        literalForPartitionValue(
                            structField.getDataType(), partitionValues.get(structField.getName())),
                        structField.getDataType()),
                "Get the expression evaluator for partition column %s with datatype=%s and "
                    + "value=%s",
                structField.getName(),
                structField.getDataType(),
                partitionValues.get(structField.getName()));

        ColumnVector partitionVector =
            wrapEngineException(
                () -> evaluator.eval(finalDataBatch),
                "Evaluating the partition value expression %s",
                literalForPartitionValue(
                    structField.getDataType(), partitionValues.get(structField.getName())));
        dataBatch = dataBatch.withNewColumn(colIdx, structField, partitionVector);
      }
    }

    return dataBatch;
  }

  /**
   * Convert the given partition values to a {@link MapValue} that can be serialized to a Delta
   * commit file.
   *
   * @param partitionValueMap Expected the partition column names to be same case as in the schema.
   *     We want to preserve the case of the partition column names when serializing to the Delta
   *     commit file.
   * @return {@link MapValue} representing the serialized partition values that can be written to a
   *     Delta commit file.
   */
  public static MapValue serializePartitionMap(Map<String, Literal> partitionValueMap) {
    if (partitionValueMap == null || partitionValueMap.isEmpty()) {
      return VectorUtils.stringStringMapValue(Collections.emptyMap());
    }

    Map<String, String> serializedPartValues = new HashMap<>();
    for (Map.Entry<String, Literal> entry : partitionValueMap.entrySet()) {
      serializedPartValues.put(
          entry.getKey(), // partition column name
          serializePartitionValue(entry.getValue())); // serialized partition value as str
    }

    return VectorUtils.stringStringMapValue(serializedPartValues);
  }

  /**
   * Validate {@code partitionValues} contains values for every partition column in the table and
   * the type of the value is correct. Once validated the partition values are sanitized to match
   * the case of the partition column names in the table schema and returned
   *
   * @param tableSchema Schema of the table.
   * @param partitionColNames Partition column name. These should be from the table metadata that
   *     retain the same case as in the table schema.
   * @param partitionValues Map of partition column to value map given by the connector
   * @return Sanitized partition values.
   */
  public static Map<String, Literal> validateAndSanitizePartitionValues(
      StructType tableSchema,
      List<String> partitionColNames,
      Map<String, Literal> partitionValues) {

    if (!toLowerCaseSet(partitionColNames).equals(toLowerCaseSet(partitionValues.keySet()))) {
      throw new IllegalArgumentException(
          String.format(
              "Partition values provided are not matching the partition columns. "
                  + "Partition columns: %s, Partition values: %s",
              partitionColNames, partitionValues));
    }

    // Convert the partition column names in given `partitionValues` to schema case. Schema
    // case is the exact case the column name was given by the connector when creating the
    // table. Comparing the column names is case-insensitive, but preserve the case as stored
    // in the table metadata when writing the partition column name to DeltaLog
    // (`partitionValues` in `AddFile`) or generating the target directory for writing the
    // data belonging to a partition.
    Map<String, Literal> schemaCasePartitionValues =
        casePreservingPartitionColNames(partitionColNames, partitionValues);

    // validate types are the same
    schemaCasePartitionValues
        .entrySet()
        .forEach(
            entry -> {
              String partColName = entry.getKey();
              Literal partValue = entry.getValue();
              StructField partColField = tableSchema.get(partColName);

              // this shouldn't happen as we have already validated the partition column names
              checkArgument(
                  partColField != null,
                  "Partition column " + partColName + " is not present in the table schema");
              DataType partColType = partColField.getDataType();

              if (!partColType.equivalent(partValue.getDataType())) {
                throw new IllegalArgumentException(
                    String.format(
                        "Partition column %s is of type %s but the value provided is of type %s",
                        partColName, partColType, partValue.getDataType()));
              }
            });

    return schemaCasePartitionValues;
  }

  /**
   * Split the given predicate into predicate on partition columns and predicate on data columns.
   *
   * @param predicate
   * @param partitionColNames
   * @return Tuple of partition column predicate and data column predicate.
   */
  public static Tuple2<Predicate, Predicate> splitMetadataAndDataPredicates(
      Predicate predicate, Set<String> partitionColNames) {
    String predicateName = predicate.getName();
    List<Expression> children = predicate.getChildren();
    if ("AND".equalsIgnoreCase(predicateName)) {
      Predicate left = (Predicate) children.get(0);
      Predicate right = (Predicate) children.get(1);
      Tuple2<Predicate, Predicate> leftResult =
          splitMetadataAndDataPredicates(left, partitionColNames);
      Tuple2<Predicate, Predicate> rightResult =
          splitMetadataAndDataPredicates(right, partitionColNames);

      return new Tuple2<>(
          combineWithAndOp(leftResult._1, rightResult._1),
          combineWithAndOp(leftResult._2, rightResult._2));
    }
    if (hasNonPartitionColumns(children, partitionColNames)) {
      return new Tuple2(ALWAYS_TRUE, predicate);
    } else {
      return new Tuple2<>(predicate, ALWAYS_TRUE);
    }
  }

  /**
   * Rewrite the given predicate on partition columns on `partitionValues_parsed` in checkpoint
   * schema. The rewritten predicate can be pushed to the Parquet reader when reading the checkpoint
   * files.
   *
   * @param predicate Predicate on partition columns.
   * @param partitionColNameToField Map of partition column name (in lower case) to its {@link
   *     StructField}.
   * @return Rewritten {@link Predicate} on `partitionValues_parsed` in `add`.
   */
  public static Predicate rewritePartitionPredicateOnCheckpointFileSchema(
      Predicate predicate, Map<String, StructField> partitionColNameToField) {
    return new Predicate(
        predicate.getName(),
        predicate.getChildren().stream()
            .map(child -> rewriteColRefOnPartitionValuesParsed(child, partitionColNameToField))
            .collect(Collectors.toList()));
  }

  private static Expression rewriteColRefOnPartitionValuesParsed(
      Expression expression, Map<String, StructField> partitionColMetadata) {
    if (expression instanceof Column) {
      Column column = (Column) expression;
      String partColName = column.getNames()[0];
      StructField partColField = partitionColMetadata.get(partColName.toLowerCase(Locale.ROOT));
      if (partColField == null) {
        throw new IllegalArgumentException(partColName + " is not present in metadata");
      }

      String partColPhysicalName = ColumnMapping.getPhysicalName(partColField);

      return InternalScanFileUtils.getPartitionValuesParsedRefInAddFile(partColPhysicalName);
    } else if (expression instanceof Predicate) {
      return rewritePartitionPredicateOnCheckpointFileSchema(
          (Predicate) expression, partitionColMetadata);
    }

    return expression;
  }

  /**
   * Utility method to rewrite the partition predicate referring to the table schema as predicate
   * referring to the {@code partitionValues} in scan files read from Delta log. The scan file batch
   * is returned by the {@link io.delta.kernel.Scan#getScanFiles(Engine)}.
   *
   * <p>E.g. given predicate on partition columns: {@code p1 = 'new york' && p2 >= 26} where p1 is
   * of type string and p2 is of int Rewritten expression looks like: {@code
   * element_at(Column('add', 'partitionValues'), 'p1') = 'new york' &&
   * partition_value(element_at(Column('add', 'partitionValues'), 'p2'), 'integer') >= 26}
   *
   * <p>The column `add.partitionValues` is a {@literal map(string -> string)} type. Each partition
   * values is in string serialization format according to the Delta protocol. Expression
   * `partition_value` deserializes the string value into the given partition column type value.
   * String type partition values don't need any deserialization.
   *
   * @param predicate Predicate containing filters only on partition columns.
   * @param partitionColMetadata Map of partition column name (in lower case) to its type.
   * @return
   */
  public static Predicate rewritePartitionPredicateOnScanFileSchema(
      Predicate predicate, Map<String, StructField> partitionColMetadata) {
    return new Predicate(
        predicate.getName(),
        predicate.getChildren().stream()
            .map(child -> rewritePartitionColumnRef(child, partitionColMetadata))
            .collect(Collectors.toList()));
  }

  private static Expression rewritePartitionColumnRef(
      Expression expression, Map<String, StructField> partitionColMetadata) {
    Column scanFilePartitionValuesRef = InternalScanFileUtils.ADD_FILE_PARTITION_COL_REF;
    if (expression instanceof Column) {
      Column column = (Column) expression;
      String partColName = column.getNames()[0];
      StructField partColField = partitionColMetadata.get(partColName.toLowerCase(Locale.ROOT));
      if (partColField == null) {
        throw new IllegalArgumentException(partColName + " is not present in metadata");
      }
      DataType partColType = partColField.getDataType();
      String partColPhysicalName = ColumnMapping.getPhysicalName(partColField);

      Expression elementAt =
          new ScalarExpression(
              "element_at",
              asList(
                  scanFilePartitionValuesRef,
                  Literal.ofString(partColPhysicalName, "UTF8_BINARY")));

      if (partColType instanceof StringType) {
        return elementAt;
      }

      // Add expression to decode the partition value based on the partition column type.
      return new PartitionValueExpression(elementAt, partColType);
    } else if (expression instanceof Predicate) {
      return rewritePartitionPredicateOnScanFileSchema(
          (Predicate) expression, partitionColMetadata);
    }

    return expression;
  }

  /**
   * Get the target directory for writing data for given partition values. Example: Given partition
   * values (part1=1, part2='abc'), the target directory will be for a table rooted at
   * 's3://bucket/table': 's3://bucket/table/part1=1/part2=abc'.
   *
   * @param dataRoot Root directory where the data is stored.
   * @param partitionColNames Partition column names. We need this to create the target directory
   *     structure that is consistent levels of directories.
   * @param partitionValues Partition values to create the target directory.
   * @return Target directory path.
   */
  public static String getTargetDirectory(
      String dataRoot, List<String> partitionColNames, Map<String, Literal> partitionValues) {
    Path targetDirectory = new Path(dataRoot);
    for (String partitionColName : partitionColNames) {
      Literal partitionValue = partitionValues.get(partitionColName);
      checkArgument(
          partitionValue != null,
          "Partition column value is missing for column: " + partitionColName);
      String serializedValue = serializePartitionValue(partitionValue);
      if (serializedValue == null) {
        // Follow the delta-spark behavior to use "__HIVE_DEFAULT_PARTITION__" for null
        serializedValue = "__HIVE_DEFAULT_PARTITION__";
      } else {
        serializedValue = escapePartitionValue(serializedValue);
      }
      String partitionDirectory = partitionColName + "=" + serializedValue;
      targetDirectory = new Path(targetDirectory, partitionDirectory);
    }

    return targetDirectory.toString();
  }

  private static boolean hasNonPartitionColumns(
      List<Expression> children, Set<String> partitionColNames) {
    for (Expression child : children) {
      if (child instanceof Column) {
        String[] names = ((Column) child).getNames();
        // Partition columns are never of nested types.
        if (names.length != 1 || !partitionColNames.contains(names[0].toLowerCase(Locale.ROOT))) {
          return true;
        }
      } else {
        if (hasNonPartitionColumns(child.getChildren(), partitionColNames)) {
          return true;
        }
      }
    }
    return false;
  }

  private static Predicate combineWithAndOp(Predicate left, Predicate right) {
    String leftName = left.getName().toUpperCase();
    String rightName = right.getName().toUpperCase();
    if (leftName.equals("ALWAYS_FALSE") || rightName.equals("ALWAYS_FALSE")) {
      return ALWAYS_FALSE;
    }
    if (leftName.equals("ALWAYS_TRUE")) {
      return right;
    }
    if (rightName.equals("ALWAYS_TRUE")) {
      return left;
    }
    return new And(left, right);
  }

  protected static Literal literalForPartitionValue(DataType dataType, String partitionValue) {
    if (partitionValue == null) {
      return Literal.ofNull(dataType);
    }

    if (dataType instanceof BooleanType) {
      return Literal.ofBoolean(Boolean.parseBoolean(partitionValue));
    }
    if (dataType instanceof ByteType) {
      return Literal.ofByte(Byte.parseByte(partitionValue));
    }
    if (dataType instanceof ShortType) {
      return Literal.ofShort(Short.parseShort(partitionValue));
    }
    if (dataType instanceof IntegerType) {
      return Literal.ofInt(Integer.parseInt(partitionValue));
    }
    if (dataType instanceof LongType) {
      return Literal.ofLong(Long.parseLong(partitionValue));
    }
    if (dataType instanceof FloatType) {
      return Literal.ofFloat(Float.parseFloat(partitionValue));
    }
    if (dataType instanceof DoubleType) {
      return Literal.ofDouble(Double.parseDouble(partitionValue));
    }
    if (dataType instanceof StringType) {
      return Literal.ofString(partitionValue, ((StringType) dataType).getCollationIdentifier());
    }
    if (dataType instanceof BinaryType) {
      return Literal.ofBinary(partitionValue.getBytes());
    }
    if (dataType instanceof DateType) {
      return Literal.ofDate(InternalUtils.daysSinceEpoch(Date.valueOf(partitionValue)));
    }
    if (dataType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) dataType;
      return Literal.ofDecimal(
          new BigDecimal(partitionValue), decimalType.getPrecision(), decimalType.getScale());
    }
    if (dataType instanceof TimestampType) {
      return Literal.ofTimestamp(InternalUtils.microsSinceEpoch(Timestamp.valueOf(partitionValue)));
    }
    if (dataType instanceof TimestampNTZType) {
      // Both the timestamp and timestamp_ntz have no timezone info, so they are interpreted
      // in local time zone.
      return Literal.ofTimestampNtz(
          InternalUtils.microsSinceEpoch(Timestamp.valueOf(partitionValue)));
    }

    throw new UnsupportedOperationException("Unsupported partition column: " + dataType);
  }

  /**
   * Serialize the given partition value to a string according to the Delta protocol <a
   * href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md
   * #partition-value-serialization">partition value serialization rules</a>.
   *
   * @param literal Literal representing the partition value of specific datatype.
   * @return Serialized string representation of the partition value.
   */
  protected static String serializePartitionValue(Literal literal) {
    Object value = literal.getValue();
    if (value == null) {
      return null;
    }
    DataType dataType = literal.getDataType();
    if (dataType instanceof ByteType
        || dataType instanceof ShortType
        || dataType instanceof IntegerType
        || dataType instanceof LongType
        || dataType instanceof FloatType
        || dataType instanceof DoubleType
        || dataType instanceof BooleanType) {
      return String.valueOf(value);
    } else if (dataType instanceof StringType) {
      return (String) value;
    } else if (dataType instanceof DateType) {
      int daysSinceEpochUTC = (int) value;
      return LocalDate.ofEpochDay(daysSinceEpochUTC).toString();
    } else if (dataType instanceof TimestampType || dataType instanceof TimestampNTZType) {
      long microsSinceEpochUTC = (long) value;
      long seconds = microsSinceEpochUTC / 1_000_000;
      int microsOfSecond = (int) (microsSinceEpochUTC % 1_000_000);
      if (microsOfSecond < 0) {
        // also adjust for negative microsSinceEpochUTC
        microsOfSecond = 1_000_000 + microsOfSecond;
      }
      int nanosOfSecond = microsOfSecond * 1_000;
      LocalDateTime localDateTime =
          LocalDateTime.ofEpochSecond(seconds, nanosOfSecond, ZoneOffset.UTC);
      return localDateTime.format(PARTITION_TIMESTAMP_FORMATTER);
    } else if (dataType instanceof DecimalType) {
      return ((BigDecimal) value).toString();
    } else if (dataType instanceof BinaryType) {
      return new String((byte[]) value, StandardCharsets.UTF_8);
    }
    throw new UnsupportedOperationException("Unsupported partition column type: " + dataType);
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////
  // The following string escaping code is mainly copied from Spark                             //
  // (org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils) which is copied from          //
  // Hive (o.a.h.h.common.FileUtils).                                                           //
  ////////////////////////////////////////////////////////////////////////////////////////////////
  private static final BitSet CHARS_TO_ESCAPE = new BitSet(128);

  static {
    // ASCII 01-1F are HTTP control characters that need to be escaped.
    char[] controlChars =
        new char[] {
          '\u0001', '\u0002', '\u0003', '\u0004', '\u0005', '\u0006', '\u0007', '\b', '\t', '\n',
          '\u000B', '\f', '\r', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012', '\u0013',
          '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019', '\u001A', '\u001B', '\u001C',
          '\u001D', '\u001E', '\u001F', '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\',
          '\u007F', '{', '[', ']', '^'
        };

    for (char c : controlChars) {
      CHARS_TO_ESCAPE.set(c);
    }
  }

  /**
   * Escapes the given string to be used as a partition value in the path. Basically this escapes
   *
   * <ul>
   *   <li>characters that can't be in a file path. E.g. `a\nb` will be escaped to `a%0Ab`.
   *   <li>character that are cause ambiguity in partition value parsing. E.g. For partition column
   *       `a` having value `b=c`, the path should be `a=b%3Dc`
   * </ul>
   *
   * @param value The partition value to escape.
   * @return The escaped partition value.
   */
  private static String escapePartitionValue(String value) {
    StringBuilder escaped = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c >= 0 && c < CHARS_TO_ESCAPE.size() && CHARS_TO_ESCAPE.get(c)) {
        escaped.append('%');
        escaped.append(String.format("%02X", (int) c));
      } else {
        escaped.append(c);
      }
    }
    return escaped.toString();
  }
}
