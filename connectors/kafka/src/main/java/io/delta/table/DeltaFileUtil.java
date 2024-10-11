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

import static io.delta.table.DeltaTableScan.fromLambda;
import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.PartitionUtils;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.Pair;

public class DeltaFileUtil {
  private DeltaFileUtil() {}

  private static final Joiner SLASH = Joiner.on("/");
  private static final DeleteFile[] NO_DELETES = new DeleteFile[0];

  private static final Map<String, Integer> ADD_FILE_ORDINALS =
      IntStream.range(0, AddFile.FULL_SCHEMA.length())
          .boxed()
          .collect(toMap(i -> AddFile.FULL_SCHEMA.at(i).getName(), i -> i));

  private static final int PATH = ADD_FILE_ORDINALS.get("path");
  private static final int PARTITION = ADD_FILE_ORDINALS.get("partitionValues");
  private static final int FILE_SIZE = ADD_FILE_ORDINALS.get("size");
  private static final int MODIFIED_TIMESTAMP = ADD_FILE_ORDINALS.get("modificationTime");
  private static final int IS_DATA_CHANGE = ADD_FILE_ORDINALS.get("dataChange");
  private static final int DV = ADD_FILE_ORDINALS.get("deletionVector");
  private static final int TAGS = ADD_FILE_ORDINALS.get("tags");
  private static final int STATS = ADD_FILE_ORDINALS.get("stats");

  private static final Map<String, Integer> DV_ORDINALS =
      IntStream.range(0, DeletionVectorDescriptor.READ_SCHEMA.length())
          .boxed()
          .collect(toMap(i -> DeletionVectorDescriptor.READ_SCHEMA.at(i).getName(), i -> i));

  private static final int DV_TYPE = DV_ORDINALS.get("storageType");
  private static final int DV_PATH = DV_ORDINALS.get("pathOrInlineDv");
  private static final int DV_OFFSET = DV_ORDINALS.get("offset");
  private static final int DV_SIZE = DV_ORDINALS.get("sizeInBytes");
  private static final int DV_CARDINALITY = DV_ORDINALS.get("cardinality");

  //  public static RemoveFile removeFile(
  //      Schema schema, PartitionSpec spec, long timestamp, DataFile file) {
  //    return addFile(schema, spec, timestamp, file).remove(timestamp, true /* data change */);
  //  }

  public static Row addFile(
      URI tableRoot, Schema schema, PartitionSpec spec, long timestamp, DataFile file) {
    return SingleAction.createAddFileSingleAction(
        new GenericRow(
            AddFile.FULL_SCHEMA,
            ImmutableMap.of(
                PATH,
                tableRoot.relativize(URI.create(file.path().toString())).toString(),
                PARTITION,
                toPartitionMap(spec.partitionType(), file.partition()),
                FILE_SIZE,
                file.fileSizeInBytes(),
                MODIFIED_TIMESTAMP,
                timestamp,
                IS_DATA_CHANGE,
                true,
                STATS,
                toJsonStats(schema, file))));
  }

  private static MapValue toPartitionMap(Types.StructType partitionType, StructLike partition) {
    ImmutableMap.Builder<String, Literal> builder = ImmutableMap.builder();
    List<Types.NestedField> fields = partitionType.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Types.NestedField field = fields.get(i);
      builder.put(
          field.name(), DeltaExpressionUtil.literal(partition.get(i, Object.class), field.type()));
    }

    return PartitionUtils.serializePartitionMap(builder.build());
  }

  private static String toJsonStats(Schema schema, ContentFile<?> file) {
    return JsonUtil.generate(
        gen -> {
          gen.writeStartObject();
          gen.writeNumberField("numRecords", file.recordCount());
          writeCountsMap("nullCount", schema, file.nullValueCounts(), gen);
          writeCountsMap("nanCounts", schema, file.nanValueCounts(), gen);
          writeBoundsMap("minValues", schema, file.lowerBounds(), gen);
          writeBoundsMap("maxValues", schema, file.upperBounds(), gen);
          gen.writeEndObject();
        },
        false);
  }

  private static void writeCountsMap(
      String fieldName, Schema schema, Map<Integer, Long> counts, JsonGenerator gen)
      throws IOException {
    gen.writeObjectFieldStart(fieldName);
    for (Map.Entry<Integer, Long> entry : counts.entrySet()) {
      gen.writeNumberField(schema.findColumnName(entry.getKey()), entry.getValue());
    }
    gen.writeEndObject();
  }

  private static void writeBoundsMap(
      String fieldName, Schema schema, Map<Integer, ByteBuffer> bounds, JsonGenerator gen)
      throws IOException {
    gen.writeObjectFieldStart(fieldName);
    //    for (Map.Entry<Integer, ByteBuffer> entry : bounds.entrySet()) {
    //      int id = entry.getKey();
    //      Type.PrimitiveType type = schema.findType(id).asPrimitiveType();
    //      gen.writeFieldName(schema.findColumnName(id));
    //      JsonUtil.writeValue(type, Conversions.fromByteBuffer(type, entry.getValue()), gen);
    //    }
    gen.writeEndObject();
  }

  //  public static DataFile removeFile(PartitionSpec spec, RemoveFile file) {
  //    return DataFiles.builder(spec)
  //        .withPath(file.())
  //        .withPartitionPath(toPartitionString(spec, file.getPartitionValues()))
  //        .withFileSizeInBytes(file.getSize().orElse(-1L))
  //        .withRecordCount(-1L)
  //        .build();
  //  }
  //
  //  public static DataFile addFile(Schema schema, PartitionSpec spec, AddFile file) {
  //    String jsonStats = file.getStats(); // TODO: use these
  //    return DataFiles.builder(spec)
  //        .withPath(file.getPath())
  //        .withPartitionPath(toPartitionString(spec, file.getPartitionValues()))
  //        .withFileSizeInBytes(file.getSize())
  //        .withMetrics(metrics(schema, jsonStats))
  //        .build();
  //  }

  static CloseableIterable<ScanTask> asTasks(
      String schemaString,
      String specString,
      ResidualEvaluator residualEval,
      CloseableIterable<Pair<DataFile, DeleteFile>> files) {
    return CloseableIterable.transform(
        files,
        filePair -> {
          DeleteFile delete = filePair.second();
          DeleteFile[] deletes = delete != null ? new DeleteFile[] {delete} : NO_DELETES;
          return new BaseFileScanTask(
              filePair.first(), deletes, schemaString, specString, residualEval);
        });
  }

  static CloseableIterable<Pair<DataFile, DeleteFile>> files(
      String baseLocation, Schema schema, PartitionSpec spec, FilteredColumnarBatch batch) {
    return CloseableIterable.transform(
        fromLambda(batch::getRows),
        row -> {
          Row add = row.getStruct(0);

          // DVs are not currently supported
          Preconditions.checkState(add.isNullAt(DV), "Cannot convert AddFile with non-null DV");

          String path = add.getString(PATH);
          Map<String, String> partition = asStringMap(add.getMap(PARTITION));
          long fileSize = add.getLong(FILE_SIZE);
          String stats = add.getSchema().fields().size() > STATS ? add.getString(STATS) : null;

          return Pair.of(
              DataFiles.builder(spec)
                  .withPath(SLASH.join(baseLocation, path))
                  .withFileSizeInBytes(fileSize)
                  .withPartitionPath(toPartitionString(spec, partition))
                  .withMetrics(metrics(schema, stats))
                  .build(),
              null /* TODO: when a DV is supported, pass it here */);
        });
  }

  static List<String> asStringList(ArrayValue value) {
    ColumnVector asVector = value.getElements();
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    for (int i = 0; i < value.getSize(); i += 1) {
      Preconditions.checkArgument(!asVector.isNullAt(i), "Cannot convert to string: null");
      builder.add(asVector.getString(i));
    }

    return builder.build();
  }

  static Map<String, String> asStringMap(MapValue value) {
    ColumnVector keyVector = value.getKeys();
    ColumnVector valueVector = value.getValues();
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (int i = 0; i < keyVector.getSize(); i += 1) {
      Preconditions.checkArgument(!keyVector.isNullAt(i), "Invalid key: null");
      Preconditions.checkArgument(!valueVector.isNullAt(i), "Invalid value: null");
      builder.put(keyVector.getString(i), valueVector.getString(i));
    }

    return builder.build();
  }

  private static Metrics metrics(Schema schema, String jsonStats) {
    if (jsonStats != null) {
      return JsonUtil.parse(jsonStats, node -> metrics(schema, node));
    } else {
      return new Metrics(-1L, null, null, null, null);
    }
  }

  private static Metrics metrics(Schema schema, JsonNode node) {
    long rowCount = JsonUtil.getLong("numRecords", node);
    Map<Integer, ByteBuffer> lowerBounds = null;
    if (node.has("minValues")) {
      lowerBounds = toBoundMap(schema, node.get("minValues"));
    }

    Map<Integer, ByteBuffer> upperBounds = null;
    if (node.has("maxValues")) {
      upperBounds = toBoundMap(schema, node.get("maxValues"));
    }

    Map<Integer, Long> nullCounts = null;
    Map<Integer, Long> valueCounts = null;
    if (node.has("nullCount")) {
      nullCounts = Maps.newHashMap();
      valueCounts = Maps.newHashMap();
      Iterable<Map.Entry<String, JsonNode>> fields = node.get("nullCount")::fields;
      for (Map.Entry<String, JsonNode> entry : fields) {
        Object value = asJava(entry.getValue());
        Types.NestedField field = schema.findField(entry.getKey());
        if (field != null) {
          if (value != null) {
            nullCounts.put(field.fieldId(), node.longValue());
          }

          if (field.type().isPrimitiveType() && schema.asStruct().field(field.fieldId()) != null) {
            // the column is a top-level primitive so the value count is equal to the row count
            valueCounts.put(field.fieldId(), rowCount);
          }
        }
      }
    }

    return new Metrics(rowCount, null, valueCounts, nullCounts, null, lowerBounds, upperBounds);
  }

  private static Map<Integer, ByteBuffer> toBoundMap(Schema schema, JsonNode node) {
    Preconditions.checkArgument(
        node.isObject(), "Invalid type for bounds map: %s", node.getNodeType());

    ImmutableMap.Builder<Integer, ByteBuffer> bounds = ImmutableMap.builder();
    Iterable<Map.Entry<String, JsonNode>> fields = node::fields;
    for (Map.Entry<String, JsonNode> entry : fields) {
      // FIXME: this is not correct, we need to convert the value to the correct type
      //      Types.NestedField field = schema.findField(entry.getKey());
      //      if (field != null) {
      //        Object value = asJava(entry.getValue());
      //        if (value != null) {
      //          bounds.put(field.fieldId(), Literals.from(value).to(field.type()).toByteBuffer());
      //        }
      //      }
    }

    return bounds.build();
  }

  private static Object asJava(JsonNode node) {
    if (node.isInt()) {
      return node.intValue();
    } else if (node.isLong()) {
      return node.longValue();
    } else if (node.isTextual()) {
      return node.textValue();
    } else if (node.isFloat()) {
      return node.floatValue();
    } else if (node.isDouble()) {
      return node.doubleValue();
    } else if (node.isBoolean()) {
      return node.booleanValue();
    }

    return null;
  }

  private static String toPartitionString(PartitionSpec spec, Map<String, String> values) {
    List<String> partitions = Lists.newArrayList();
    for (PartitionField field : spec.fields()) {
      partitions.add(field.name() + "=" + values.get(field.name()));
    }

    return SLASH.join(partitions);
  }
}
