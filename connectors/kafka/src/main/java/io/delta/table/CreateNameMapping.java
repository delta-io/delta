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
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

class CreateNameMapping extends DeltaTypeVisitor<MappedFields> {
  private static final String FIELD_ID_KEY = "delta.columnMapping.id";
  private static final String PHYSICAL_NAME_KEY = "delta.columnMapping.physicalName";
  private static final Joiner DOT = Joiner.on(".");

  private final Deque<String> fieldNames =
      org.apache.iceberg.relocated.com.google.common.collect.Lists.newLinkedList();
  private final NameMapping existingMapping;
  private final AtomicInteger nextId;

  int lastAssignedId() {
    return nextId.get();
  }

  private int findOrAssignId(String name, FieldMetadata metadata) {
    Object deltaId = metadata.get(FIELD_ID_KEY);
    if (deltaId instanceof Number) {
      return ((Number) deltaId).intValue();
    } else {
      return findOrAssignId(name);
    }
  }

  private int findOrAssignId(String name) {
    if (existingMapping != null) {
      MappedField mapping = existingMapping.find(fieldNames, name);
      if (mapping != null) {
        return mapping.id();
      }
    }

    return nextId.incrementAndGet();
  }

  CreateNameMapping(NameMapping existingMapping, int lastAssignedId) {
    this.existingMapping = existingMapping;
    this.nextId = new AtomicInteger(lastAssignedId);
  }

  CreateNameMapping(int lastAssignedId) {
    this.existingMapping = null;
    this.nextId = new AtomicInteger(lastAssignedId);
  }

  @Override
  public MappedFields struct(StructType struct, List<MappedFields> fieldResults) {
    List<StructField> fields = struct.fields();
    List<MappedField> mappings = Lists.newArrayList();
    for (int i = 0; i < fieldResults.size(); i += 1) {
      StructField field = fields.get(i);
      String name = field.getName();
      FieldMetadata metadata = field.getMetadata();

      int id = findOrAssignId(name, metadata);
      Object physicalName = metadata.get(PHYSICAL_NAME_KEY);
      if (physicalName != null) {
        mappings.add(
            MappedField.of(
                id, ImmutableList.of(name, physicalName.toString()), fieldResults.get(i)));
      } else {
        mappings.add(MappedField.of(id, name, fieldResults.get(i)));
      }
    }

    return MappedFields.of(mappings);
  }

  @Override
  public MappedFields field(StructField field, MappedFields fieldResult) {
    return fieldResult;
  }

  @Override
  public MappedFields array(ArrayType array, MappedFields elementResult) {
    int elementId = findOrAssignId("element");
    return MappedFields.of(MappedField.of(elementId, "element", elementResult));
  }

  @Override
  public MappedFields map(MapType map, MappedFields keyResult, MappedFields valueResult) {
    int keyId = findOrAssignId("key");
    int valueId = findOrAssignId("value");
    return MappedFields.of(
        MappedField.of(keyId, "key", keyResult), MappedField.of(valueId, "value", valueResult));
  }

  @Override
  public MappedFields atomic(DataType atomic) {
    return null; // primitives have no nested fields
  }

  private String fieldName(String name) {
    return DOT.join(Iterables.concat(fieldNames, ImmutableList.of(name)));
  }

  @Override
  public void beforeField(StructField field) {
    fieldNames.addLast(field.getName());
  }

  @Override
  public void afterField(StructField field) {
    fieldNames.removeLast();
  }
}
