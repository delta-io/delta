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

package io.delta.flink.kernel;

import static io.delta.flink.kernel.Checkpoint.CHECKPOINT_SCHEMA;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.checkpoints.*;
import io.delta.kernel.types.StructType;
import java.math.BigDecimal;
import java.util.List;
import java.util.function.Function;

/** Represent a row in a v2 checkpoint */
public class CheckpointActionRow implements Row {

  static final List<Function<Object, Row>> ROW_MAPPERS =
      List.of(
          (obj) -> ((CheckpointMetadataAction) obj).toRow(),
          (obj) -> ((Metadata) obj).toRow(),
          (obj) -> ((Protocol) obj).toRow(),
          (obj) -> ((SetTransaction) obj).toRow(),
          (obj) -> ((SidecarFile) obj).toRow());

  private final Object action;

  public CheckpointActionRow(Object action) {
    this.action = action;
  }

  @Override
  public StructType getSchema() {
    return CHECKPOINT_SCHEMA;
  }

  @Override
  public boolean isNullAt(int ordinal) {
    try {
      ROW_MAPPERS.get(ordinal).apply(action);
      return false;
    } catch (ClassCastException e) {
      return true;
    }
  }

  @Override
  public Row getStruct(int ordinal) {
    try {
      return ROW_MAPPERS.get(ordinal).apply(action);
    } catch (ClassCastException e) {
      return null;
    }
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return false;
  }

  @Override
  public byte getByte(int ordinal) {
    return 0;
  }

  @Override
  public short getShort(int ordinal) {
    return 0;
  }

  @Override
  public int getInt(int ordinal) {
    return 0;
  }

  @Override
  public long getLong(int ordinal) {
    return 0;
  }

  @Override
  public float getFloat(int ordinal) {
    return 0;
  }

  @Override
  public double getDouble(int ordinal) {
    return 0;
  }

  @Override
  public String getString(int ordinal) {
    return "";
  }

  @Override
  public BigDecimal getDecimal(int ordinal) {
    return null;
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return new byte[0];
  }

  @Override
  public ArrayValue getArray(int ordinal) {
    return null;
  }

  @Override
  public MapValue getMap(int ordinal) {
    return null;
  }
}
