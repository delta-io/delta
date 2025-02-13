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
package io.delta.kernel.internal.actions;

import static io.delta.kernel.internal.TableFeatures.TABLE_FEATURES_MIN_READER_VERSION;
import static io.delta.kernel.internal.TableFeatures.TABLE_FEATURES_MIN_WRITER_VERSION;
import static io.delta.kernel.internal.util.VectorUtils.stringArrayValue;

import io.delta.kernel.actions.AbstractProtocol;
import io.delta.kernel.data.*;
import io.delta.kernel.internal.TableFeatures;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.InternalUtils;
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.*;

public class Protocol implements AbstractProtocol {

  //////////////////////////////////
  // Static variables and methods //
  //////////////////////////////////

  /**
   * This method should always be used after a check to ensure the column vector is not null at the
   * given row ID.
   *
   * @throws IllegalArgumentException if the column vector is null at the given row ID
   */
  public static Protocol fromColumnVector(ColumnVector vector, int rowId) {
    InternalUtils.requireNonNull(vector, rowId, "protocol");

    return new Protocol(
        vector.getChild(0).getInt(rowId),
        vector.getChild(1).getInt(rowId),
        vector.getChild(2).isNullAt(rowId)
            ? null
            : new HashSet<>(VectorUtils.toJavaList(vector.getChild(2).getArray(rowId))),
        vector.getChild(3).isNullAt(rowId)
            ? null
            : new HashSet<>(VectorUtils.toJavaList(vector.getChild(3).getArray(rowId))));
  }

  public static final StructType FULL_SCHEMA =
      new StructType()
          .add("minReaderVersion", IntegerType.INTEGER, false /* nullable */)
          .add("minWriterVersion", IntegerType.INTEGER, false /* nullable */)
          .add("readerFeatures", new ArrayType(StringType.STRING, false /* contains null */))
          .add("writerFeatures", new ArrayType(StringType.STRING, false /* contains null */));

  //////////////////////////////////
  // Member variables and methods //
  //////////////////////////////////

  private final int minReaderVersion;
  private final int minWriterVersion;
  private final Set<String> readerFeatures;
  private final Set<String> writerFeatures;

  public Protocol(
      int minReaderVersion,
      int minWriterVersion,
      Set<String> nullableReaderFeatures,
      Set<String> nullableWriterFeatures) {
    Preconditions.checkArgument(minReaderVersion >= 1, "minReaderVersion must be >= 1");
    Preconditions.checkArgument(minWriterVersion >= 1, "minWriterVersion must be >= 1");

    this.minReaderVersion = minReaderVersion;
    this.minWriterVersion = minWriterVersion;
    this.readerFeatures =
        nullableReaderFeatures == null
            ? Collections.emptySet()
            : Collections.unmodifiableSet(nullableReaderFeatures);
    this.writerFeatures =
        nullableWriterFeatures == null
            ? Collections.emptySet()
            : Collections.unmodifiableSet(nullableWriterFeatures);

    final boolean supportsReaderFeatures = minReaderVersion >= TABLE_FEATURES_MIN_READER_VERSION;
    final boolean supportsWriterFeatures = minWriterVersion >= TABLE_FEATURES_MIN_WRITER_VERSION;

    if (!supportsReaderFeatures && !readerFeatures.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "This protocol has minReaderVersion %d but readerFeatures is not empty: %s. "
                  + "readerFeatures are only supported with minReaderVersion >= %d.",
              minReaderVersion, readerFeatures, TABLE_FEATURES_MIN_READER_VERSION));
    }

    if (!supportsWriterFeatures && !writerFeatures.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "This protocol has minWriterVersion %d but writerFeatures is not empty: %s. "
                  + "writerFeatures are only supported with minWriterVersion >= %d.",
              minWriterVersion, writerFeatures, TABLE_FEATURES_MIN_WRITER_VERSION));
    }

    if (supportsReaderFeatures && !supportsWriterFeatures) {
      throw new IllegalArgumentException(
          String.format(
              "This protocol has minReaderVersion %d but minWriterVersion %d. "
                  + "When minReaderVersion is >= %d, minWriterVersion must be >= %d.",
              minReaderVersion,
              minWriterVersion,
              TABLE_FEATURES_MIN_READER_VERSION,
              TABLE_FEATURES_MIN_WRITER_VERSION));
    }
  }

  @Override
  public int getMinReaderVersion() {
    return minReaderVersion;
  }

  @Override
  public int getMinWriterVersion() {
    return minWriterVersion;
  }

  @Override
  public Set<String> getReaderFeatures() {
    return readerFeatures;
  }

  @Override
  public Set<String> getWriterFeatures() {
    return writerFeatures;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Protocol{");
    sb.append("minReaderVersion=").append(minReaderVersion);
    sb.append(", minWriterVersion=").append(minWriterVersion);
    sb.append(", readerFeatures=").append(readerFeatures);
    sb.append(", writerFeatures=").append(writerFeatures);
    sb.append('}');
    return sb.toString();
  }

  /**
   * Encode as a {@link Row} object with the schema {@link Protocol#FULL_SCHEMA}.
   *
   * @return {@link Row} object with the schema {@link Protocol#FULL_SCHEMA}
   */
  public Row toRow() {
    final Map<Integer, Object> protocolMap = new HashMap<>();
    protocolMap.put(0, minReaderVersion);
    protocolMap.put(1, minWriterVersion);

    // readerFeatures can only exist in the serialized protocol action when minReaderVersion >= 3
    protocolMap.put(
        2,
        minReaderVersion >= TABLE_FEATURES_MIN_READER_VERSION
            ? stringArrayValue(new ArrayList<>(readerFeatures))
            : null);

    // writerFeatures can only exist in the serialized protocol action when minWriterVersion >= 7
    protocolMap.put(
        3,
        minWriterVersion >= TABLE_FEATURES_MIN_WRITER_VERSION
            ? stringArrayValue(new ArrayList<>(writerFeatures))
            : null);

    return new GenericRow(Protocol.FULL_SCHEMA, protocolMap);
  }

  public Protocol withNewWriterFeatures(Set<String> newWriterFeatures) {
    final Tuple2<Integer, Integer> newProtocolVersions =
        TableFeatures.minProtocolVersionFromAutomaticallyEnabledFeatures(newWriterFeatures);
    final Set<String> allNewWriterFeatures = new HashSet<>();
    allNewWriterFeatures.addAll(writerFeatures);
    allNewWriterFeatures.addAll(newWriterFeatures);

    return new Protocol(
        newProtocolVersions._1, newProtocolVersions._2, readerFeatures, allNewWriterFeatures);
  }
}
