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

import static io.delta.kernel.internal.util.VectorUtils.stringArrayValue;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.*;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractProtocol;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.TableFeatures;
import io.delta.kernel.internal.TableFeatures.TableFeatureType;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.*;

public class Protocol implements AbstractProtocol {

  ///////////////////
  // Static Fields //
  ///////////////////

  public static final StructType FULL_SCHEMA =
      new StructType()
          .add("minReaderVersion", IntegerType.INTEGER, false /* nullable */)
          .add("minWriterVersion", IntegerType.INTEGER, false /* nullable */)
          .add("readerFeatures", new ArrayType(StringType.STRING, false /* contains null */))
          .add("writerFeatures", new ArrayType(StringType.STRING, false /* contains null */));

  ////////////////////
  // Static Methods //
  ////////////////////

  public static Protocol fromColumnVector(ColumnVector vector, int rowId) {
    if (vector.isNullAt(rowId)) {
      return null;
    }

    return new Protocol(
        vector.getChild(0).getInt(rowId),
        vector.getChild(1).getInt(rowId),
        vector.getChild(2).isNullAt(rowId)
            ? Collections.emptySet()
            : new HashSet<>(VectorUtils.toJavaList(vector.getChild(2).getArray(rowId))),
        vector.getChild(3).isNullAt(rowId)
            ? Collections.emptySet()
            : new HashSet<>(VectorUtils.toJavaList(vector.getChild(3).getArray(rowId))));
  }

  /////////////////////////////
  // Member Fields / Methods //
  /////////////////////////////

  private final int minReaderVersion;
  private final int minWriterVersion;
  private final Set<String> readerFeatures;
  private final Set<String> writerFeatures;

  public Protocol(
      int minReaderVersion,
      int minWriterVersion,
      Set<String> readerFeatures,
      Set<String> writerFeatures) {
    this.minReaderVersion = minReaderVersion;
    this.minWriterVersion = minWriterVersion;
    this.readerFeatures =
        Collections.unmodifiableSet(requireNonNull(readerFeatures, "readerFeatures is null"));
    this.writerFeatures =
        Collections.unmodifiableSet(requireNonNull(writerFeatures, "writerFeatures is null"));

    final boolean supportsReaderFeatures =
        minReaderVersion >= TableFeatures.TABLE_FEATURES_MIN_READER_VERSION;
    final boolean supportsWriterFeatures =
        minWriterVersion >= TableFeatures.TABLE_FEATURES_MIN_WRITER_VERSION;

    if (!supportsReaderFeatures && !readerFeatures.isEmpty()) {
      throw DeltaErrors.mismatchedProtocolVersionFeatureSet(
          TableFeatureType.READER,
          minReaderVersion /* tableFeatureVersion */,
          TableFeatures.TABLE_FEATURES_MIN_READER_VERSION /* minRequiredVersion */,
          readerFeatures /* tableFeatures */);
    }

    if (!supportsWriterFeatures && !writerFeatures.isEmpty()) {
      throw DeltaErrors.mismatchedProtocolVersionFeatureSet(
          TableFeatureType.WRITER,
          minWriterVersion /* tableFeatureVersion */,
          TableFeatures.TABLE_FEATURES_MIN_WRITER_VERSION /* minRequiredVersion */,
          writerFeatures /* tableFeatures */);
    }

    if (supportsReaderFeatures && !supportsWriterFeatures) {
      throw DeltaErrors.tableFeatureReadRequiresWrite(minReaderVersion, minWriterVersion);
    }
  }

  public int getMinReaderVersion() {
    return minReaderVersion;
  }

  public int getMinWriterVersion() {
    return minWriterVersion;
  }

  public Set<String> getReaderFeatures() {
    return readerFeatures;
  }

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
    Map<Integer, Object> protocolMap = new HashMap<>();
    protocolMap.put(0, minReaderVersion);
    protocolMap.put(1, minWriterVersion);

    // Note that we only write readerFeatures when the minReaderVersion is at least
    // TableFeatures.TABLE_FEATURES_MIN_READER_VERSION, else we write null.
    protocolMap.put(
        2,
        minReaderVersion < TableFeatures.TABLE_FEATURES_MIN_READER_VERSION
            ? null
            : stringArrayValue(new ArrayList<>(readerFeatures)));

    // Note that we only write writerFeatures when the minWriterVersion is at least
    // TableFeatures.TABLE_FEATURES_MIN_WRITER_VERSION, else we write null.
    protocolMap.put(
        3,
        minWriterVersion < TableFeatures.TABLE_FEATURES_MIN_WRITER_VERSION
            ? null
            : stringArrayValue(new ArrayList<>(writerFeatures)));

    return new GenericRow(Protocol.FULL_SCHEMA, protocolMap);
  }

  public Protocol withAdditionalWriterFeatures(Set<String> additionalWriterFeatures) {
    requireNonNull(additionalWriterFeatures, "additionalWriterFeatures is null");

    Tuple2<Integer, Integer> protocolVersions =
        TableFeatures.minProtocolVersionFromAutomaticallyEnabledFeatures(additionalWriterFeatures);

    Set<String> combinedWriterFeatures = new HashSet<>(additionalWriterFeatures);

    if (!writerFeatures.isEmpty()) {
      combinedWriterFeatures.addAll(writerFeatures);
    }

    return new Protocol(
        protocolVersions._1,
        protocolVersions._2,
        readerFeatures.isEmpty() ? Collections.emptySet() : new HashSet<>(this.readerFeatures),
        combinedWriterFeatures);
  }
}
