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
package io.delta.kernel.test;

import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.actions.Format;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Java counterpart of the {@code TestFixtures}, {@code ActionUtils} and {@code VectorTestUtils}
 * Scala traits, for use by JUnit 5 tests.
 *
 * <p>Those traits hold {@code val} members, so their initializers cannot run from Java and Java
 * tests cannot mix them in. This class provides the subset that Java suites need, as static members
 * rather than inherited ones.
 */
public final class KernelTestFixtures {

  private KernelTestFixtures() {}

  public static final String LOG_PATH = "/fake/_delta_log";

  public static final Protocol PROTOCOL_12 = new Protocol(1, 2);

  public static final Protocol PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT =
      new Protocol(
          TableFeatures.TABLE_FEATURES_MIN_READER_VERSION,
          TableFeatures.TABLE_FEATURES_MIN_WRITER_VERSION,
          Collections.singleton(TableFeatures.CATALOG_MANAGED_RW_FEATURE.featureName()),
          Stream.of(
                  TableFeatures.CATALOG_MANAGED_RW_FEATURE.featureName(),
                  TableFeatures.IN_COMMIT_TIMESTAMP_W_FEATURE.featureName())
              .collect(Collectors.toSet()));

  public static final Metadata BASIC_PARTITIONED_METADATA =
      testMetadata(
          new StructType().add("part1", IntegerType.INTEGER).add("col1", IntegerType.INTEGER),
          Collections.singletonList("part1"));

  /** Mirrors {@code VectorTestUtils.stringVector}. */
  public static ColumnVector stringVector(List<String> values) {
    return new ColumnVector() {
      @Override
      public DataType getDataType() {
        return StringType.STRING;
      }

      @Override
      public int getSize() {
        return values.size();
      }

      @Override
      public void close() {}

      @Override
      public boolean isNullAt(int rowId) {
        return values.get(rowId) == null;
      }

      @Override
      public String getString(int rowId) {
        return values.get(rowId);
      }
    };
  }

  /** Mirrors {@code VectorTestUtils.emptyActionsIterator}. */
  public static CloseableIterator<Row> emptyActionsIterator() {
    return new CloseableIterator<Row>() {
      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public Row next() {
        throw new NoSuchElementException("No more elements");
      }

      @Override
      public void close() {}
    };
  }

  /** Mirrors {@code VectorTestUtils.emptyColumnarBatch}. */
  public static ColumnarBatch emptyColumnarBatch() {
    return new ColumnarBatch() {
      @Override
      public StructType getSchema() {
        return null;
      }

      @Override
      public ColumnVector getColumnVector(int ordinal) {
        return null;
      }

      @Override
      public int getSize() {
        return 0;
      }
    };
  }

  /** Mirrors {@code ActionUtils.testMetadata}, with no table properties. */
  public static Metadata testMetadata(StructType schema, List<String> partitionCols) {
    return new Metadata(
        "id",
        Optional.of("name"),
        Optional.of("description"),
        new Format("parquet", Collections.emptyMap()),
        schema.toJson(),
        schema,
        new ArrayValue() {
          @Override
          public int getSize() {
            return partitionCols.size();
          }

          @Override
          public ColumnVector getElements() {
            return stringVector(partitionCols);
          }
        },
        Optional.empty(),
        new MapValue() {
          @Override
          public int getSize() {
            return 0;
          }

          @Override
          public ColumnVector getKeys() {
            return stringVector(Collections.emptyList());
          }

          @Override
          public ColumnVector getValues() {
            return stringVector(Collections.emptyList());
          }
        });
  }

  /** Mirrors {@code ActionUtils.testCommitInfo}. */
  public static CommitInfo testCommitInfo(boolean ictEnabled) {
    return new CommitInfo(
        ictEnabled ? Optional.of(1L) : Optional.empty(), // ICT
        1L, // timestamp
        Optional.of("engineInfo"),
        Optional.of("operation"),
        Collections.emptyMap(), // operationParameters
        Optional.of(false), // isBlindAppend
        Optional.of("txnId"),
        Collections.emptyMap() // operationMetrics
        );
  }

  /** Read state pairing the given protocol with {@link #BASIC_PARTITIONED_METADATA}. */
  public static Optional<Tuple2<Protocol, Metadata>> readState(Protocol protocol) {
    return readState(protocol, BASIC_PARTITIONED_METADATA);
  }

  public static Optional<Tuple2<Protocol, Metadata>> readState(
      Protocol protocol, Metadata metadata) {
    return Optional.of(new Tuple2<>(protocol, metadata));
  }

  public static CommitMetadataBuilder commitMetadata(long version) {
    return new CommitMetadataBuilder().version(version);
  }

  /**
   * Java stand-in for {@code TestFixtures.createCommitMetadata}, whose nine parameters are all
   * defaulted in Scala. Callers override only the fields they exercise.
   */
  public static final class CommitMetadataBuilder {
    private long version;
    private String logPath = LOG_PATH;
    private CommitInfo commitInfo = testCommitInfo(true);
    private List<DomainMetadata> commitDomainMetadatas = Collections.emptyList();
    private Supplier<Map<String, String>> committerProperties = Collections::emptyMap;
    private Optional<Tuple2<Protocol, Metadata>> readPandMOpt = Optional.empty();
    private Optional<Protocol> newProtocolOpt = Optional.empty();
    private Optional<Metadata> newMetadataOpt = Optional.empty();
    private Optional<Long> maxKnownPublishedDeltaVersion = Optional.empty();

    public CommitMetadataBuilder version(long version) {
      this.version = version;
      return this;
    }

    public CommitMetadataBuilder logPath(String logPath) {
      this.logPath = logPath;
      return this;
    }

    public CommitMetadataBuilder commitInfo(CommitInfo commitInfo) {
      this.commitInfo = commitInfo;
      return this;
    }

    public CommitMetadataBuilder commitDomainMetadatas(List<DomainMetadata> commitDomainMetadatas) {
      this.commitDomainMetadatas = commitDomainMetadatas;
      return this;
    }

    public CommitMetadataBuilder committerProperties(
        Supplier<Map<String, String>> committerProperties) {
      this.committerProperties = committerProperties;
      return this;
    }

    public CommitMetadataBuilder readPandMOpt(Optional<Tuple2<Protocol, Metadata>> readPandMOpt) {
      this.readPandMOpt = readPandMOpt;
      return this;
    }

    public CommitMetadataBuilder newProtocolOpt(Optional<Protocol> newProtocolOpt) {
      this.newProtocolOpt = newProtocolOpt;
      return this;
    }

    public CommitMetadataBuilder newMetadataOpt(Optional<Metadata> newMetadataOpt) {
      this.newMetadataOpt = newMetadataOpt;
      return this;
    }

    public CommitMetadataBuilder maxKnownPublishedDeltaVersion(
        Optional<Long> maxKnownPublishedDeltaVersion) {
      this.maxKnownPublishedDeltaVersion = maxKnownPublishedDeltaVersion;
      return this;
    }

    public CommitMetadata build() {
      return new CommitMetadata(
          version,
          logPath,
          commitInfo,
          commitDomainMetadatas,
          committerProperties,
          readPandMOpt,
          newProtocolOpt,
          newMetadataOpt,
          maxKnownPublishedDeltaVersion);
    }
  }
}
