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
package io.delta.kernel.internal.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.commit.CommitMetadata.CommitType;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CommitMetadataTest {

  private static final Protocol PROTOCOL_12 = new Protocol(1, 2);
  private static final String LOG_PATH = "/fake/_delta_log";
  private static final long CREATE_VERSION_0 = 0L;
  private static final long UPDATE_VERSION_NON_ZERO = 1L;

  private static final Protocol PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT =
      new Protocol(
          TableFeatures.TABLE_FEATURES_MIN_READER_VERSION,
          TableFeatures.TABLE_FEATURES_MIN_WRITER_VERSION,
          Collections.singleton(TableFeatures.CATALOG_MANAGED_RW_FEATURE.featureName()),
          Stream.of(
                  TableFeatures.CATALOG_MANAGED_RW_FEATURE.featureName(),
                  TableFeatures.IN_COMMIT_TIMESTAMP_W_FEATURE.featureName())
              .collect(Collectors.toSet()));

  private static final Metadata BASIC_PARTITIONED_METADATA =
      testMetadata(
          new StructType().add("part1", IntegerType.INTEGER).add("col1", IntegerType.INTEGER),
          Collections.singletonList("part1"));

  // ========== Fixtures ==========
  //
  // The kernel-api test fixtures (TestFixtures, ActionUtils, VectorTestUtils) are Scala traits
  // carrying vals, so their initializers cannot run from Java. The handful this suite needs are
  // rebuilt below; extract them to a shared Java fixture once a second Java suite wants them.

  /** Mirrors VectorTestUtils.stringVector. */
  private static ColumnVector stringVector(List<String> values) {
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

  /** Mirrors ActionUtils.testMetadata. */
  private static Metadata testMetadata(StructType schema, List<String> partitionCols) {
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

  /** Mirrors ActionUtils.testCommitInfo. */
  private static CommitInfo testCommitInfo(boolean ictEnabled) {
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

  /**
   * Java stand-in for TestFixtures.createCommitMetadata, whose nine parameters are all defaulted in
   * Scala. Each test overrides only the fields it exercises.
   */
  private static final class CommitMetadataBuilder {
    private long version;
    private String logPath = LOG_PATH;
    private CommitInfo commitInfo = testCommitInfo(true);
    private List<DomainMetadata> commitDomainMetadatas = Collections.emptyList();
    private Supplier<Map<String, String>> committerProperties = Collections::emptyMap;
    private Optional<Tuple2<Protocol, Metadata>> readPandMOpt = Optional.empty();
    private Optional<Protocol> newProtocolOpt = Optional.empty();
    private Optional<Metadata> newMetadataOpt = Optional.empty();
    private Optional<Long> maxKnownPublishedDeltaVersion = Optional.empty();

    CommitMetadataBuilder version(long version) {
      this.version = version;
      return this;
    }

    CommitMetadataBuilder logPath(String logPath) {
      this.logPath = logPath;
      return this;
    }

    CommitMetadataBuilder commitInfo(CommitInfo commitInfo) {
      this.commitInfo = commitInfo;
      return this;
    }

    CommitMetadataBuilder commitDomainMetadatas(List<DomainMetadata> commitDomainMetadatas) {
      this.commitDomainMetadatas = commitDomainMetadatas;
      return this;
    }

    CommitMetadataBuilder committerProperties(Supplier<Map<String, String>> committerProperties) {
      this.committerProperties = committerProperties;
      return this;
    }

    CommitMetadataBuilder readPandMOpt(Optional<Tuple2<Protocol, Metadata>> readPandMOpt) {
      this.readPandMOpt = readPandMOpt;
      return this;
    }

    CommitMetadataBuilder newProtocolOpt(Optional<Protocol> newProtocolOpt) {
      this.newProtocolOpt = newProtocolOpt;
      return this;
    }

    CommitMetadataBuilder newMetadataOpt(Optional<Metadata> newMetadataOpt) {
      this.newMetadataOpt = newMetadataOpt;
      return this;
    }

    CommitMetadataBuilder maxKnownPublishedDeltaVersion(
        Optional<Long> maxKnownPublishedDeltaVersion) {
      this.maxKnownPublishedDeltaVersion = maxKnownPublishedDeltaVersion;
      return this;
    }

    CommitMetadata build() {
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

  private static CommitMetadataBuilder commitMetadata(long version) {
    return new CommitMetadataBuilder().version(version);
  }

  private static Optional<Tuple2<Protocol, Metadata>> readState(Protocol protocol) {
    return Optional.of(new Tuple2<>(protocol, BASIC_PARTITIONED_METADATA));
  }

  // ========== Tests ==========

  @Test
  void constructorValidatesNonNegativeVersion() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> commitMetadata(-1L).build());
    assertTrue(ex.getMessage().contains("version must be non-negative"));
  }

  @Test
  void constructorRejectsNullLogPath() {
    assertThrows(
        NullPointerException.class,
        () ->
            commitMetadata(UPDATE_VERSION_NON_ZERO)
                .logPath(null)
                .readPandMOpt(readState(PROTOCOL_12))
                .build());
  }

  @Test
  void constructorRejectsNullCommitInfo() {
    assertThrows(
        NullPointerException.class,
        () ->
            commitMetadata(UPDATE_VERSION_NON_ZERO)
                .commitInfo(null)
                .readPandMOpt(readState(PROTOCOL_12))
                .build());
  }

  @Test
  void constructorRejectsNullDomainMetadatas() {
    assertThrows(
        NullPointerException.class,
        () ->
            commitMetadata(CREATE_VERSION_0)
                .commitDomainMetadatas(null)
                .newProtocolOpt(Optional.of(PROTOCOL_12))
                .newMetadataOpt(Optional.of(BASIC_PARTITIONED_METADATA))
                .build());
  }

  @Test
  void constructorRejectsNullCommitterProperties() {
    assertThrows(
        NullPointerException.class,
        () ->
            commitMetadata(UPDATE_VERSION_NON_ZERO)
                .readPandMOpt(readState(PROTOCOL_12))
                .committerProperties(null)
                .build());
  }

  @Test
  void constructorRejectsNullMaxKnownPublishedDeltaVersion() {
    assertThrows(
        NullPointerException.class,
        () ->
            commitMetadata(UPDATE_VERSION_NON_ZERO)
                .readPandMOpt(readState(PROTOCOL_12))
                .maxKnownPublishedDeltaVersion(null)
                .build());
  }

  @Test
  void constructorValidatesReadProtocolAndReadMetadataConsistency() {
    // Both present is valid
    commitMetadata(UPDATE_VERSION_NON_ZERO).readPandMOpt(readState(PROTOCOL_12)).build();

    // Both absent is valid if new ones are present
    commitMetadata(CREATE_VERSION_0)
        .newProtocolOpt(Optional.of(PROTOCOL_12))
        .newMetadataOpt(Optional.of(BASIC_PARTITIONED_METADATA))
        .build();
  }

  @Test
  void constructorValidatesAtLeastOneProtocolMustBePresent() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            commitMetadata(CREATE_VERSION_0)
                .newMetadataOpt(Optional.of(BASIC_PARTITIONED_METADATA))
                .build());
  }

  @Test
  void constructorValidatesAtLeastOneMetadataMustBePresent() {
    assertThrows(
        IllegalArgumentException.class,
        () -> commitMetadata(CREATE_VERSION_0).newProtocolOpt(Optional.of(PROTOCOL_12)).build());
  }

  @Test
  void constructorValidatesIctPresentIfCatalogManagedEnabled() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                commitMetadata(CREATE_VERSION_0)
                    .commitInfo(testCommitInfo(false))
                    .newProtocolOpt(Optional.of(PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT))
                    .newMetadataOpt(Optional.of(BASIC_PARTITIONED_METADATA))
                    .build());

    assertTrue(
        ex.getMessage()
            .contains("InCommitTimestamp must be present for commits to catalogManaged tables"));
  }

  @Test
  void getNewDomainMetadatasReturnsProvidedDomainMetadata() {
    DomainMetadata domainMetadata1 = new DomainMetadata("domain1", "{\"key\":\"value\"}", false);
    DomainMetadata domainMetadata2 = new DomainMetadata("domain2", "", false);

    CommitMetadata commitMetadata =
        commitMetadata(CREATE_VERSION_0)
            .commitDomainMetadatas(Arrays.asList(domainMetadata1, domainMetadata2))
            .newProtocolOpt(Optional.of(PROTOCOL_12))
            .newMetadataOpt(Optional.of(BASIC_PARTITIONED_METADATA))
            .build();

    List<DomainMetadata> returnedMetadatas = commitMetadata.getCommitDomainMetadatas();
    assertEquals(2, returnedMetadatas.size());
    assertTrue(returnedMetadatas.contains(domainMetadata1));
    assertTrue(returnedMetadatas.contains(domainMetadata2));
  }

  @Test
  void getCommitterPropertiesReturnsProvidedSupplier() {
    Map<String, String> props = new HashMap<>();
    props.put("key1", "value1");
    props.put("key2", "value2");

    CommitMetadata commitMetadata =
        commitMetadata(UPDATE_VERSION_NON_ZERO)
            .readPandMOpt(readState(PROTOCOL_12))
            .committerProperties(() -> props)
            .build();

    assertEquals(props, commitMetadata.getCommitterProperties().get());
  }

  @Test
  void getEffectiveProtocolReturnsNewProtocolWhenPresent() {
    Protocol newProtocol = new Protocol(2, 3);
    CommitMetadata commitMetadata =
        commitMetadata(UPDATE_VERSION_NON_ZERO)
            .readPandMOpt(readState(PROTOCOL_12))
            .newProtocolOpt(Optional.of(newProtocol))
            .build();

    assertSame(newProtocol, commitMetadata.getEffectiveProtocol());
  }

  @Test
  void getEffectiveProtocolReturnsReadProtocolWhenNewProtocolAbsent() {
    CommitMetadata commitMetadata =
        commitMetadata(UPDATE_VERSION_NON_ZERO).readPandMOpt(readState(PROTOCOL_12)).build();

    assertSame(PROTOCOL_12, commitMetadata.getEffectiveProtocol());
  }

  @Test
  void getEffectiveMetadataReturnsNewMetadataWhenPresent() {
    Metadata newMetadata =
        testMetadata(new StructType().add("newCol", IntegerType.INTEGER), Collections.emptyList());
    CommitMetadata commitMetadata =
        commitMetadata(UPDATE_VERSION_NON_ZERO)
            .readPandMOpt(readState(PROTOCOL_12))
            .newMetadataOpt(Optional.of(newMetadata))
            .build();

    assertSame(newMetadata, commitMetadata.getEffectiveMetadata());
  }

  @Test
  void getEffectiveMetadataReturnsReadMetadataWhenNewMetadataAbsent() {
    CommitMetadata commitMetadata =
        commitMetadata(UPDATE_VERSION_NON_ZERO).readPandMOpt(readState(PROTOCOL_12)).build();

    assertSame(BASIC_PARTITIONED_METADATA, commitMetadata.getEffectiveMetadata());
  }

  // ========== CommitType Tests START ==========

  private static Stream<Arguments> commitTypeCases() {
    return Stream.of(
        Arguments.of(
            // No read state for table creation
            Optional.empty(),
            Optional.of(PROTOCOL_12),
            Optional.of(BASIC_PARTITIONED_METADATA),
            CommitType.FILESYSTEM_CREATE),
        Arguments.of(
            // No read state for table creation
            Optional.empty(),
            Optional.of(PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT),
            Optional.of(BASIC_PARTITIONED_METADATA),
            CommitType.CATALOG_CREATE),
        Arguments.of(
            readState(PROTOCOL_12),
            Optional.empty(),
            Optional.empty(),
            CommitType.FILESYSTEM_WRITE),
        Arguments.of(
            readState(PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT),
            Optional.empty(),
            Optional.empty(),
            CommitType.CATALOG_WRITE),
        Arguments.of(
            readState(PROTOCOL_12),
            Optional.of(PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT),
            Optional.empty(),
            CommitType.FILESYSTEM_UPGRADE_TO_CATALOG),
        Arguments.of(
            readState(PROTOCOL_WITH_CATALOG_MANAGED_SUPPORT),
            Optional.of(PROTOCOL_12),
            Optional.empty(),
            CommitType.CATALOG_DOWNGRADE_TO_FILESYSTEM));
  }

  @ParameterizedTest
  @MethodSource("commitTypeCases")
  void getCommitTypeReturnsExpectedType(
      Optional<Tuple2<Protocol, Metadata>> readPandMOpt,
      Optional<Protocol> newProtocolOpt,
      Optional<Metadata> newMetadataOpt,
      CommitType expectedCommitType) {
    // version > 0 for writes, version 0 for create
    long version = readPandMOpt.isPresent() ? 1L : 0L;

    CommitMetadata commitMetadata =
        commitMetadata(version)
            .logPath(LOG_PATH)
            .readPandMOpt(readPandMOpt)
            .newProtocolOpt(newProtocolOpt)
            .newMetadataOpt(newMetadataOpt)
            .build();

    assertEquals(expectedCommitType, commitMetadata.getCommitType());
  }

  // ========== CommitType Tests END ==========

  @Test
  void version0WithAbsentReadStateShouldPass() {
    commitMetadata(CREATE_VERSION_0)
        .newProtocolOpt(Optional.of(PROTOCOL_12))
        .newMetadataOpt(Optional.of(BASIC_PARTITIONED_METADATA))
        .build();
  }

  @Test
  void version0WithPresentReadStateShouldFail() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> commitMetadata(CREATE_VERSION_0).readPandMOpt(readState(PROTOCOL_12)).build());
    assertTrue(ex.getMessage().contains("Table creation (version 0) requires absent readPandMOpt"));
  }

  @Test
  void versionAboveZeroWithPresentReadStateShouldPass() {
    commitMetadata(UPDATE_VERSION_NON_ZERO).readPandMOpt(readState(PROTOCOL_12)).build();
  }

  @Test
  void versionAboveZeroWithAbsentReadStateShouldFail() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                commitMetadata(UPDATE_VERSION_NON_ZERO)
                    .newProtocolOpt(Optional.of(PROTOCOL_12))
                    .newMetadataOpt(Optional.of(BASIC_PARTITIONED_METADATA))
                    .build());
    assertTrue(
        ex.getMessage()
            .contains("existing table writes (version > 0) require present readPandMOpt"));
  }
}
