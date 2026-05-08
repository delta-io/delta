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

package io.sparkuctest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import scala.collection.JavaConverters;

/** DDL test suite for Delta Table operations through Unity Catalog. */
public class UCDeltaTableDDLTest extends UCDeltaTableIntegrationBaseTest {

  private static final String UC_TABLE_ID_KEY = "io.unitycatalog.tableId";
  private static final String SUPPORTED = "supported";
  private static final String DELTA_FEATURE_PREFIX = "delta.feature.";
  private static final List<String> EXPECTED_MANAGED_TABLE_FEATURES =
      List.of(
          "appendOnly",
          "catalogManaged",
          "deletionVectors",
          "domainMetadata",
          "inCommitTimestamp",
          "invariants",
          "rowTracking",
          "v2Checkpoint",
          "vacuumProtocolCheck");
  private static final Map<String, String> EXPECTED_MANAGED_TABLE_PROPERTIES =
      Map.of(
          "delta.checkpointPolicy", "v2",
          "delta.enableDeletionVectors", "true",
          "delta.enableInCommitTimestamps", "true",
          "delta.enableRowTracking", "true");
  private static final List<String> MUTABLE_CATALOG_PROPERTIES =
      List.of("delta.lastCommitTimestamp", "delta.lastUpdateVersion", "transient_lastDdlTime");
  private static final List<String> STABLE_DESCRIBE_DETAIL_FIELDS =
      List.of(
          "format",
          "id",
          "name",
          "description",
          "location",
          "createdAt",
          "partitionColumns",
          "clusteringColumns",
          "properties",
          "minReaderVersion",
          "minWriterVersion",
          "tableFeatures");
  private static final String DELTA_IDEMPOTENT_DML_TXN_APP_ID_KEY =
      DeltaSQLConf.DELTA_IDEMPOTENT_DML_TXN_APP_ID().key();
  private static final String DELTA_IDEMPOTENT_DML_TXN_VERSION_KEY =
      DeltaSQLConf.DELTA_IDEMPOTENT_DML_TXN_VERSION().key();

  // ---------------------------------------------------------------------------
  // TRUNCATE TABLE
  // ---------------------------------------------------------------------------

  @TestAllTableTypes
  public void testTruncateNonEmptyTable(TableType tableType) throws Exception {
    withNewTable(
        "ddl_truncate_non_empty",
        "id INT, name STRING",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')", tableName);
          long versionBeforeTruncate = currentVersion(tableName);
          Map<String, String> tableSnapshotBeforeTruncate =
              preservedTableSnapshot(tableName, tableType);

          sql("TRUNCATE TABLE %s", tableName);

          check(tableName, List.of());
          assertThat(currentVersion(tableName)).isEqualTo(versionBeforeTruncate + 1);
          assertThat(latestHistoryOperation(tableName)).isEqualTo("TRUNCATE");
          assertPreservedTableSnapshot(tableName, tableType, tableSnapshotBeforeTruncate);

          // The table should remain usable after the DDL operation.
          sql("INSERT INTO %s VALUES (4, 'delta')", tableName);
          check(tableName, List.of(row("4", "delta")));
        });
  }

  @TestAllTableTypes
  public void testTruncateEmptyTableIsNoOp(TableType tableType) throws Exception {
    withNewTable(
        "ddl_truncate_empty",
        "id INT",
        tableType,
        tableName -> {
          long versionBeforeTruncate = currentVersion(tableName);
          Map<String, String> tableSnapshotBeforeTruncate =
              preservedTableSnapshot(tableName, tableType);

          sql("TRUNCATE TABLE %s", tableName);

          check(tableName, List.of());
          assertThat(currentVersion(tableName)).isEqualTo(versionBeforeTruncate);
          assertThat(latestHistoryOperation(tableName)).isEqualTo("CREATE TABLE");
          assertPreservedTableSnapshot(tableName, tableType, tableSnapshotBeforeTruncate);
        });
  }

  @TestAllTableTypes
  public void testRepeatedTruncateIsIdempotent(TableType tableType) throws Exception {
    withNewTable(
        "ddl_truncate_repeated",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2)", tableName);
          long versionBeforeFirstTruncate = currentVersion(tableName);
          Map<String, String> tableSnapshotBeforeTruncate =
              preservedTableSnapshot(tableName, tableType);

          sql("TRUNCATE TABLE %s", tableName);
          long versionAfterFirstTruncate = currentVersion(tableName);
          assertThat(versionAfterFirstTruncate).isEqualTo(versionBeforeFirstTruncate + 1);
          check(tableName, List.of());
          assertPreservedTableSnapshot(tableName, tableType, tableSnapshotBeforeTruncate);

          sql("TRUNCATE TABLE %s", tableName);

          check(tableName, List.of());
          assertThat(currentVersion(tableName)).isEqualTo(versionAfterFirstTruncate);
          assertThat(latestHistoryOperation(tableName)).isEqualTo("TRUNCATE");
          assertPreservedTableSnapshot(tableName, tableType, tableSnapshotBeforeTruncate);
        });
  }

  @TestAllTableTypes
  public void testTruncatePartitionedTablePreservesMetadata(TableType tableType) throws Exception {
    String description = "metadata survives truncate";
    withNewTableWithComment(
        "ddl_truncate_partitioned",
        "id INT, name STRING, part INT",
        "part",
        tableType,
        "'ddlSuiteProp'='preserved'",
        description,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'alpha', 0), (2, 'beta', 1), (3, 'gamma', 1)", tableName);
          long versionBeforeTruncate = currentVersion(tableName);
          Map<String, String> propertiesBeforeTruncate = tableProperties(tableName);
          Map<String, String> tableSnapshotBeforeTruncate =
              preservedTableSnapshot(tableName, tableType);

          assertThat(tableSnapshotBeforeTruncate)
              .containsEntry("detail.partitionColumns", "[part]")
              .containsEntry("detail.description", description);
          assertThat(propertiesBeforeTruncate).containsEntry("ddlSuiteProp", "preserved");

          sql("TRUNCATE TABLE %s", tableName);

          check(tableName, List.of());
          assertThat(currentVersion(tableName)).isEqualTo(versionBeforeTruncate + 1);
          assertThat(latestHistoryOperation(tableName)).isEqualTo("TRUNCATE");
          assertThat(tableProperties(tableName)).containsEntry("ddlSuiteProp", "preserved");
          assertPreservedTableSnapshot(tableName, tableType, tableSnapshotBeforeTruncate);

          sql("INSERT INTO %s VALUES (4, 'delta', 0), (5, 'epsilon', 2)", tableName);
          check(tableName, List.of(row("4", "delta", "0"), row("5", "epsilon", "2")));
        });
  }

  @TestAllTableTypes
  public void testTruncateAppendOnlyTableFailsAtomically(TableType tableType) throws Exception {
    withNewTable(
        "ddl_truncate_append_only",
        "id INT",
        null,
        tableType,
        "'delta.appendOnly'='true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2)", tableName);
          long versionBeforeTruncate = currentVersion(tableName);
          Map<String, String> tableSnapshotBeforeTruncate =
              preservedTableSnapshot(tableName, tableType);

          assertThatThrownBy(() -> sql("TRUNCATE TABLE %s", tableName))
              .satisfies(
                  e ->
                      assertThat(causeChainMessages(e))
                          .contains("DELTA_CANNOT_MODIFY_APPEND_ONLY")
                          .contains("delta.appendOnly"));

          check(tableName, List.of(row("1"), row("2")));
          assertThat(currentVersion(tableName)).isEqualTo(versionBeforeTruncate);
          assertPreservedTableSnapshot(tableName, tableType, tableSnapshotBeforeTruncate);
        });
  }

  @TestAllTableTypes
  public void testTruncateWithPartitionSpecFailsAtomically(TableType tableType) throws Exception {
    withNewTable(
        "ddl_truncate_partition_spec",
        "id INT, part INT",
        "part",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 0), (2, 1), (3, 1)", tableName);
          long versionBeforeTruncate = currentVersion(tableName);
          Map<String, String> tableSnapshotBeforeTruncate =
              preservedTableSnapshot(tableName, tableType);

          assertThatThrownBy(() -> sql("TRUNCATE TABLE %s PARTITION (part = 1)", tableName))
              .satisfies(
                  e ->
                      assertThat(causeChainMessages(e).toLowerCase(Locale.ROOT))
                          .contains("truncate")
                          .contains("partition"));

          check(tableName, List.of(row("1", "0"), row("2", "1"), row("3", "1")));
          assertThat(currentVersion(tableName)).isEqualTo(versionBeforeTruncate);
          assertPreservedTableSnapshot(tableName, tableType, tableSnapshotBeforeTruncate);
        });
  }

  @TestAllTableTypes
  public void testTruncateWithIdempotencyTokenRecordsSetTransaction(TableType tableType)
      throws Exception {
    withNewTable(
        "ddl_truncate_idempotent_token",
        "id INT",
        tableType,
        tableName -> {
          String txnAppIdKey = DELTA_IDEMPOTENT_DML_TXN_APP_ID_KEY;
          String txnVersionKey = DELTA_IDEMPOTENT_DML_TXN_VERSION_KEY;
          String appId = "ucDdlTruncateIdempotentTokenTestApp";

          long versionBeforeEmptyTruncate = currentVersion(tableName);
          Map<String, String> snapshotBeforeEmptyTruncate =
              preservedTableSnapshot(tableName, tableType);

          spark().conf().set(txnAppIdKey, appId);
          spark().conf().set(txnVersionKey, "1");
          try {
            // Empty truncate carrying an idempotency token must still commit a SetTransaction so a
            // later replay with the same token can be deduplicated.
            sql("TRUNCATE TABLE %s", tableName);
            assertThat(currentVersion(tableName)).isEqualTo(versionBeforeEmptyTruncate + 1);
            check(tableName, List.of());
            assertPreservedTableSnapshot(tableName, tableType, snapshotBeforeEmptyTruncate);
          } finally {
            spark().conf().unset(txnAppIdKey);
            spark().conf().unset(txnVersionKey);
          }

          // Insert real data outside the idempotent path, then capture a fresh snapshot.
          sql("INSERT INTO %s VALUES (1), (2)", tableName);
          long versionAfterInsert = currentVersion(tableName);
          Map<String, String> snapshotAfterInsert = preservedTableSnapshot(tableName, tableType);

          // Replay the truncate with the same (appId, version=1) tuple. Because that token was
          // already recorded by the empty truncate above, this replay must be a no-op: data is
          // preserved AND every observed property remains unchanged.
          spark().conf().set(txnAppIdKey, appId);
          spark().conf().set(txnVersionKey, "1");
          try {
            sql("TRUNCATE TABLE %s", tableName);
            assertThat(currentVersion(tableName)).isEqualTo(versionAfterInsert);
            check(tableName, List.of(row("1"), row("2")));
            assertPreservedTableSnapshot(tableName, tableType, snapshotAfterInsert);
          } finally {
            spark().conf().unset(txnAppIdKey);
            spark().conf().unset(txnVersionKey);
          }
        });
  }

  /**
   * Creates a Delta table with an inline {@code COMMENT} clause, runs {@code testCode}, then drops
   * the table. The shared {@link #withNewTable} helper does not expose a slot for COMMENT, and
   * {@code ALTER TABLE ... SET TBLPROPERTIES} is blocked on catalog-managed tables, so the
   * description must be set at CREATE time.
   */
  private void withNewTableWithComment(
      String tableName,
      String tableSchema,
      String partitionFields,
      TableType tableType,
      String tableProperties,
      String comment,
      TestCode testCode)
      throws Exception {
    String fullName = fullTableName(tableName);
    String partitionClause =
        (partitionFields == null || partitionFields.trim().isEmpty())
            ? ""
            : String.format("PARTITIONED BY (%s)", partitionFields);
    StringBuilder propsBuilder = new StringBuilder();
    if (tableType == TableType.MANAGED) {
      propsBuilder.append("'delta.feature.catalogManaged'='supported'");
    }
    if (tableProperties != null && !tableProperties.trim().isEmpty()) {
      if (propsBuilder.length() > 0) {
        propsBuilder.append(", ");
      }
      propsBuilder.append(tableProperties);
    }
    String tblPropsClause = propsBuilder.length() > 0 ? "TBLPROPERTIES (" + propsBuilder + ")" : "";
    String commentClause = comment == null ? "" : String.format("COMMENT '%s'", comment);

    if (tableType == TableType.EXTERNAL) {
      withTempDir(
          dir -> {
            String tableLocation = dir.toString() + "/" + tableName;
            sql("DROP TABLE IF EXISTS %s", fullName);
            sql(
                "CREATE TABLE %s (%s) USING DELTA %s %s %s LOCATION '%s'",
                fullName,
                tableSchema,
                partitionClause,
                commentClause,
                tblPropsClause,
                tableLocation);
            try {
              testCode.run(fullName);
            } finally {
              sql("DROP TABLE IF EXISTS %s", fullName);
            }
          });
    } else {
      sql("DROP TABLE IF EXISTS %s", fullName);
      sql(
          "CREATE TABLE %s (%s) USING DELTA %s %s %s",
          fullName, tableSchema, partitionClause, commentClause, tblPropsClause);
      try {
        testCode.run(fullName);
      } finally {
        sql("DROP TABLE IF EXISTS %s", fullName);
      }
    }
  }

  private String latestHistoryOperation(String tableName) {
    return sql("DESCRIBE HISTORY %s LIMIT 1", tableName).get(0).get(4);
  }

  private Map<String, String> preservedTableSnapshot(String tableName, TableType tableType)
      throws Exception {
    Map<String, String> snapshot = new LinkedHashMap<>();
    String ucTableId = currentUcTableId(tableName);
    snapshot.put("ucTableId", ucTableId);
    snapshot.put("schema", spark().table(tableName).schema().treeString());

    Row detail = describeDetailRow(tableName);
    if (tableType == TableType.MANAGED) {
      assertManagedDescribeDetail(detail);
    }
    stableDescribeDetail(detail).forEach((key, value) -> snapshot.put("detail." + key, value));

    Map<String, String> properties = tableProperties(tableName);
    assertUcTableIdPropertyVisibility(tableType, properties, ucTableId);
    if (tableType == TableType.MANAGED) {
      assertManagedTablePropertyView(properties);
    }
    stableTableProperties(properties)
        .forEach((key, value) -> snapshot.put("property." + key, value));
    return snapshot;
  }

  private void assertPreservedTableSnapshot(
      String tableName, TableType tableType, Map<String, String> expected) throws Exception {
    assertThat(preservedTableSnapshot(tableName, tableType)).isEqualTo(expected);
  }

  private void assertUcTableIdPropertyVisibility(
      TableType tableType, Map<String, String> properties, String ucTableId) {
    if (tableType == TableType.MANAGED) {
      assertThat(properties).containsEntry(UC_TABLE_ID_KEY, ucTableId);
    } else {
      assertThat(properties).doesNotContainKey(UC_TABLE_ID_KEY);
    }
  }

  private void assertManagedDescribeDetail(Row row) {
    assertThat(normalizeValue(row.getAs("format"))).isEqualTo("delta");
    assertThat(normalizeValue(row.getAs("minReaderVersion"))).isEqualTo("3");
    assertThat(normalizeValue(row.getAs("minWriterVersion"))).isEqualTo("7");
    assertThat(stringList(row.getAs("tableFeatures"))).containsAll(EXPECTED_MANAGED_TABLE_FEATURES);
    Map<String, String> properties = stringMap(row.getAs("properties"));
    EXPECTED_MANAGED_TABLE_PROPERTIES.forEach(
        (key, expected) -> assertThat(properties).containsEntry(key, expected));
  }

  private void assertManagedTablePropertyView(Map<String, String> properties) {
    assertThat(properties)
        .containsEntry("delta.minReaderVersion", "3")
        .containsEntry("delta.minWriterVersion", "7");
    EXPECTED_MANAGED_TABLE_PROPERTIES.forEach(
        (key, expected) -> assertThat(properties).containsEntry(key, expected));
    EXPECTED_MANAGED_TABLE_FEATURES.forEach(
        feature -> assertThat(properties).containsEntry(DELTA_FEATURE_PREFIX + feature, SUPPORTED));
  }

  private Row describeDetailRow(String tableName) {
    return spark().sql(String.format("DESCRIBE DETAIL %s", tableName)).collectAsList().get(0);
  }

  private Map<String, String> stableDescribeDetail(Row row) {
    Map<String, String> details = new LinkedHashMap<>();
    for (String field : STABLE_DESCRIBE_DETAIL_FIELDS) {
      details.put(field, normalizeValue(row.getAs(field)));
    }
    return details;
  }

  private Map<String, String> tableProperties(String tableName) {
    Map<String, String> properties = new LinkedHashMap<>();
    for (List<String> row : sql("SHOW TBLPROPERTIES %s", tableName)) {
      if (row.size() >= 2) {
        properties.put(row.get(0), row.get(1));
      }
    }
    return properties;
  }

  private Map<String, String> stableTableProperties(Map<String, String> properties) {
    Map<String, String> stableProperties = new TreeMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (!MUTABLE_CATALOG_PROPERTIES.contains(entry.getKey())) {
        stableProperties.put(entry.getKey(), entry.getValue());
      }
    }
    return stableProperties;
  }

  @SuppressWarnings("unchecked")
  private Map<String, String> stringMap(Object value) {
    if (value instanceof scala.collection.Map) {
      return normalizeMap(
          JavaConverters.mapAsJavaMap((scala.collection.Map<Object, Object>) value));
    }
    if (value instanceof Map) {
      return normalizeMap((Map<Object, Object>) value);
    }
    throw new IllegalArgumentException("Expected a map value, got: " + value);
  }

  @SuppressWarnings("unchecked")
  private List<String> stringList(Object value) {
    if (value instanceof scala.collection.Iterable) {
      return normalizeScalaIterable((scala.collection.Iterable<Object>) value);
    }
    if (value instanceof Iterable) {
      return normalizeJavaIterable((Iterable<Object>) value);
    }
    throw new IllegalArgumentException("Expected an iterable value, got: " + value);
  }

  @SuppressWarnings("unchecked")
  private String normalizeValue(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof scala.collection.Map) {
      return normalizeMap(JavaConverters.mapAsJavaMap((scala.collection.Map<Object, Object>) value))
          .toString();
    }
    if (value instanceof Map) {
      return normalizeMap((Map<Object, Object>) value).toString();
    }
    if (value instanceof scala.collection.Iterable) {
      return normalizeScalaIterable((scala.collection.Iterable<Object>) value).toString();
    }
    if (value instanceof Iterable) {
      return normalizeJavaIterable((Iterable<Object>) value).toString();
    }
    return value.toString();
  }

  private Map<String, String> normalizeMap(Map<Object, Object> map) {
    Map<String, String> normalized = new TreeMap<>();
    for (Map.Entry<Object, Object> entry : map.entrySet()) {
      normalized.put(String.valueOf(entry.getKey()), normalizeValue(entry.getValue()));
    }
    return normalized;
  }

  private List<String> normalizeScalaIterable(scala.collection.Iterable<Object> iterable) {
    List<String> normalized = new ArrayList<>();
    scala.collection.Iterator<Object> iterator = iterable.iterator();
    while (iterator.hasNext()) {
      normalized.add(normalizeValue(iterator.next()));
    }
    return normalized;
  }

  private List<String> normalizeJavaIterable(Iterable<Object> iterable) {
    List<String> normalized = new ArrayList<>();
    for (Object value : iterable) {
      normalized.add(normalizeValue(value));
    }
    return normalized;
  }

  private String causeChainMessages(Throwable throwable) {
    StringBuilder builder = new StringBuilder();
    Throwable current = throwable;
    while (current != null) {
      if (current.getMessage() != null) {
        builder.append(current.getMessage()).append('\n');
      }
      current = current.getCause();
    }
    return builder.toString();
  }
}
