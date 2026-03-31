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
package io.delta.spark.internal.v2.ddl;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.utils.CloseableIterable;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.skipping.clustering.temp.ClusterByTransform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for the full prepare → build → commit flow.
 *
 * <p>Exercises path-based tables without requiring a live Unity Catalog server.
 */
public class CreateTableIntegrationTest {

  @Test
  public void testCreateTable_basic(@TempDir File tempDir) {
    String tablePath = new File(tempDir, "basic").getAbsolutePath();
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("name", DataTypes.StringType, true);

    DDLRequestContext request = prepareRequest(tablePath, schema, new Transform[0]);
    TransactionCommitResult result = commitCreateTable(request);

    assertEquals(0, result.getVersion());
    Snapshot snapshot = result.getPostCommitSnapshot().orElseThrow();

    // Schema roundtrip: field names, types, nullability
    StructField idField = snapshot.getSchema().at(0);
    assertEquals("id", idField.getName());
    assertInstanceOf(IntegerType.class, idField.getDataType());
    assertFalse(idField.isNullable());

    StructField nameField = snapshot.getSchema().at(1);
    assertEquals("name", nameField.getName());
    assertInstanceOf(StringType.class, nameField.getDataType());
    assertTrue(nameField.isNullable());

    assertTrue(snapshot.getPartitionColumnNames().isEmpty());
    assertTrue(
        Files.exists(Path.of(tablePath, "_delta_log", "00000000000000000000.json")),
        "Version 0 commit file should exist");
  }

  @Test
  public void testCreateTable_withPartitionsAndProperties(@TempDir File tempDir) {
    String tablePath = new File(tempDir, "partitioned").getAbsolutePath();
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("year", DataTypes.IntegerType, false)
            .add("name", DataTypes.StringType, true);
    Map<String, String> properties = new HashMap<>();
    properties.put("location", tablePath);
    properties.put("Foo", "Bar");
    properties.put("provider", "delta");
    properties.put("comment", "test comment");

    DDLRequestContext request =
        CreateTableBuilder.prepare(
            ident("parts"),
            schema,
            new Transform[] {Expressions.identity("year")},
            properties,
            new Configuration(),
            /* ucTableInfo = */ Optional.empty());

    TransactionCommitResult result = commitCreateTable(request);
    Snapshot snapshot = result.getPostCommitSnapshot().orElseThrow();

    // Partition column
    assertEquals(1, snapshot.getPartitionColumnNames().size());
    assertEquals("year", snapshot.getPartitionColumnNames().get(0));

    // User property survived, DSv2 internal keys filtered
    assertEquals("Bar", snapshot.getTableProperties().get("Foo"));
    assertNull(snapshot.getTableProperties().get("provider"));
    assertNull(snapshot.getTableProperties().get("comment"));

    // Comment preserved in DDLRequestContext for future Kernel API support
    assertEquals(Optional.of("test comment"), request.comment());
  }

  @Test
  public void testCreateTable_withClustering(@TempDir File tempDir) {
    String tablePath = new File(tempDir, "clustered").getAbsolutePath();
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("year", DataTypes.IntegerType, false)
            .add("name", DataTypes.StringType, true);
    Transform[] clusterBy = new Transform[] {clusterByTransform("year", "id")};

    DDLRequestContext request = prepareRequest(tablePath, schema, clusterBy);
    assertTrue(request.dataLayoutSpec().hasClustering());
    assertEquals("year", request.dataLayoutSpec().getClusteringColumns().get(0).getNames()[0]);
    assertEquals("id", request.dataLayoutSpec().getClusteringColumns().get(1).getNames()[0]);

    TransactionCommitResult result = commitCreateTable(request);
    Snapshot snapshot = result.getPostCommitSnapshot().orElseThrow();

    assertEquals(0, result.getVersion());
    assertEquals(3, snapshot.getSchema().length());
    assertTrue(snapshot.getPartitionColumnNames().isEmpty());
    assertTrue(Files.exists(Path.of(tablePath, "_delta_log")));
  }

  // ── helpers ────────────────────────────────────────────────────────

  private static TransactionCommitResult commitCreateTable(DDLRequestContext request) {
    Transaction txn = CreateTableBuilder.buildTransaction(request);
    return txn.commit(request.engine(), CloseableIterable.emptyIterable());
  }

  private static DDLRequestContext prepareRequest(
      String tablePath, StructType schema, Transform[] partitions) {
    Map<String, String> properties = new HashMap<>();
    properties.put("location", tablePath);
    return CreateTableBuilder.prepare(
        ident("test"),
        schema,
        partitions,
        properties,
        new Configuration(),
        /* ucTableInfo = */ Optional.empty());
  }

  private static Identifier ident(String name) {
    return Identifier.of(new String[] {"default"}, name);
  }

  private static ClusterByTransform clusterByTransform(String... colNames) {
    List<NamedReference> refs =
        java.util.Arrays.stream(colNames)
            .map(Expressions::column)
            .collect(java.util.stream.Collectors.toList());
    return new ClusterByTransform(scala.jdk.javaapi.CollectionConverters.asScala(refs).toSeq());
  }
}
