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
import io.delta.kernel.utils.CloseableIterable;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for the full prepare → build → commit → publish flow.
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

    DDLRequest request = prepareRequest(tablePath, schema, new Transform[0]);

    Transaction txn = CreateTableBuilder.buildTransaction(request);
    TransactionCommitResult result =
        txn.commit(request.engine(), CloseableIterable.emptyIterable());

    assertEquals(0, result.getVersion());
    Snapshot snapshot = result.getPostCommitSnapshot().orElseThrow();
    assertEquals(2, snapshot.getSchema().length());
    assertEquals("id", snapshot.getSchema().at(0).getName());
    assertEquals("name", snapshot.getSchema().at(1).getName());
    assertTrue(snapshot.getPartitionColumnNames().isEmpty());
  }

  @Test
  public void testCreateTable_withPartitions(@TempDir File tempDir) {
    String tablePath = new File(tempDir, "partitioned").getAbsolutePath();
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("year", DataTypes.IntegerType, false)
            .add("name", DataTypes.StringType, true);
    Transform[] partitions = new Transform[] {Expressions.identity("year")};

    DDLRequest request = prepareRequest(tablePath, schema, partitions);
    Transaction txn = CreateTableBuilder.buildTransaction(request);
    TransactionCommitResult result =
        txn.commit(request.engine(), CloseableIterable.emptyIterable());

    Snapshot snapshot = result.getPostCommitSnapshot().orElseThrow();
    assertEquals(1, snapshot.getPartitionColumnNames().size());
    assertEquals("year", snapshot.getPartitionColumnNames().get(0));
  }

  @Test
  public void testCreateTable_withUserProperties(@TempDir File tempDir) {
    String tablePath = new File(tempDir, "props").getAbsolutePath();
    StructType schema = new StructType().add("id", DataTypes.IntegerType, false);
    Map<String, String> properties = new HashMap<>();
    properties.put("location", tablePath);
    properties.put("Foo", "Bar");
    properties.put("provider", "delta"); // should be filtered

    DDLRequest request =
        CreateTableBuilder.prepare(
            ident("props"),
            schema,
            new Transform[0],
            properties,
            new Configuration(),
            Optional.empty());

    Transaction txn = CreateTableBuilder.buildTransaction(request);
    TransactionCommitResult result =
        txn.commit(request.engine(), CloseableIterable.emptyIterable());

    Map<String, String> committed =
        result.getPostCommitSnapshot().orElseThrow().getTableProperties();
    assertEquals("Bar", committed.get("Foo"));
  }

  // ── helpers ────────────────────────────────────────────────────────

  private static DDLRequest prepareRequest(
      String tablePath, StructType schema, Transform[] partitions) {
    Map<String, String> properties = new HashMap<>();
    properties.put("location", tablePath);
    return CreateTableBuilder.prepare(
        ident("test"), schema, partitions, properties, new Configuration(), Optional.empty());
  }

  private static Identifier ident(String name) {
    return Identifier.of(new String[] {"default"}, name);
  }
}
