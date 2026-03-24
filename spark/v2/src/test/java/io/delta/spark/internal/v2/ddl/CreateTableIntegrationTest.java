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
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for the DSv2 + Kernel CREATE TABLE path.
 *
 * <p>These tests exercise the full build → commit → publish flow for path-based tables without
 * requiring a live Unity Catalog server.
 */
public class CreateTableIntegrationTest {

  @Test
  public void testPathBasedCreateTable_metadataOnly(@TempDir File tempDir) {
    String tablePath = new File(tempDir, "test_table").getAbsolutePath();
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("name", DataTypes.StringType, true);
    Map<String, String> properties = new HashMap<>();
    properties.put("location", tablePath);
    Configuration hadoopConf = new Configuration();
    Identifier ident = Identifier.of(new String[] {"default"}, "test_table");

    CreateTableContext ctx =
        new CreateTableContext(
            ident, schema, new Transform[0], properties, hadoopConf, /* isUnityCatalog */ false);

    // build
    PreparedCreateTableTxn prepared =
        new CreateTableTxnBuilder(
                ctx,
                c -> {
                  throw new AssertionError("should not pre-register");
                })
            .build();

    assertNotNull(prepared.getTransaction());
    assertNotNull(prepared.getEngine());
    assertEquals(tablePath, prepared.getTablePath());

    // commit
    CommittedTableTxn committed = TableCommitter.commit(prepared);

    assertEquals(0, committed.getVersion());
    assertNotNull(committed.getPostCommitSnapshot());

    // verify the committed snapshot
    Snapshot snapshot = committed.getPostCommitSnapshot();
    assertEquals(2, snapshot.getSchema().length()); // id, name
    assertEquals("id", snapshot.getSchema().at(0).getName());
    assertEquals("name", snapshot.getSchema().at(1).getName());
    assertTrue(snapshot.getPartitionColumnNames().isEmpty());

    // publish (non-UC → no catalog update)
    CreateTablePublisher publisher =
        new CreateTablePublisher(
            ctx,
            (id, changes) -> {
              throw new AssertionError("should not update");
            });
    CreateTableCatalogPublication pub = publisher.buildCatalogPublication(committed);

    assertNotNull(pub.getSchema());
    assertEquals(2, pub.getSchema().fields().length);
    assertNotNull(pub.getLocation());
    // publish should be a no-op for non-UC
    publisher.publish(committed);
  }

  @Test
  public void testPathBasedCreateTable_withPartitions(@TempDir File tempDir) {
    String tablePath = new File(tempDir, "partitioned_table").getAbsolutePath();
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("year", DataTypes.IntegerType, false)
            .add("name", DataTypes.StringType, true);
    Map<String, String> properties = new HashMap<>();
    properties.put("location", tablePath);
    Transform[] partitions = new Transform[] {Expressions.identity("year")};
    Configuration hadoopConf = new Configuration();
    Identifier ident = Identifier.of(new String[] {"default"}, "partitioned_table");

    CreateTableContext ctx =
        new CreateTableContext(
            ident, schema, partitions, properties, hadoopConf, /* isUnityCatalog */ false);

    // build
    PreparedCreateTableTxn prepared =
        new CreateTableTxnBuilder(
                ctx,
                c -> {
                  throw new AssertionError("should not pre-register");
                })
            .build();

    // commit
    CommittedTableTxn committed = TableCommitter.commit(prepared);

    assertEquals(0, committed.getVersion());
    Snapshot snapshot = committed.getPostCommitSnapshot();
    assertEquals(1, snapshot.getPartitionColumnNames().size());
    assertEquals("year", snapshot.getPartitionColumnNames().get(0));
  }

  @Test
  public void testPathBasedCreateTable_withUserProperties(@TempDir File tempDir) {
    String tablePath = new File(tempDir, "props_table").getAbsolutePath();
    StructType schema = new StructType().add("id", DataTypes.IntegerType, false);
    Map<String, String> properties = new HashMap<>();
    properties.put("location", tablePath);
    properties.put("Foo", "Bar");
    properties.put("provider", "delta"); // should be filtered
    Configuration hadoopConf = new Configuration();
    Identifier ident = Identifier.of(new String[] {"default"}, "props_table");

    CreateTableContext ctx =
        new CreateTableContext(
            ident, schema, new Transform[0], properties, hadoopConf, /* isUnityCatalog */ false);

    PreparedCreateTableTxn prepared =
        new CreateTableTxnBuilder(
                ctx,
                c -> {
                  throw new AssertionError("should not pre-register");
                })
            .build();
    CommittedTableTxn committed = TableCommitter.commit(prepared);

    // Verify user property "Foo=Bar" was committed
    Map<String, String> committedProps = committed.getPostCommitSnapshot().getTableProperties();
    assertEquals("Bar", committedProps.get("Foo"));
  }

  @Test
  public void testTableCommitter_returnsPostCommitSnapshot(@TempDir File tempDir) {
    String tablePath = new File(tempDir, "commit_test").getAbsolutePath();
    StructType schema = new StructType().add("x", DataTypes.LongType, false);
    Configuration hadoopConf = new Configuration();
    Identifier ident = Identifier.of(new String[] {"default"}, "commit_test");
    Map<String, String> props = new HashMap<>();
    props.put("location", tablePath);

    CreateTableContext ctx =
        new CreateTableContext(ident, schema, new Transform[0], props, hadoopConf, false);

    PreparedCreateTableTxn prepared =
        new CreateTableTxnBuilder(
                ctx,
                c -> {
                  throw new AssertionError("unused");
                })
            .build();
    CommittedTableTxn committed = TableCommitter.commit(prepared);

    assertNotNull(committed.getCommitResult());
    assertEquals(0, committed.getCommitResult().getVersion());
    assertNotNull(committed.getPostCommitSnapshot());
    assertEquals(0, committed.getPostCommitSnapshot().getVersion());
  }

  @Test
  public void testPublisher_derivesFromCommittedSnapshot(@TempDir File tempDir) {
    String tablePath = new File(tempDir, "pub_test").getAbsolutePath();
    StructType schema =
        new StructType()
            .add("a", DataTypes.IntegerType, false)
            .add("b", DataTypes.StringType, true);
    Configuration hadoopConf = new Configuration();
    Identifier ident = Identifier.of(new String[] {"default"}, "pub_test");
    Map<String, String> props = new HashMap<>();
    props.put("location", tablePath);

    CreateTableContext ctx =
        new CreateTableContext(ident, schema, new Transform[0], props, hadoopConf, false);

    PreparedCreateTableTxn prepared =
        new CreateTableTxnBuilder(
                ctx,
                c -> {
                  throw new AssertionError("unused");
                })
            .build();
    CommittedTableTxn committed = TableCommitter.commit(prepared);

    CreateTablePublisher publisher = new CreateTablePublisher(ctx, (id, ch) -> {});
    CreateTableCatalogPublication pub = publisher.buildCatalogPublication(committed);

    // publication schema should come from committed snapshot, not raw input
    assertEquals(2, pub.getSchema().fields().length);
    assertEquals("a", pub.getSchema().fields()[0].name());
    assertEquals("b", pub.getSchema().fields()[1].name());
    assertNotNull(pub.getLocation());
    assertFalse(pub.getLocation().isEmpty());
  }
}
