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
package org.apache.spark.sql.delta.catalog;

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import io.unitycatalog.test.RecordingTableCatalog;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.DeltaSQLCommandJavaTest;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class DeltaCatalogCreateTableSuite implements DeltaSQLCommandJavaTest {

  private transient SparkSession spark;

  @Before
  public void setUp() {
    spark = buildSparkSession();
    spark.conf().set("spark.databricks.delta.v2.enableMode", "STRICT");
    SparkSession.setActiveSession(spark);
    SparkSession.setDefaultSession(spark);
  }

  @After
  public void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
      SparkSession.setActiveSession(null);
      SparkSession.setDefaultSession(null);
    }
  }

  @Test
  public void testCreateTableStrictUcManagedRegistersViaDelegateAndReturnsLoadedTable() {
    RecordingTableCatalog delegate = new RecordingTableCatalog();
    RecordingStagedTable staged = new RecordingStagedTable();
    Table expected = new SimpleTable("loaded");

    DeltaCatalog catalog = new TestDeltaCatalog(staged, expected);
    catalog.setDelegateCatalog(delegate);

    Identifier ident = Identifier.of(new String[] {"default"}, "uc_table");
    StructType schema = new StructType().add("id", DataTypes.IntegerType);
    Transform[] partitions = new Transform[] {Expressions.identity("id")};
    Map<String, String> properties = new HashMap<>();
    properties.put(TableCatalog.PROP_PROVIDER, "delta");
    properties.put(TableCatalog.PROP_LOCATION, "/tmp/staging");
    properties.put("delta.feature.catalogManaged", "supported");
    properties.put(UCCommitCoordinatorClient.UC_TABLE_ID_KEY_OLD, "uc-table-id");
    properties.put(TableCatalog.PROP_IS_MANAGED_LOCATION, "true");

    Table created = catalog.createTable(ident, schema, partitions, properties);

    assertSame(expected, created);
    assertTrue(staged.wasCommitted());
    assertTrue(delegate.wasCreateTableCalled());
    Map<String, String> registeredProps = delegate.getRecordedProperties();
    assertEquals("delta", registeredProps.get(TableCatalog.PROP_PROVIDER));
    assertEquals("/tmp/staging", registeredProps.get(TableCatalog.PROP_LOCATION));
    assertEquals("uc-table-id", registeredProps.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY));
    assertFalse(registeredProps.containsKey(UCCommitCoordinatorClient.UC_TABLE_ID_KEY_OLD));
  }

  private static final class TestDeltaCatalog extends DeltaCatalog {
    private final StagedTable staged;
    private final Table loadResult;

    private TestDeltaCatalog(StagedTable staged, Table loadResult) {
      this.staged = staged;
      this.loadResult = loadResult;
    }

    @Override
    public StagedTable stageCreate(
        Identifier ident,
        StructType schema,
        Transform[] partitions,
        Map<String, String> properties) {
      return staged;
    }

    @Override
    public Table loadTable(Identifier ident) {
      return loadResult;
    }
  }

  private static final class RecordingStagedTable implements StagedTable {
    private boolean committed = false;

    @Override
    public void commitStagedChanges() {
      committed = true;
    }

    @Override
    public void abortStagedChanges() {}

    @Override
    public String name() {
      return "staged";
    }

    @Override
    public StructType schema() {
      return new StructType();
    }

    @Override
    public Transform[] partitioning() {
      return new Transform[0];
    }

    @Override
    public Map<String, String> properties() {
      return Collections.emptyMap();
    }

    @Override
    public java.util.Set<TableCapability> capabilities() {
      return Collections.emptySet();
    }

    private boolean wasCommitted() {
      return committed;
    }
  }

  private static final class SimpleTable implements Table {
    private final String name;

    private SimpleTable(String name) {
      this.name = name;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public StructType schema() {
      return new StructType();
    }

    @Override
    public java.util.Set<TableCapability> capabilities() {
      return Collections.emptySet();
    }

    @Override
    public Map<String, String> properties() {
      return Collections.emptyMap();
    }
  }
}

