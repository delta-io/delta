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

import io.delta.kernel.expressions.Column;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link CreateTableBuilder} helper methods. */
public class CreateTableBuilderTest {

  // ── filterProperties ──────────────────────────────────────────────

  @Test
  public void testFilterProperties_removesDsv2InternalKeys() {
    Map<String, String> props = new HashMap<>();
    props.put(TableCatalog.PROP_LOCATION, "/some/path");
    props.put(TableCatalog.PROP_PROVIDER, "delta");
    props.put(TableCatalog.PROP_COMMENT, "a comment");
    props.put(TableCatalog.PROP_OWNER, "owner");
    props.put(TableCatalog.PROP_EXTERNAL, "true");
    props.put(TableCatalog.PROP_IS_MANAGED_LOCATION, "true");
    props.put("path", "/some/path");
    props.put("option.path", "/some/path");
    props.put("delta.feature.catalogManaged", "supported");
    props.put("io.unitycatalog.tableId", "abc-123");
    props.put("Foo", "Bar");

    Map<String, String> filtered = DDLUtils.filterProperties(props);

    assertFalse(filtered.containsKey(TableCatalog.PROP_LOCATION));
    assertFalse(filtered.containsKey(TableCatalog.PROP_PROVIDER));
    assertFalse(filtered.containsKey(TableCatalog.PROP_COMMENT));
    assertFalse(filtered.containsKey("path"));
    assertFalse(filtered.containsKey("option.path"));
    assertFalse(filtered.containsKey("io.unitycatalog.tableId"));
    assertEquals("supported", filtered.get("delta.feature.catalogManaged"));
    assertEquals("Bar", filtered.get("Foo"));
    assertEquals(2, filtered.size());
  }

  @Test
  public void testFilterProperties_throwsOnProtocolVersionOverrides() {
    for (String key :
        List.of(
            "delta.minReaderVersion", "delta.minWriterVersion", "delta.ignoreProtocolDefaults")) {
      Map<String, String> props = new HashMap<>();
      props.put(key, "3");
      UnsupportedOperationException ex =
          assertThrows(UnsupportedOperationException.class, () -> DDLUtils.filterProperties(props));
      assertTrue(ex.getMessage().contains(key));
      assertTrue(ex.getMessage().contains("V2 connector"));
    }
  }

  // ── toDataLayoutSpec ──────────────────────────────────────────────

  @Test
  public void testDataLayoutSpec_emptyAndNull() {
    assertTrue(DDLUtils.toDataLayoutSpec(new Transform[0]).hasNoDataLayoutSpec());
    assertTrue(DDLUtils.toDataLayoutSpec(null).hasNoDataLayoutSpec());
  }

  @Test
  public void testDataLayoutSpec_identityPartitions() {
    Transform[] partitions =
        new Transform[] {Expressions.identity("year"), Expressions.identity("month")};

    DataLayoutSpec spec = DDLUtils.toDataLayoutSpec(partitions);

    assertTrue(spec.hasPartitioning());
    assertFalse(spec.hasClustering());
    assertEquals("year", spec.getPartitionColumnsAsStrings().get(0));
    assertEquals("month", spec.getPartitionColumnsAsStrings().get(1));
  }

  @Test
  public void testDataLayoutSpec_clusterBy() {
    Transform[] partitions = new Transform[] {DDLTestUtils.clusterByTransform("year", "month")};

    DataLayoutSpec spec = DDLUtils.toDataLayoutSpec(partitions);

    assertTrue(spec.hasClustering());
    assertFalse(spec.hasPartitioning());
    assertEquals("year", spec.getClusteringColumns().get(0).getNames()[0]);
    assertEquals("month", spec.getClusteringColumns().get(1).getNames()[0]);
  }

  @Test
  public void testDataLayoutSpec_unsupportedTransformThrows() {
    Transform[] partitions = new Transform[] {Expressions.bucket(10, "col")};
    assertThrows(UnsupportedOperationException.class, () -> DDLUtils.toDataLayoutSpec(partitions));
  }

  @Test
  public void testDataLayoutSpec_mixedClusterByAndPartitionThrows() {
    Transform[] partitions =
        new Transform[] {DDLTestUtils.clusterByTransform("year"), Expressions.identity("month")};
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> DDLUtils.toDataLayoutSpec(partitions));
    assertTrue(ex.getMessage().contains("CLUSTER BY cannot be combined"));
  }

  // ── resolveTablePath ──────────────────────────────────────────────

  @Test
  public void testResolveTablePath_precedenceAndFallback() {
    Identifier ident = Identifier.of(new String[] {"db"}, "tbl");

    assertEquals(
        "/loc",
        DDLUtils.resolveTablePath(
            Map.of(TableCatalog.PROP_LOCATION, "/loc", "path", "/path", "option.path", "/opt"),
            ident));

    assertThrows(IllegalArgumentException.class, () -> DDLUtils.resolveTablePath(Map.of(), ident));
  }

  @Test
  public void testResolveTablePath_skipsNullAndBlankValues() {
    Identifier ident = Identifier.of(new String[] {"db"}, "tbl");
    Map<String, String> props = new HashMap<>();
    props.put(TableCatalog.PROP_LOCATION, null);
    props.put("path", "   ");
    props.put("option.path", "/valid");

    assertEquals("/valid", DDLUtils.resolveTablePath(props, ident));
  }

  // ── extractComment ────────────────────────────────────────────────

  @Test
  public void testExtractComment() {
    assertEquals(
        Optional.of("my table"),
        DDLUtils.extractComment(Map.of(TableCatalog.PROP_COMMENT, "my table")));
    assertTrue(DDLUtils.extractComment(Map.of()).isEmpty());
    assertTrue(DDLUtils.extractComment(Map.of(TableCatalog.PROP_COMMENT, "")).isEmpty());
  }

  // ── validateClusteringColumns ─────────────────────────────────────

  private static final StructType KERNEL_SCHEMA =
      new StructType()
          .add(new StructField("id", IntegerType.INTEGER, false))
          .add(new StructField("name", StringType.STRING, true))
          .add(new StructField("year", IntegerType.INTEGER, false))
          .add(
              new StructField(
                  "address",
                  new StructType()
                      .add(new StructField("city", StringType.STRING, true))
                      .add(new StructField("zip", IntegerType.INTEGER, true)),
                  true))
          .add(
              new StructField(
                  "tags", new MapType(StringType.STRING, StringType.STRING, true), true));

  @Test
  public void testValidateClusteringColumns_validColumns() {
    DataLayoutSpec spec = DataLayoutSpec.clustered(List.of(new Column("id"), new Column("year")));
    DDLUtils.validateClusteringColumns(spec, KERNEL_SCHEMA);
  }

  @Test
  public void testValidateClusteringColumns_nonExistentColumnThrows() {
    DataLayoutSpec spec = DataLayoutSpec.clustered(List.of(new Column("nonexistent")));
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> DDLUtils.validateClusteringColumns(spec, KERNEL_SCHEMA));
    assertTrue(ex.getMessage().contains("nonexistent"));
  }

  @Test
  public void testValidateClusteringColumns_duplicateColumnThrows() {
    DataLayoutSpec spec = DataLayoutSpec.clustered(List.of(new Column("id"), new Column("id")));
    assertThrows(
        IllegalArgumentException.class,
        () -> DDLUtils.validateClusteringColumns(spec, KERNEL_SCHEMA));
  }

  @Test
  public void testValidateClusteringColumns_nestedColumns() {
    DataLayoutSpec spec =
        DataLayoutSpec.clustered(
            List.of(new Column(new String[] {"address", "city"}), new Column("id")));
    DDLUtils.validateClusteringColumns(spec, KERNEL_SCHEMA);
  }

  @Test
  public void testValidateClusteringColumns_nestedColumnNotFoundThrows() {
    DataLayoutSpec spec =
        DataLayoutSpec.clustered(List.of(new Column(new String[] {"address", "nonexistent"})));
    assertThrows(
        IllegalArgumentException.class,
        () -> DDLUtils.validateClusteringColumns(spec, KERNEL_SCHEMA));
  }

  @Test
  public void testValidateClusteringColumns_nestedThroughNonStructThrows() {
    DataLayoutSpec spec =
        DataLayoutSpec.clustered(List.of(new Column(new String[] {"name", "nested"})));
    assertThrows(
        IllegalArgumentException.class,
        () -> DDLUtils.validateClusteringColumns(spec, KERNEL_SCHEMA));
  }

  @Test
  public void testValidateClusteringColumns_nestedThroughMapTypeThrows() {
    DataLayoutSpec spec =
        DataLayoutSpec.clustered(List.of(new Column(new String[] {"tags", "key"})));
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> DDLUtils.validateClusteringColumns(spec, KERNEL_SCHEMA));
    assertTrue(ex.getMessage().contains("not a struct"));
  }
}
