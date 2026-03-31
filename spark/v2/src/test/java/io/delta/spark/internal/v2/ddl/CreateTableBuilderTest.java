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
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.skipping.clustering.temp.ClusterByTransform;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link CreateTableBuilder} helper methods. */
public class CreateTableBuilderTest {

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
    props.put("delta.minReaderVersion", "3");
    props.put("delta.minWriterVersion", "7");
    props.put("delta.ignoreProtocolDefaults", "true");
    props.put("Foo", "Bar");

    Map<String, String> filtered = CreateTableBuilder.filterProperties(props);

    assertFalse(filtered.containsKey(TableCatalog.PROP_LOCATION));
    assertFalse(filtered.containsKey(TableCatalog.PROP_PROVIDER));
    assertFalse(filtered.containsKey(TableCatalog.PROP_COMMENT));
    assertFalse(filtered.containsKey("path"));
    assertFalse(filtered.containsKey("option.path"));
    assertFalse(filtered.containsKey("delta.minReaderVersion"));
    assertFalse(filtered.containsKey("delta.minWriterVersion"));
    assertFalse(filtered.containsKey("delta.ignoreProtocolDefaults"));
    assertEquals("supported", filtered.get("delta.feature.catalogManaged"));
    assertEquals("Bar", filtered.get("Foo"));
    assertEquals(2, filtered.size());
  }

  @Test
  public void testDataLayoutSpec_emptyAndNull() {
    assertTrue(CreateTableBuilder.toDataLayoutSpec(new Transform[0]).hasNoDataLayoutSpec());
    assertTrue(CreateTableBuilder.toDataLayoutSpec(null).hasNoDataLayoutSpec());
  }

  @Test
  public void testDataLayoutSpec_identityPartitions() {
    Transform[] partitions =
        new Transform[] {Expressions.identity("year"), Expressions.identity("month")};

    DataLayoutSpec spec = CreateTableBuilder.toDataLayoutSpec(partitions);

    assertTrue(spec.hasPartitioning());
    assertFalse(spec.hasClustering());
    assertEquals("year", spec.getPartitionColumnsAsStrings().get(0));
    assertEquals("month", spec.getPartitionColumnsAsStrings().get(1));
  }

  @Test
  public void testDataLayoutSpec_clusterBy() {
    Transform[] partitions = new Transform[] {clusterByTransform("year", "month")};

    DataLayoutSpec spec = CreateTableBuilder.toDataLayoutSpec(partitions);

    assertTrue(spec.hasClustering());
    assertFalse(spec.hasPartitioning());
    assertEquals("year", spec.getClusteringColumns().get(0).getNames()[0]);
    assertEquals("month", spec.getClusteringColumns().get(1).getNames()[0]);
  }

  @Test
  public void testDataLayoutSpec_unsupportedTransformThrows() {
    Transform[] partitions = new Transform[] {Expressions.bucket(10, "col")};
    assertThrows(
        UnsupportedOperationException.class, () -> CreateTableBuilder.toDataLayoutSpec(partitions));
  }

  @Test
  public void testResolveTablePath_precedenceAndFallback() {
    Identifier ident = Identifier.of(new String[] {"db"}, "tbl");

    // location takes precedence over path and option.path
    assertEquals(
        "/loc",
        CreateTableBuilder.resolveTablePath(
            Map.of(TableCatalog.PROP_LOCATION, "/loc", "path", "/path", "option.path", "/opt"),
            ident));

    // throws when no path keys present (non-UC tables must specify a location)
    assertThrows(
        IllegalArgumentException.class, () -> CreateTableBuilder.resolveTablePath(Map.of(), ident));
  }

  @Test
  public void testExtractComment() {
    assertEquals(
        Optional.of("my table"),
        CreateTableBuilder.extractComment(Map.of(TableCatalog.PROP_COMMENT, "my table")));
    assertTrue(CreateTableBuilder.extractComment(Map.of()).isEmpty());
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
                  true));

  @Test
  public void testValidateClusteringColumns_validColumns() {
    DataLayoutSpec spec = DataLayoutSpec.clustered(List.of(new Column("id"), new Column("year")));
    CreateTableBuilder.validateClusteringColumns(spec, KERNEL_SCHEMA);
  }

  @Test
  public void testValidateClusteringColumns_nonExistentColumnThrows() {
    DataLayoutSpec spec = DataLayoutSpec.clustered(List.of(new Column("nonexistent")));
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> CreateTableBuilder.validateClusteringColumns(spec, KERNEL_SCHEMA));
    assertTrue(ex.getMessage().contains("nonexistent"));
    assertTrue(ex.getMessage().contains("CLUSTER BY"));
  }

  @Test
  public void testValidateClusteringColumns_duplicateColumnThrows() {
    DataLayoutSpec spec = DataLayoutSpec.clustered(List.of(new Column("id"), new Column("id")));
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> CreateTableBuilder.validateClusteringColumns(spec, KERNEL_SCHEMA));
    assertTrue(ex.getMessage().contains("Duplicate"));
  }

  @Test
  public void testValidateClusteringColumns_nestedColumns() {
    DataLayoutSpec spec =
        DataLayoutSpec.clustered(
            List.of(new Column(new String[] {"address", "city"}), new Column("id")));
    CreateTableBuilder.validateClusteringColumns(spec, KERNEL_SCHEMA);
  }

  @Test
  public void testValidateClusteringColumns_multipleNestedSameParent() {
    DataLayoutSpec spec =
        DataLayoutSpec.clustered(
            List.of(
                new Column(new String[] {"address", "city"}),
                new Column(new String[] {"address", "zip"})));
    CreateTableBuilder.validateClusteringColumns(spec, KERNEL_SCHEMA);
  }

  @Test
  public void testValidateClusteringColumns_nestedColumnNotFoundThrows() {
    DataLayoutSpec spec =
        DataLayoutSpec.clustered(List.of(new Column(new String[] {"address", "nonexistent"})));
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> CreateTableBuilder.validateClusteringColumns(spec, KERNEL_SCHEMA));
    assertTrue(ex.getMessage().contains("address.nonexistent"));
  }

  @Test
  public void testValidateClusteringColumns_nestedThroughNonStructThrows() {
    DataLayoutSpec spec =
        DataLayoutSpec.clustered(List.of(new Column(new String[] {"name", "nested"})));
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> CreateTableBuilder.validateClusteringColumns(spec, KERNEL_SCHEMA));
    assertTrue(ex.getMessage().contains("name.nested"));
  }

  private static ClusterByTransform clusterByTransform(String... colNames) {
    List<NamedReference> refs =
        java.util.Arrays.stream(colNames)
            .map(Expressions::column)
            .collect(java.util.stream.Collectors.toList());
    return new ClusterByTransform(scala.jdk.javaapi.CollectionConverters.asScala(refs).toSeq());
  }
}
