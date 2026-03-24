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

import io.delta.kernel.transaction.DataLayoutSpec;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
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
    props.put("Foo", "Bar");

    Map<String, String> filtered = CreateTableBuilder.filterProperties(props);

    assertFalse(filtered.containsKey(TableCatalog.PROP_LOCATION));
    assertFalse(filtered.containsKey(TableCatalog.PROP_PROVIDER));
    assertFalse(filtered.containsKey(TableCatalog.PROP_COMMENT));
    assertFalse(filtered.containsKey("path"));
    assertFalse(filtered.containsKey("option.path"));
    assertEquals("supported", filtered.get("delta.feature.catalogManaged"));
    assertEquals("Bar", filtered.get("Foo"));
    assertEquals(2, filtered.size());
  }

  @Test
  public void testFilterProperties_emptyInput() {
    assertTrue(CreateTableBuilder.filterProperties(Map.of()).isEmpty());
  }

  @Test
  public void testBuildDataLayoutSpec_noPartitions() {
    DataLayoutSpec spec = CreateTableBuilder.buildDataLayoutSpec(new Transform[0]);
    assertTrue(spec.hasNoDataLayoutSpec());
  }

  @Test
  public void testBuildDataLayoutSpec_nullPartitions() {
    DataLayoutSpec spec = CreateTableBuilder.buildDataLayoutSpec(null);
    assertTrue(spec.hasNoDataLayoutSpec());
  }

  @Test
  public void testBuildDataLayoutSpec_withIdentityPartitions() {
    Transform[] partitions =
        new Transform[] {Expressions.identity("year"), Expressions.identity("month")};

    DataLayoutSpec spec = CreateTableBuilder.buildDataLayoutSpec(partitions);

    assertTrue(spec.hasPartitioning());
    assertFalse(spec.hasClustering());
    assertEquals(2, spec.getPartitionColumns().size());
    assertEquals("year", spec.getPartitionColumnsAsStrings().get(0));
    assertEquals("month", spec.getPartitionColumnsAsStrings().get(1));
  }

  @Test
  public void testResolveTablePath_fromLocation() {
    Map<String, String> props = Map.of(TableCatalog.PROP_LOCATION, "/my/path");
    Identifier ident = Identifier.of(new String[] {"db"}, "tbl");
    assertEquals("/my/path", CreateTableBuilder.resolveTablePath(props, ident));
  }

  @Test
  public void testResolveTablePath_fallsBackToIdentName() {
    Identifier ident = Identifier.of(new String[] {"db"}, "fallback_table");
    assertEquals("fallback_table", CreateTableBuilder.resolveTablePath(Map.of(), ident));
  }
}
