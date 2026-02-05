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
package org.apache.spark.sql.delta.catalog;

import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ResolvedCreateRequestSuite {

  // Validates path-based CREATE normalization, property filtering, and partition layout creation.
  @Test
  public void testForPathCreateNormalizesFieldsAndFiltersProperties() throws Exception {
    String path = Files.createTempDirectory("dsv2-path").toFile().getAbsolutePath();
    Identifier ident = Identifier.of(new String[] {"delta"}, path);
    StructType schema = new StructType()
        .add("id", DataTypes.IntegerType)
        .add("name", DataTypes.StringType);
    Transform[] partitions = new Transform[] {Expressions.identity("id")};
    Map<String, String> props = new HashMap<>();
    props.put(TableCatalog.PROP_LOCATION, path);
    props.put("delta.appendOnly", "true");

    ResolvedCreateRequest request = ResolvedCreateRequest.forPathCreate(
        ident,
        schema,
        partitions,
        props);

    assertEquals(path, request.getTablePath());
    assertEquals(ResolvedCreateRequest.Operation.CREATE, request.getOperation());
    assertFalse(request.isUCManaged());
    assertEquals(Collections.singletonList("id"), request.getPartitionColumns());
    assertFalse(request.getProperties().containsKey(TableCatalog.PROP_LOCATION));
    assertEquals("true", request.getProperties().get("delta.appendOnly"));
    assertTrue(request.getDataLayoutSpec().hasPartitioning());
  }

  // Validates UC-managed CREATE persists UC metadata and staging path details.
  @Test
  public void testForUcManagedCreateSetsUcMetadata() throws Exception {
    String path = Files.createTempDirectory("dsv2-uc").toFile().getAbsolutePath();
    Identifier ident = Identifier.of(new String[] {"default"}, "uc_table");
    StructType schema = new StructType().add("id", DataTypes.IntegerType);
    Transform[] partitions = new Transform[] {Expressions.identity("id")};
    Map<String, String> props = new HashMap<>();
    props.put("delta.appendOnly", "true");

    ResolvedCreateRequest request = ResolvedCreateRequest.forUCManagedCreate(
        ident,
        schema,
        partitions,
        props,
        "uc-table-id",
        path);

    assertTrue(request.isUCManaged());
    assertTrue(request.getUcTableId().isPresent());
    assertEquals("uc-table-id", request.getUcTableId().get());
    assertTrue(request.getUcStagingPath().isPresent());
    assertEquals(path, request.getUcStagingPath().get());
    assertEquals(path, request.getTablePath());
    assertEquals(Collections.singletonList("id"), request.getPartitionColumns());
  }
}
