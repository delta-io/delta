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

import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCTableInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Test;

/** Unit tests for the {@link DDLRequestContext} POJO. */
public class DDLRequestContextTest {

  private static final Engine ENGINE = DefaultEngine.create(new Configuration());

  @Test
  public void testConstructionWithAllFields() {
    Identifier ident = Identifier.of(new String[] {"prod_catalog", "analytics"}, "page_views");
    String tablePath = "/managed/prod_catalog/analytics/page_views";
    StructType schema =
        new StructType()
            .add(new StructField("user_id", IntegerType.INTEGER, false))
            .add(new StructField("url", StringType.STRING, false))
            .add(new StructField("event_ts", TimestampType.TIMESTAMP, false));
    DataLayoutSpec clustering =
        DataLayoutSpec.clustered(List.of(new Column("user_id"), new Column("event_ts")));
    UCTableInfo ucInfo =
        new UCTableInfo(
            "uc-table-id-123", tablePath, "https://uc.example.com", Map.of("token", "fake-token"));
    CreateTableTransactionBuilder txnBuilder =
        TableManager.buildCreateTableTransaction(tablePath, schema, "test");

    DDLRequestContext request =
        new DDLRequestContext(
            ident,
            tablePath,
            schema,
            Map.of("delta.appendOnly", "true", "delta.logRetentionDuration", "interval 30 days"),
            Optional.of("Tracks page view events from the analytics pipeline"),
            clustering,
            ENGINE,
            Optional.of(ucInfo),
            txnBuilder);

    assertEquals("page_views", request.ident().name());
    assertArrayEquals(new String[] {"prod_catalog", "analytics"}, request.ident().namespace());
    assertEquals(tablePath, request.tablePath());
    assertEquals(3, request.kernelSchema().length());
    assertEquals("true", request.properties().get("delta.appendOnly"));
    assertEquals(
        Optional.of("Tracks page view events from the analytics pipeline"), request.comment());
    assertTrue(request.dataLayoutSpec().hasClustering());
    assertEquals(2, request.dataLayoutSpec().getClusteringColumns().size());
    assertTrue(request.ucTableInfo().isPresent());
    assertEquals("uc-table-id-123", request.ucTableInfo().get().getTableId());
    assertNotNull(request.transactionBuilder());
  }

  @Test
  public void testPropertiesAreImmutableAndDefensivelyCopied() {
    Identifier ident = Identifier.of(new String[] {"db"}, "tbl");
    StructType schema = new StructType().add(new StructField("id", IntegerType.INTEGER, false));

    HashMap<String, String> original = new HashMap<>();
    original.put("k", "v");
    CreateTableTransactionBuilder txnBuilder =
        TableManager.buildCreateTableTransaction("/p", schema, "test");
    DDLRequestContext request =
        new DDLRequestContext(
            ident,
            "/p",
            schema,
            original,
            /* comment = */ Optional.empty(),
            DataLayoutSpec.noDataLayout(),
            ENGINE,
            /* ucTableInfo = */ Optional.empty(),
            txnBuilder);

    // Mutating the original map must not leak into the DDLRequestContext
    original.put("injected", "bad");
    assertNull(request.properties().get("injected"));

    // The returned map must be unmodifiable
    assertThrows(UnsupportedOperationException.class, () -> request.properties().put("x", "y"));
  }
}
