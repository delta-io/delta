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

import io.delta.kernel.Snapshot;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes a committed CREATE TABLE result to the catalog.
 *
 * <p>All catalog-interaction logic is encapsulated here so the orchestrator sees only {@code
 * publisher.publish(committed)}.
 */
public class CreateTablePublisher
    implements TablePublisher<CommittedTableTxn, CreateTableCatalogPublication> {

  private static final Logger LOG = LoggerFactory.getLogger(CreateTablePublisher.class);

  private final CreateTableContext ctx;

  /** Callback to push property changes to the catalog (e.g., super.alterTable). */
  private final BiConsumer<Identifier, TableChange[]> catalogUpdater;

  public CreateTablePublisher(
      CreateTableContext ctx, BiConsumer<Identifier, TableChange[]> catalogUpdater) {
    this.ctx = ctx;
    this.catalogUpdater = catalogUpdater;
  }

  @Override
  public CreateTableCatalogPublication buildCatalogPublication(CommittedTableTxn committed) {
    Snapshot snapshot = committed.getPostCommitSnapshot();
    StructType committedSchema = SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema());
    Map<String, String> committedProps = new HashMap<>(snapshot.getTableProperties());
    return new CreateTableCatalogPublication(committedSchema, committedProps, snapshot.getPath());
  }

  /** Builds the publication and pushes committed metadata to the catalog. */
  public void publish(CommittedTableTxn committed) {
    CreateTableCatalogPublication pub = buildCatalogPublication(committed);

    if (!ctx.isUnityCatalog) {
      return; // non-UC tables: catalog entry is the Delta log itself
    }

    // UC-managed: update the pre-registered catalog entry with committed Delta metadata
    List<TableChange> changes = new ArrayList<>();
    for (Map.Entry<String, String> entry : pub.getProperties().entrySet()) {
      changes.add(TableChange.setProperty(entry.getKey(), entry.getValue()));
    }
    if (!changes.isEmpty()) {
      try {
        catalogUpdater.accept(ctx.ident, changes.toArray(new TableChange[0]));
      } catch (Exception e) {
        LOG.warn(
            "Failed to update catalog with committed metadata for {}: {}",
            ctx.ident,
            e.getMessage());
      }
    }
  }
}
