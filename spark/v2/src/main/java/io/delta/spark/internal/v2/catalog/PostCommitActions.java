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
package io.delta.spark.internal.v2.catalog;

import static java.util.Objects.requireNonNull;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;

/** Factories for common {@link PostCommitAction} implementations. */
public final class PostCommitActions {

  private PostCommitActions() {}

  public static PostCommitAction none() {
    return NoOpPostCommitAction.INSTANCE;
  }

  /** Post-commit action used for Unity Catalog managed tables. Currently a no-op. */
  public static PostCommitAction unityCatalog() {
    return UnityCatalogPostCommitAction.INSTANCE;
  }

  /** Action that registers a {@link CatalogTable} via Spark's {@code SessionCatalog}. */
  public static PostCommitAction sessionCatalog(SparkSession spark, CatalogTable tableDesc) {
    requireNonNull(spark, "spark is null");
    requireNonNull(tableDesc, "tableDesc is null");
    return new SessionCatalogPostCommitAction(spark, tableDesc);
  }

  private static final class NoOpPostCommitAction implements PostCommitAction {
    private static final NoOpPostCommitAction INSTANCE = new NoOpPostCommitAction();

    @Override
    public void execute() {}

    @Override
    public void abort(Throwable cause) {}
  }

  private static final class UnityCatalogPostCommitAction implements PostCommitAction {
    private static final UnityCatalogPostCommitAction INSTANCE = new UnityCatalogPostCommitAction();

    @Override
    public void execute() {}

    @Override
    public void abort(Throwable cause) {}
  }

  private static final class SessionCatalogPostCommitAction implements PostCommitAction {
    private final SparkSession spark;
    private final CatalogTable tableDesc;

    private SessionCatalogPostCommitAction(SparkSession spark, CatalogTable tableDesc) {
      this.spark = spark;
      this.tableDesc = tableDesc;
    }

    @Override
    public void execute() {
      // Kernel creates the table directory before catalog registration, so location validation
      // would fail for managed tables (the directory already exists).
      boolean validateLoc = tableDesc.tableType() != CatalogTableType.MANAGED();
      spark.sessionState().catalog().createTable(tableDesc, false, validateLoc);
    }

    @Override
    public void abort(Throwable cause) {}
  }
}
