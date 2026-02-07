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
package io.unitycatalog.test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Simple TableCatalog implementation used to record UC registration calls in unit tests. */
public final class RecordingTableCatalog implements TableCatalog {

  private String catalogName = "recording";
  private Map<String, String> recordedProperties;

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    return new Identifier[0];
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    throw new UnsupportedOperationException("loadTable not implemented for RecordingTableCatalog");
  }

  @Override
  public Table createTable(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    recordedProperties = properties == null ? Collections.emptyMap() : new HashMap<>(properties);
    return new DummyTable();
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    throw new UnsupportedOperationException("alterTable not implemented for RecordingTableCatalog");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    throw new UnsupportedOperationException("dropTable not implemented for RecordingTableCatalog");
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new UnsupportedOperationException("renameTable not implemented for RecordingTableCatalog");
  }

  public boolean wasCreateTableCalled() {
    return recordedProperties != null;
  }

  public Map<String, String> getRecordedProperties() {
    return recordedProperties;
  }

  private static final class DummyTable implements Table {
    @Override
    public String name() {
      return "dummy";
    }

    @Override
    public StructType schema() {
      return new StructType();
    }

    @Override
    public java.util.Set<org.apache.spark.sql.connector.catalog.TableCapability> capabilities() {
      return Collections.emptySet();
    }

    @Override
    public Map<String, String> properties() {
      return Collections.emptyMap();
    }
  }
}

