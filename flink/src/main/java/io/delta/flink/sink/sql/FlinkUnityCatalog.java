/*
 *  Copyright (2021) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.sink.sql;

import io.delta.flink.table.ExceptionUtils;
import io.delta.flink.table.UnityCatalog;
import io.unitycatalog.client.model.SchemaInfo;
import java.net.URI;
import java.util.List;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;

public class FlinkUnityCatalog extends AbstractCatalog {

  private final UnityCatalog deltaCatalog;

  public FlinkUnityCatalog(String name, String defaultDatabase, String endpoint, String token) {
    super(name, defaultDatabase);
    this.deltaCatalog = new UnityCatalog(getName(), URI.create(endpoint), token);
  }

  @Override
  public void open() throws CatalogException {}

  @Override
  public void close() throws CatalogException {}

  @Override
  public List<String> listDatabases() throws CatalogException {
    return deltaCatalog.listSchemas();
  }

  protected void notSupported(String operation) {
    throw new CatalogException(
        String.format("Operation not supported: <%s>. Please operate in UC", operation));
  }

  @Override
  public CatalogDatabase getDatabase(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    try {
      SchemaInfo info = deltaCatalog.getSchema(databaseName);
      return new FlinkUnityCatalogDatabase(info);
    } catch (ExceptionUtils.ResourceNotFoundException e) {
      throw new DatabaseNotExistException(getName(), databaseName);
    }
  }

  @Override
  public boolean databaseExists(String databaseName) throws CatalogException {
    try {
      deltaCatalog.getSchema(databaseName);
      return true;
    } catch (ExceptionUtils.ResourceNotFoundException e) {
      return false;
    }
  }

  @Override
  public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
      throws CatalogException {
    notSupported("CREATE DATABASE");
  }

  @Override
  public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
      throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
    notSupported("DROP DATABASE");
  }

  @Override
  public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
      throws DatabaseNotExistException, CatalogException {
    notSupported("ALTER DATABASE");
  }

  @Override
  public List<String> listTables(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    try {
      return deltaCatalog.listTables(databaseName);
    } catch (ExceptionUtils.ResourceNotFoundException e) {
      throw new DatabaseNotExistException(getName(), databaseName);
    }
  }

  @Override
  public List<String> listViews(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    notSupported("LIST VIEWS");
    return null;
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    try {
      return new FlinkUnityCatalogTable(
          deltaCatalog.getTableDetail(String.format("%s.%s", getName(), tablePath.getFullName())),
          deltaCatalog.getEndpoint(),
          deltaCatalog.getToken());
    } catch (ExceptionUtils.ResourceNotFoundException e) {
      throw new TableNotExistException(getName(), tablePath);
    }
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath, long timestamp)
      throws TableNotExistException, CatalogException {
    return super.getTable(tablePath, timestamp);
  }

  @Override
  public boolean tableExists(ObjectPath tablePath) throws CatalogException {
    try {
      deltaCatalog.getTableDetail(String.format("%s.%s", getName(), tablePath.getFullName()));
      return true;
    } catch (ExceptionUtils.ResourceNotFoundException e) {
      return false;
    }
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    notSupported("DROP TABLE");
  }

  @Override
  public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
      throws TableNotExistException, TableAlreadyExistException, CatalogException {
    notSupported("RENAME TABLE");
  }

  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
      throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    notSupported("CREATE TABLE");
  }

  @Override
  public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    notSupported("ALTER TABLE");
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    notSupported("LIST PARTITIONS");
    return null;
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
          CatalogException {
    notSupported("LIST PARTITIONS");
    return List.of();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitionsByFilter(
      ObjectPath tablePath, List<Expression> filters)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    notSupported("LIST PARTITIONS BY FILTER");
    return List.of();
  }

  @Override
  public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    notSupported("GET PARTITION");
    return null;
  }

  @Override
  public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws CatalogException {
    notSupported("PARTITION EXISTS");
    return false;
  }

  @Override
  public void createPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition partition,
      boolean ignoreIfExists)
      throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
          PartitionAlreadyExistsException, CatalogException {
    notSupported("CREATE PARTITION");
  }

  @Override
  public void dropPartition(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    notSupported("DROP PARTITION");
  }

  @Override
  public void alterPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition newPartition,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    notSupported("ALTER PARTITION");
  }

  @Override
  public List<String> listFunctions(String dbName)
      throws DatabaseNotExistException, CatalogException {
    notSupported("LIST FUNCTIONS");
    return List.of();
  }

  @Override
  public CatalogFunction getFunction(ObjectPath functionPath)
      throws FunctionNotExistException, CatalogException {
    notSupported("GET FUNCTION");
    return null;
  }

  @Override
  public boolean functionExists(ObjectPath functionPath) throws CatalogException {
    notSupported("FUNCTION EXISTS");
    return false;
  }

  @Override
  public void createFunction(
      ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
      throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
    notSupported("CREATE FUNCTION");
  }

  @Override
  public void alterFunction(
      ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
      throws FunctionNotExistException, CatalogException {
    notSupported("ALTER FUNCTION");
  }

  @Override
  public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
      throws FunctionNotExistException, CatalogException {
    notSupported("DROP FUNCTION");
  }

  @Override
  public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    notSupported("GET TABLE STATISTICS");
    return null;
  }

  @Override
  public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    notSupported("GET TABLE COLUMN STATISTICS");
    return null;
  }

  @Override
  public CatalogTableStatistics getPartitionStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    notSupported("GET PARTITION STATISTICS");
    return null;
  }

  @Override
  public CatalogColumnStatistics getPartitionColumnStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    notSupported("GET PARTITION COLUMN STATISTICS");
    return null;
  }

  @Override
  public void alterTableStatistics(
      ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    notSupported("ALTER TABLE STATISTICS");
  }

  @Override
  public void alterTableColumnStatistics(
      ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException, TablePartitionedException {
    notSupported("ALTER TABLE COLUMN STATISTICS");
  }

  @Override
  public void alterPartitionStatistics(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogTableStatistics partitionStatistics,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    notSupported("ALTER PARTITION STATISTICS");
  }

  @Override
  public void alterPartitionColumnStatistics(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogColumnStatistics columnStatistics,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    notSupported("ALTER PARTITION COLUMN STATISTICS");
  }
}
