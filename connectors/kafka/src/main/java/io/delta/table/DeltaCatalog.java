/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.delta.table;

import io.delta.kernel.Operation;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.utils.SchemaUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.LocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaCatalog implements Catalog, SupportsNamespaces, Configurable<Configuration> {
  private static final Logger LOG = LoggerFactory.getLogger(DeltaCatalog.class);
  public static final String HIVE_WAREHOUSE_PROP = "hive.metastore.warehouse.dir";
  private static final Joiner SLASH = Joiner.on("/");

  private Configuration conf = null;
  private String name = null;
  private Map<String, String> catalogProperties = null;
  private String warehouse = null;
  private FileIO fileIO = null;

  @Override
  public String name() {
    return name;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return Collections.emptyList();
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    throw new UnsupportedOperationException("Drop is not supported");
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException("Rename is not supported");
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema) {
    String tableLocation = tableLocation(identifier);
    Engine engine = DefaultEngine.create(conf);
    io.delta.kernel.Table kernelTable = io.delta.kernel.Table.forPath(engine, tableLocation);
    kernelTable
        .createTransactionBuilder(engine, "iceberg", Operation.CREATE_TABLE)
        .withSchema(engine, SchemaUtils.fromIcebergSchema(schema.asStruct()))
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable());

    return new DeltaTable(identifier, conf, tableLocation);
  }

  @Override
  public Table loadTable(TableIdentifier ident) {
    String tableLocation = tableLocation(ident);
    return new DeltaTable(ident, conf, tableLocation);
  }

  private String tableLocation(TableIdentifier ident) {
    return SLASH.join(warehouse, ident.name());
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    if (null == conf) {
      LOG.warn("No Hadoop Configuration was set, using the default environment Configuration");
      conf = new Configuration();
    }

    this.name = name;
    this.catalogProperties = ImmutableMap.copyOf(properties);

    if (properties.containsKey(CatalogProperties.WAREHOUSE_LOCATION)) {
      this.warehouse =
          LocationUtil.stripTrailingSlash(properties.get(CatalogProperties.WAREHOUSE_LOCATION));
      conf.set(HIVE_WAREHOUSE_PROP, warehouse); // keep the Configuration in sync
    } else {
      this.warehouse = LocationUtil.stripTrailingSlash(conf.get(HIVE_WAREHOUSE_PROP, null));
    }

    Preconditions.checkArgument(
        warehouse != null, "Missing required property: %s", CatalogProperties.WAREHOUSE_LOCATION);

    String ioImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    this.fileIO =
        ioImpl == null ? new HadoopFileIO(conf) : CatalogUtil.loadFileIO(ioImpl, properties, conf);

    // copy S3 properties to Hadoop Configuration
    ImmutableMap.of(
            "s3.endpoint", "fs.s3a.endpoint",
            "s3.access-key-id", "fs.s3a.access.key",
            "s3.secret-access-key", "fs.s3a.secret.key")
        .forEach(
            (s3Prop, hadoopProp) -> {
              if (properties.containsKey(s3Prop)) {
                conf.set(hadoopProp, properties.get(s3Prop));
              }
            });
    conf.set("fs.s3a.connection.ssl.enabled", "false");
    conf.set("fs.s3a.path.style.access", "true");
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {}

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    return Collections.emptyList();
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    return Collections.emptyMap();
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    return false;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    return false;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    return false;
  }
}
