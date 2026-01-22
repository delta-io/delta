/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.test

import java.util

import scala.collection.{immutable, mutable}
import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.catalog.{DeltaCatalog, DeltaTableV2}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{DelegatingCatalogExtension, Identifier, NamespaceChange, SupportsNamespaces, Table, TableCatalog, TableChange, V1Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils


/**
 * A Utils class for custom catalog implementations that could be used for testing.
 */
class DummyCatalog extends TableCatalog {
  private val spark: SparkSession = SparkSession.active
  protected lazy val tempDir: Path = new Path(Utils.createTempDir().getAbsolutePath)
  // scalastyle:off deltahadoopconfiguration
  protected lazy val fs: FileSystem =
    tempDir.getFileSystem(spark.sessionState.newHadoopConf())
  // scalastyle:on deltahadoopconfiguration

  override def name: String = "dummy"

  def getTablePath(tableName: String): Path = {
    new Path(tempDir.toString + "/" + tableName)
  }
  override def defaultNamespace(): Array[String] = Array("default")

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    val status = fs.listStatus(tempDir)
    status.filter(_.isDirectory).map { dir =>
      Identifier.of(namespace, dir.getPath.getName)
    }
  }

  override def tableExists(ident: Identifier): Boolean = {
    val tablePath = getTablePath(ident.name())
    fs.exists(tablePath)
  }

  override def loadTable(ident: Identifier): Table = {
    if (!tableExists(ident)) {
      throw new NoSuchTableException(ident)
    }
    val tablePath = getTablePath(ident.name())
    DeltaTableV2(spark = spark, path = tablePath, catalogTable = Some(createCatalogTable(ident)))
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    val tablePath = getTablePath(ident.name())
    // Create an empty Delta table on the tablePath
    val part = partitions.map(_.arguments().head.toString)
    spark.createDataFrame(List.empty[Row].asJava, schema)
      .write.format("delta").partitionBy(part: _*).save(tablePath.toString)
    DeltaTableV2(spark = spark, path = tablePath, catalogTable = Some(createCatalogTable(ident)))
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    // hack hack: no-op just for testing
    loadTable(ident)
  }

  override def dropTable(ident: Identifier): Boolean = {
    val tablePath = getTablePath(ident.name())
    try {
      fs.delete(tablePath, true)
      true
    } catch {
      case _: Exception => false
    }
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("Rename table operation is not supported.")
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    // Initialize tempDir here
    if (!fs.exists(tempDir)) {
      fs.mkdirs(tempDir)
    }
  }

  private def createCatalogTable(ident: Identifier): CatalogTable = {
    val tablePath = getTablePath(ident.name())
    CatalogTable(
      identifier = TableIdentifier(ident.name(), defaultNamespace.headOption, Some(name)),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat(Some(tablePath.toUri), None, None, None, false, Map.empty),
      schema = spark.range(0).schema
    )
  }
}

// A dummy catalog that adds additional table storage properties after the table is loaded.
// It's only used inside `DummySessionCatalog`.
class DummySessionCatalogInner extends DelegatingCatalogExtension {
  override def loadTable(ident: Identifier): Table = {
    val t = super.loadTable(ident).asInstanceOf[V1Table]
    V1Table(t.v1Table.copy(
      storage = t.v1Table.storage.copy(
        properties = t.v1Table.storage.properties ++ Map("fs.myKey" -> "val")
      )
    ))
  }
}

// A dummy catalog that adds a layer between DeltaCatalog and the Spark SessionCatalog,
// to attach additional table storage properties after the table is loaded, and generates location
// for managed tables.
class DummySessionCatalog extends TableCatalog {
  private var deltaCatalog: DeltaCatalog = null

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    val inner = new DummySessionCatalogInner()
    inner.setDelegateCatalog(new V2SessionCatalog(
      SparkSession.active.sessionState.catalogManager.v1SessionCatalog))
    deltaCatalog = new DeltaCatalog()
    deltaCatalog.setDelegateCatalog(inner)
  }

  override def name(): String = deltaCatalog.name()

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    deltaCatalog.listTables(namespace)
  }

  override def loadTable(ident: Identifier): Table = deltaCatalog.loadTable(ident)

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    if (!properties.containsKey(TableCatalog.PROP_EXTERNAL) &&
      !properties.containsKey(TableCatalog.PROP_LOCATION)) {
      val newProps = new java.util.HashMap[String, String]
      newProps.putAll(properties)
      newProps.put(TableCatalog.PROP_LOCATION, properties.get("fakeLoc"))
      newProps.put(TableCatalog.PROP_IS_MANAGED_LOCATION, "true")
      deltaCatalog.createTable(ident, schema, partitions, newProps)
    } else {
      deltaCatalog.createTable(ident, schema, partitions, properties)
    }
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    deltaCatalog.alterTable(ident, changes: _*)
  }

  override def dropTable(ident: Identifier): Boolean = deltaCatalog.dropTable(ident)

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    deltaCatalog.renameTable(oldIdent, newIdent)
  }
}

// This catalog always does a CASCADE on DROP SCHEMA ...
class DummyCatalogWithNamespace extends DummyCatalog with SupportsNamespaces {
  private val spark: SparkSession = SparkSession.active
  // To load a catalog into spark CatalogPlugin calls the Catalog's no-arg constructor and
  // then Catalog.initialize. To have a consistent state across different invocations
  // in the same test, this catalog impl uses a hard coded path.
  override lazy val tempDir: Path = DummyCatalogWithNamespace.catalogDir
  // scalastyle:off deltahadoopconfiguration
  override lazy val fs: FileSystem =
    tempDir.getFileSystem(spark.sessionState.newHadoopConf())
  // scalastyle:on deltahadoopconfiguration

  // Map each namespace to its metadata
  protected val namespaces: util.Map[List[String], Map[String, String]] =
    new util.HashMap[List[String], Map[String, String]]()

  protected val tables: mutable.Map[Array[String], mutable.HashSet[Identifier]] =
    new mutable.HashMap[Array[String], mutable.HashSet[Identifier]]()

  override def name: String = "test_catalog"

  override def getTablePath(tableName: String): Path = {
    new Path(s"${tempDir.toString}/$name.$tableName")
  }

  override def tableExists(ident: Identifier): Boolean = {
    val tablePath = getTablePath(ident.toString)
    fs.exists(tablePath)
  }

  override def loadTable(ident: Identifier): Table = {
    if (!tableExists(ident)) {
      throw new NoSuchTableException(ident)
    }
    val tablePath = getTablePath(ident.toString)
    DeltaTableV2(spark = spark, path = tablePath, catalogTable = Some(createCatalogTable(ident)))
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    val tablePath = getTablePath(ident.toString)
    // Create an empty Delta table on the tablePath
    val part = partitions.map(_.arguments().head.toString)
    spark.createDataFrame(List.empty[Row].asJava, schema)
      .write.format("delta").partitionBy(part: _*).save(tablePath.toString)
    val map = tables.getOrElseUpdate(ident.namespace(), new mutable.HashSet[Identifier]())
    map.add(ident)
    tables.put(ident.namespace(), map)
    DeltaTableV2(spark = spark, path = tablePath, catalogTable = Some(createCatalogTable(ident)))
  }

  override def dropTable(ident: Identifier): Boolean = {
    val tablePath = getTablePath(ident.toString)
    try {
      fs.delete(tablePath, true)
      true
    } catch {
      case _: Exception => false
    }
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    // Initialize tempDir here
    if (!fs.exists(tempDir)) {
      fs.mkdirs(tempDir)
    }
    fs.deleteOnExit(tempDir)
  }

  private def createCatalogTable(ident: Identifier): CatalogTable = {
    val tablePath = getTablePath(ident.toString)
    CatalogTable(
      identifier = TableIdentifier(ident.toString, defaultNamespace.headOption, Some(name)),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat(
        Some(tablePath.toUri), None, None, None, false, immutable.Map.empty),
      schema = spark.range(0).schema
    )
  }

  override def createNamespace(
      namespace: Array[String],
      metadata: util.Map[String, String]): Unit = {
    Option(namespaces.putIfAbsent(namespace.toList, metadata.asScala.toMap)) match {
      case Some(_) =>
        throw new NamespaceAlreadyExistsException(namespace)
      case _ =>
      // success
    }
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    throw new UnsupportedOperationException("alter namespace metadata is not supported.")
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    tables.getOrElse(namespace, mutable.HashSet.empty[Identifier]).foreach(dropTable)
    Option(namespaces.remove(namespace.toList)).isDefined
  }

  override def namespaceExists(namespace: Array[String]): Boolean = {
    namespaces.containsKey(namespace.toList)
  }

  override def listNamespaces(): Array[Array[String]] = {
    throw new UnsupportedOperationException("List namespaces operation is not supported.")
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    throw new UnsupportedOperationException("List namespaces operation is not supported.")
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    new util.HashMap[String, String]()
  }
}

object DummyCatalogWithNamespace {
  val catalogDir: Path = new Path(Utils.createTempDir().getAbsolutePath)
}
