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

package org.apache.spark.sql.delta.hooks

import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaTableIdentifier, OptimisticTransactionImpl, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, Metadata}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.threads.DeltaThreadPool
import org.apache.commons.lang3.exception.ExceptionUtils

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.ThreadUtils

/**
 * Factory object to create an UpdateCatalog post commit hook. This should always be used
 * instead of directly creating a specific hook.
 */
object UpdateCatalogFactory {
  def getUpdateCatalogHook(table: CatalogTable, spark: SparkSession): UpdateCatalogBase = {
    UpdateCatalog(table)
  }
}

/**
 * Base trait for post commit hooks that want to update the catalog with the
 * latest table schema and properties.
 */
trait UpdateCatalogBase extends PostCommitHook with DeltaLogging {

  protected val table: CatalogTable

  override def run(
      spark: SparkSession,
      txn: OptimisticTransactionImpl,
      committedVersion: Long,
      postCommitSnapshot: Snapshot,
      actions: Seq[Action]): Unit = {
    // There's a potential race condition here, where a newer commit has already triggered
    // this to run. That's fine.
    executeOnWrite(spark, postCommitSnapshot)
  }

  /**
   * Used to manually execute an UpdateCatalog hook during a write.
   */
  def executeOnWrite(
    spark: SparkSession,
    snapshot: Snapshot
    ): Unit


  /**
   * Update the schema in the catalog based on the provided snapshot.
   */
  def updateSchema(spark: SparkSession, snapshot: Snapshot): Unit

  /**
   * Update the properties in the catalog based on the provided snapshot.
   */
  protected def updateProperties(spark: SparkSession, snapshot: Snapshot): Unit

  /**
   * Checks if the table schema has changed in the Snapshot with respect to what's stored in
   * the catalog.
   */
  protected def schemaHasChanged(snapshot: Snapshot, spark: SparkSession): Boolean

  /**
   * Checks if the table properties have changed in the Snapshot with respect to what's stored in
   * the catalog.
   *
   * Visible for testing.
   */
  protected[sql] def propertiesHaveChanged(
    properties: Map[String, String],
    metadata: Metadata,
    spark: SparkSession): Boolean

  protected def shouldRun(
      spark: SparkSession,
      snapshot: Snapshot
      ): Boolean = {
    // Do not execute for path based tables, because they don't exist in the MetaStore
    if (isPathBasedDeltaTable(table, spark)) return false
    // Only execute if this is a Delta table
    if (snapshot.version < 0) return false
    true
  }

  private def isPathBasedDeltaTable(table: CatalogTable, spark: SparkSession): Boolean = {
    return DeltaTableIdentifier.isDeltaPath(spark, table.identifier)
  }


  /** Update the entry in the Catalog to reflect the latest schema and table properties. */
  protected def execute(
      spark: SparkSession,
      snapshot: Snapshot): Unit = {
    recordDeltaOperation(snapshot.deltaLog, "delta.catalog.update") {
      val properties = snapshot.getProperties.toMap
      val v = table.properties.get(DeltaConfigs.METASTORE_LAST_UPDATE_VERSION)
        .flatMap(v => Try(v.toLong).toOption)
        .getOrElse(-1L)
      val lastCommitTimestamp = table.properties.get(DeltaConfigs.METASTORE_LAST_COMMIT_TIMESTAMP)
        .flatMap(v => Try(v.toLong).toOption)
        .getOrElse(-1L)
      // If the metastore entry is at an older version and not the timestamp of that version, e.g.
      // a table can be rm -rf'd and get the same version number with a different timestamp
      if (v <= snapshot.version || lastCommitTimestamp < snapshot.timestamp) {
        try {
          val loggingData = Map(
            "identifier" -> table.identifier,
            "snapshotVersion" -> snapshot.version,
            "snapshotTimestamp" -> snapshot.timestamp,
            "catalogVersion" -> v,
            "catalogTimestamp" -> lastCommitTimestamp
          )
          if (schemaHasChanged(snapshot, spark)) {
            updateSchema(spark, snapshot)
            recordDeltaEvent(
              snapshot.deltaLog,
              "delta.catalog.update.schema",
              data = loggingData
            )
          } else if (propertiesHaveChanged(properties, snapshot.metadata, spark)) {
            updateProperties(spark, snapshot)
            recordDeltaEvent(
              snapshot.deltaLog,
              "delta.catalog.update.properties",
              data = loggingData
            )
          }
        } catch {
          case NonFatal(e) =>
            recordDeltaEvent(
              snapshot.deltaLog,
              "delta.catalog.update.error",
              data = Map(
                "exceptionMsg" -> ExceptionUtils.getMessage(e),
                "stackTrace" -> ExceptionUtils.getStackTrace(e))
            )
            logWarning(s"Failed to update the catalog for ${table.identifier} with the latest " +
              s"table information.", e)
        }
      }
    }
  }
}

/**
 * A post-commit hook that allows us to cache the most recent schema and table properties of a Delta
 * table in an External Catalog. In addition to the schema and table properties, we also store the
 * last commit timestamp and version for which we updated the catalog. This prevents us from
 * updating the MetaStore with potentially stale information.
 */
case class UpdateCatalog(table: CatalogTable) extends UpdateCatalogBase {

  override val name: String = "Update Catalog"

  override def executeOnWrite(
      spark: SparkSession,
      snapshot: Snapshot
     ): Unit = {
    executeAsync(spark, snapshot)
  }


  override protected def schemaHasChanged(snapshot: Snapshot, spark: SparkSession): Boolean = {
    // We need to check whether the schema in the catalog matches the current schema. If a
    // field in the schema is very long, we cannot store the schema in the catalog, therefore
    // here we have to compare what's in the catalog with what we actually can store in the
    // catalog
    val schemaChanged = UpdateCatalog.truncateSchemaIfNecessary(snapshot.schema) != table.schema
    // The table may have been dropped as we're just about to update the information. There is
    // unfortunately no great way to avoid a race condition, but we do one last check here as
    // updates may have been queued for some time.
    schemaChanged && spark.sessionState.catalog.tableExists(table.identifier)
  }

  /**
   * Checks if the table properties have changed in the Snapshot with respect to what's stored in
   * the catalog. We check to see if our table properties are a subset of what is in the MetaStore
   * to avoid flip-flopping the information between older and newer versions of Delta. The
   * assumption here is that newer Delta releases will only add newer table properties and not
   * remove them.
   */
  override protected[sql] def propertiesHaveChanged(
      properties: Map[String, String],
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    val propertiesChanged = !properties.forall { case (k, v) =>
      table.properties.get(k) == Some(v)
    }
    // The table may have been dropped as we're just about to update the information. There is
    // unfortunately no great way to avoid a race condition, but we do one last check here as
    // updates may have been queued for some time.
    propertiesChanged && spark.sessionState.catalog.tableExists(table.identifier)
  }

  override def updateSchema(spark: SparkSession, snapshot: Snapshot): Unit = {
    UpdateCatalog.replaceTable(spark, snapshot, table)
  }

  override protected def updateProperties(spark: SparkSession, snapshot: Snapshot): Unit = {
    spark.sessionState.catalog.alterTable(
      table.copy(properties = UpdateCatalog.updatedProperties(snapshot)))
  }

  /**
   * Update the entry in the Catalog to reflect the latest schema and table properties
   * asynchronously.
   */
  private def executeAsync(
      spark: SparkSession,
      snapshot: Snapshot): Unit = {
    if (!shouldRun(spark, snapshot)) return
    Future[Unit] {
      UpdateCatalog.activeAsyncRequests.incrementAndGet()
      execute(spark, snapshot)
    }(UpdateCatalog.getOrCreateExecutionContext(spark.sessionState.conf)).onComplete { _ =>
      UpdateCatalog.activeAsyncRequests.decrementAndGet()
    }(UpdateCatalog.getOrCreateExecutionContext(spark.sessionState.conf))
  }
}

object UpdateCatalog {
  private var tp: ExecutionContext = _

  // This is the encoding of the database for the Hive MetaStore
  private val latin1 = Charset.forName("ISO-8859-1")

  // Maximum number of characters that a catalog can store.
  val MAX_CATALOG_TYPE_DDL_LENGTH = 4000
  val ERROR_KEY = "delta.catalogUpdateError"
  val LONG_SCHEMA_ERROR: String = "The schema contains a very long nested field and cannot be " +
    "stored in the catalog."
  val HIVE_METASTORE_NAME = "hive_metastore"

  private def getOrCreateExecutionContext(conf: SQLConf): ExecutionContext = synchronized {
    if (tp == null) {
      tp = ExecutionContext.fromExecutorService(DeltaThreadPool.newDaemonCachedThreadPool(
        "delta-catalog-update",
        conf.getConf(DeltaSQLConf.DELTA_UPDATE_CATALOG_THREAD_POOL_SIZE)
        )
      )
    }
    tp
  }

  /** Keeps track of active or queued async requests. */
  private val activeAsyncRequests = new AtomicInteger(0)

  /**
   * Waits for all active and queued updates to finish until the given timeout. Will return true
   * if all async threads have completed execution. Will return false if not. Exposed for tests.
   */
  def awaitCompletion(timeoutMillis: Long): Boolean = {
    try {
      ThreadUtils.runInNewThread("UpdateCatalog-awaitCompletion") {
        val startTime = System.currentTimeMillis()
        while (activeAsyncRequests.get() > 0) {
          Thread.sleep(100)
          val currentTime = System.currentTimeMillis()
          if (currentTime - startTime > timeoutMillis) {
            throw new TimeoutException(
              s"Timed out waiting for catalog updates to complete after $currentTime ms")
          }
        }
      }
      true
    } catch {
      case _: TimeoutException =>
        false
    }
  }

  /** Replace the table definition in the MetaStore. */
  private def replaceTable(spark: SparkSession, snapshot: Snapshot, table: CatalogTable): Unit = {
    val catalog = spark.sessionState.catalog
    val qualifiedIdentifier =
      catalog.qualifyIdentifier(TableIdentifier(table.identifier.table, Some(table.database)))
    val db = qualifiedIdentifier.database.get
    val tblName = qualifiedIdentifier.table
    val schema = truncateSchemaIfNecessary(snapshot.schema)
    val additionalProperties = if (schema.isEmpty) {
      Map(ERROR_KEY -> LONG_SCHEMA_ERROR)
    } else {
      Map.empty
    }

    // We call the lower level API so that we can actually drop columns. We also assume that
    // all columns are data columns so that we don't have to deal with partition columns
    // having to be at the end of the schema, which Hive follows.
    val catalogName = table.identifier.catalog.getOrElse(
      spark.sessionState.catalogManager.currentCatalog.name())
    if (
      (catalogName == UpdateCatalog.HIVE_METASTORE_NAME
        || catalogName == SESSION_CATALOG_NAME) &&
      catalog.externalCatalog.tableExists(db, tblName)) {
      catalog.externalCatalog.alterTableDataSchema(db, tblName, schema)
    }

    // We have to update the properties anyway with the latest version/timestamp information
    catalog.alterTable(table.copy(properties = updatedProperties(snapshot) ++ additionalProperties))
  }

  /** Updates our properties map with the version and timestamp information of the snapshot. */
  def updatedProperties(snapshot: Snapshot): Map[String, String] = {
    var newProperties =
      snapshot.getProperties.toMap ++ Map(
        DeltaConfigs.METASTORE_LAST_UPDATE_VERSION -> snapshot.version.toString,
        DeltaConfigs.METASTORE_LAST_COMMIT_TIMESTAMP -> snapshot.timestamp.toString)
    newProperties
  }

  /**
   * If a field in the schema has a very long string representation, then the schema will be
   * truncated to an empty schema to avoid corruption.
   * Also, if the schema contains non-latin encoding characters, the schema will be garbled. In
   * this case we also truncate the schema.
   */
  def truncateSchemaIfNecessary(schema: StructType): StructType = {
    // Encoders are not threadsafe
    val encoder = latin1.newEncoder()
    def isColumnValid(f: StructField): Boolean = {
      val typeString = f.dataType.catalogString
        encoder.canEncode(f.name) &&
        typeString.length <= MAX_CATALOG_TYPE_DDL_LENGTH &&
        encoder.canEncode(typeString)
    }

    if (schema.exists(f => !isColumnValid(f))) {
      new StructType()
    } else {
      schema
    }
  }
}
