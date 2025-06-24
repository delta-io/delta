package io.delta.dsv2.table

import scala.collection.JavaConverters._

import io.delta.dsv2.read.DeltaScanBuilder
import io.delta.dsv2.utils.SchemaUtils
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.exceptions.TableNotFoundException
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.util.VectorUtils

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DeltaTable(path: String)
    extends Table
    with SupportsRead {

  private lazy val kernelEngine = DefaultEngine.create(new Configuration())

  private lazy val kernelTable =
    io.delta.kernel.Table.forPath(kernelEngine, path)

  override def name(): String = s"delta.`$path`"

  override def schema(): StructType = {
    try {
      val schema = SchemaUtils.convertKernelSchemaToSparkSchema(
        kernelTable.getLatestSnapshot(kernelEngine).getSchema())
      schema
    } catch {
      case e: TableNotFoundException =>
        new StructType()
    }
  }

  override def capabilities(): java.util.Set[TableCapability] = {
    Set(TableCapability.BATCH_WRITE, TableCapability.BATCH_READ).asJava
  }

  override def partitioning(): Array[Transform] = {
    try {
      val partColNames = VectorUtils.toJavaList[String](
        kernelTable
          .getLatestSnapshot(kernelEngine)
          .asInstanceOf[SnapshotImpl]
          .getMetadata
          .getPartitionColumns)

      val result =
        partColNames.asScala.map(partColName => Expressions.identity(partColName)).toArray
      result
    } catch {
      case e: TableNotFoundException =>
        Array.empty
    }
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new DeltaScanBuilder(kernelTable, kernelEngine)
  }
}

object DeltaTable {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}
