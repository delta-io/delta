package io.delta.dsv2.catalog

import java.util
import scala.collection.JavaConverters._
import io.delta.dsv2.catalog.DeltaCatalog.logger
import io.delta.dsv2.table.DeltaTable
import io.delta.dsv2.utils.SchemaUtils
import io.delta.kernel.Operation
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.exceptions.TableNotFoundException
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.{NamedReference, Transform}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DeltaCatalog extends TableCatalog {

  private var catalogName: String = _
  private lazy val engine = DefaultEngine.create(new Configuration())

  def tableIdentifierToPath(ident: Identifier): String = {
    s"/tmp/spark_warehouse/${ident.name()}"
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    logger.info(s"Scott > DeltaCatalog > initialize :: name=$name, options=$options")
    this.catalogName = name
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    Array.empty
  }

  override def loadTable(ident: Identifier): Table = {
    try {
      new DeltaTable(tableIdentifierToPath(ident))
    } catch {
      case _: TableNotFoundException =>
        logger.info(s"Scott > DeltaCatalog > loadTable :: ident=$ident, table does not exist")
        throw new NoSuchTableException(ident)
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val path = if (properties.containsKey("path")) {
      properties.get("path")
    } else {
      tableIdentifierToPath(ident)
    }

    logger.info(
      s"createTable: ident=$ident, schema=$schema, " +
        s"partitions=${partitions.mkString("Array(", ", ", ")")}, properties=$properties, " +
        s"path=$path")

    val partitionCols = partitions.map(extractPartitionColumn)

    val result = io.delta.kernel.Table
      .forPath(engine, path)
      .createTransactionBuilder(engine, "kernel-spark-dsv2", Operation.CREATE_TABLE)
      .withSchema(engine, SchemaUtils.convertSparkSchemaToKernelSchema(schema))
      .withPartitionColumns(engine, partitionCols.toList.asJava)
      .withTableProperties(
        engine,
        properties.asScala
          .filter(entry => entry._1.startsWith("delta."))
          .toMap
          .asJava)
      .build(engine)
      .commit(engine, io.delta.kernel.utils.CloseableIterable.emptyIterable())

    logger.info(s"createTable: resultVersion=${result.getVersion}")

    val table = new DeltaTable(path)
    table
  }

  override def dropTable(ident: Identifier): Boolean = {
    true
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("renameTable is not supported")
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("alterTable is not supported")
  }

  override def tableExists(ident: Identifier): Boolean = {
    try {
      new DeltaTable(tableIdentifierToPath(ident))
      true
    } catch {
      case _: TableNotFoundException =>
       false
    }
  }

  override def name(): String = catalogName

  private def extractPartitionColumn(transform: Transform): String = {
    logger.info(s"transform: $transform")
    // Check if the transform is an identity transform
    if (transform.name() == "identity" && transform.references().nonEmpty) {
      // Get the first reference, which should be the column
      transform.references()(0) match {
        case namedRef: NamedReference =>
          logger.info(s"namedRef: $namedRef")
          logger.info(
            s"namedRef.fieldNames: ${namedRef.fieldNames().mkString("Array(", ", ", ")")}")
          namedRef.fieldNames().mkString(".")
        case _ => throw new RuntimeException("bad aa")
      }
    } else {
      throw new RuntimeException("bad bb")
    }
  }
}

object DeltaCatalog {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}
