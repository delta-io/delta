package io.delta.dsv2.catalog

import java.util
import scala.collection.JavaConverters._
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
    assert(ident.namespace().length == 1 && ident.namespace().head == "delta")
    ident.name()
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    Array.empty
  }

  override def loadTable(ident: Identifier): Table = {
    try {
      val table = new DeltaTable(tableIdentifierToPath(ident))
      table.schema()
      table
    } catch {
      case _: TableNotFoundException =>
        throw new NoSuchTableException(ident)
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val path = tableIdentifierToPath(ident)

    if (partitions.length > 0) {
      throw new UnsupportedOperationException("partition table is not supported")
    }

    val result = io.delta.kernel.Table
      .forPath(engine, path)
      .createTransactionBuilder(engine, "kernel-spark-dsv2", Operation.CREATE_TABLE)
      .withSchema(engine, SchemaUtils.convertSparkSchemaToKernelSchema(schema))
      .withPartitionColumns(engine, new util.ArrayList[String]())
      .withTableProperties(
        engine,
        properties.asScala
          .filter(entry => entry._1.startsWith("delta."))
          .toMap
          .asJava)
      .build(engine)
      .commit(engine, io.delta.kernel.utils.CloseableIterable.emptyIterable())
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
      val table = new DeltaTable(tableIdentifierToPath(ident))
      table.schema()
      true
    } catch {
      case _: TableNotFoundException =>
        false
    }
  }

  override def name(): String = catalogName
}
