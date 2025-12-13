package io.delta.flink.table

import io.delta.flink.TestHelper

import java.net.URI
import java.util.{Collections, Optional}
import scala.jdk.CollectionConverters.{IterableHasAsJava, MapHasAsJava}
import io.delta.kernel.Transaction
import io.delta.kernel.TransactionSuite.longVector
import io.delta.kernel.data.{ColumnVector, FilteredColumnarBatch}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.types.{DataType, IntegerType, StructType}
import io.delta.kernel.utils.CloseableIterable
import org.scalatest.funsuite.AnyFunSuite

class CCv2TableSuite extends AnyFunSuite with TestHelper {

  val CATALOG_ENDPOINT = "https://e2-dogfood.staging.cloud.databricks.com/"
  val CATALOG_TOKEN = ""

  test("load table from e2dogfood") {
    val table = new CCv2Table(
      new RESTCatalog(CATALOG_ENDPOINT, CATALOG_TOKEN),
      "main.hao.testccv2",
      Map(
        CCv2Table.CATALOG_ENDPOINT -> CATALOG_ENDPOINT,
        CCv2Table.CATALOG_TOKEN -> CATALOG_TOKEN).asJava)

    assert(table.getId == "main.hao.testccv2")
    assert(table.getTablePath == URI.create("s3://us-west-2-extstaging-managed-" +
      "catalog-test-bucket-1/" +
      "19a85dee-54bc-43a2-87ab-023d0ec16013/tables/b7c3e881-4f7f-40f2-88c1-dff715835a81/"))
    assert(table.getSchema.equivalent(new StructType().add("id", IntegerType.INTEGER)))
  }

  test("commit data to e2dogfood") {
    val table = new CCv2Table(
      new RESTCatalog(CATALOG_ENDPOINT, CATALOG_TOKEN),
      "main.hao.testccv2",
      Map(
        CCv2Table.CATALOG_ENDPOINT -> CATALOG_ENDPOINT,
        CCv2Table.CATALOG_TOKEN -> CATALOG_TOKEN).asJava)

    val values = (0 until 100)
    val colVector = new ColumnVector() {
      override def getDataType: DataType = IntegerType.INTEGER
      override def getSize: Int = values.length
      override def close(): Unit = {}
      override def isNullAt(rowId: Int): Boolean = values(rowId) == null
      override def getInt(rowId: Int): Int = values(rowId)
    }

    val columnarBatchData =
      new DefaultColumnarBatch(values.size, table.getSchema, Array(colVector))
    val filteredColumnarBatchData = new FilteredColumnarBatch(columnarBatchData, Optional.empty())
    val partitionValues = Collections.emptyMap[String, Literal]()

    val data = toCloseableIterator(Seq(filteredColumnarBatchData).asJava.iterator())
    val rows = table.writeParquet("abc", data, partitionValues)

    table.commit(CloseableIterable.inMemoryIterable(rows))
  }

  test("serializablity") {
    val table = new CCv2Table(
      new RESTCatalog(CATALOG_ENDPOINT, CATALOG_TOKEN),
      "main.hao.testccv2",
      Map(
        CCv2Table.CATALOG_ENDPOINT -> CATALOG_ENDPOINT,
        CCv2Table.CATALOG_TOKEN -> CATALOG_TOKEN).asJava)

    checkSerializability(table)
  }
}
