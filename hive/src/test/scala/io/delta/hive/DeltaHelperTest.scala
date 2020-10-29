package io.delta.hive

import scala.collection.JavaConverters._

import io.delta.standalone.types._
import org.apache.hadoop.hive.metastore.api.MetaException
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport
import org.apache.hadoop.hive.serde2.typeinfo.{StructTypeInfo, TypeInfoFactory}

import org.apache.spark.SparkFunSuite

class DeltaHelperTest extends SparkFunSuite {

  test("DeltaHelper checkTableSchema correct") {
    // scalastyle:off
    val colNames = DataWritableReadSupport.getColumnNames("c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15")
    val colTypeInfos = DataWritableReadSupport.getColumnTypes("tinyint:binary:boolean:int:bigint:string:float:double:smallint:date:timestamp:decimal(38,18):array<string>:map<string,bigint>:struct<f1:string,f2:bigint>")
    // scalastyle:on
    val colDataTypes = Array(new ByteType, new BinaryType, new BooleanType, new IntegerType,
      new LongType, new StringType, new FloatType, new DoubleType, new ShortType, new DateType,
      new TimestampType, new DecimalType(38, 18), new ArrayType(new StringType, false),
      new MapType(new StringType, new LongType, false),
      new StructType(
        Array(new StructField("f1", new StringType), new StructField("f2", new LongType))))

    assert(colNames.size() == colTypeInfos.size() && colNames.size() == colDataTypes.size)

    val hiveSchema = TypeInfoFactory
      .getStructTypeInfo(colNames, colTypeInfos)
      .asInstanceOf[StructTypeInfo]

    val fields = colNames.asScala.zip(colDataTypes).map {
      case (name, dataType) => new StructField(name, dataType)
    }.toArray

    val standaloneSchema = new StructType(fields)

    DeltaHelper.checkTableSchema(standaloneSchema, hiveSchema)
  }

  test("DeltaHelper checkTableSchema incorrect throws") {
    val fields = Array(
      new StructField("c1", new IntegerType),
      new StructField("c2", new StringType))
    val standaloneSchema = new StructType(fields)

    def createHiveSchema(colNamesStr: String, colTypesStr: String): StructTypeInfo = {
      val colNames = DataWritableReadSupport.getColumnNames(colNamesStr)
      val colTypeInfos = DataWritableReadSupport.getColumnTypes(colTypesStr)

      TypeInfoFactory
        .getStructTypeInfo(colNames, colTypeInfos)
        .asInstanceOf[StructTypeInfo]
    }

    def assertSchemaException(hiveSchema: StructTypeInfo, exMsg: String): Unit = {
      val e = intercept[MetaException] {
        DeltaHelper.checkTableSchema(standaloneSchema, hiveSchema)
      }
      assert(e.getMessage.contains("The Delta table schema is not the same as the Hive schema"))
      assert(e.getMessage.contains(exMsg))
    }

    // column number mismatch (additional field)
    val hiveSchema1 = createHiveSchema("c1,c2,c3", "int:string:boolean")
    assertSchemaException(hiveSchema1, "Specified schema has additional field(s): c3")

    // column name mismatch (mising field)
    val hiveSchema2 = createHiveSchema("c1,c3", "int:string")
    assertSchemaException(hiveSchema2, "Specified schema is missing field(s): c2")

    // column order mismatch
    val hiveSchema3 = createHiveSchema("c2,c1", "string:int")
    assertSchemaException(hiveSchema3, "Columns out of order")

    // column type mismatch
    val hiveSchema4 = createHiveSchema("c1,c2", "int:tinyint")
    assertSchemaException(hiveSchema4, "Specified type for c2 is different from existing schema")
  }
}
