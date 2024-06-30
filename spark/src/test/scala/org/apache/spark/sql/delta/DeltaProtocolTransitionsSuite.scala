package org.apache.spark.sql.delta

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

import org.apache.spark.sql.delta.test.DeltaTestImplicits._

class DeltaProtocolTransitionsSuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  protected def protocolToTBLProperties(protocol: Protocol): Seq[String] = {
    val versionProperties =
      s"""
         |delta.minReaderVersion = ${protocol.minReaderVersion},
         |delta.minWriterVersion = ${protocol.minWriterVersion}""".stripMargin
    val featureProperties =
      protocol.readerAndWriterFeatureNames.map("delta.feature." + _ + " = 'Supported'")
    Seq(versionProperties) ++ featureProperties
  }
  protected def testProtocolTransition(
     createTableProtocol: Option[Protocol] = None,
     alterTableProtocol: Option[Protocol] = None,
     dropFeatures: Seq[TableFeature] = Nil,
     expectedProtocol: Protocol): Unit = {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)

      val createTableSQLbase = s"CREATE TABLE delta.`${deltaLog.dataPath}` (id bigint) USING delta"
      val createTableSQL = createTableProtocol.map { p =>
        s"""$createTableSQLbase TBLPROPERTIES (
           |${protocolToTBLProperties(p).mkString(",")}
           |)""".stripMargin
      }.getOrElse(createTableSQLbase)

      // Create table.
      sql(createTableSQL)

      alterTableProtocol.map { p =>
        sql(s"""ALTER TABLE delta.`${deltaLog.dataPath}` SET TBLPROPERTIES (
           |${protocolToTBLProperties(p).mkString(",")}
           |)""".stripMargin)
      }

      // Drop features.
      dropFeatures.foreach { f =>
        sql(s"ALTER TABLE delta.`${deltaLog.dataPath}` DROP FEATURE $f")
      }

      assert(deltaLog.update().protocol === expectedProtocol)
    }
  }

  test("CREATE TABLE normalization") {
    testProtocolTransition(
      createTableProtocol = Some(Protocol(3, 7).withFeature(TestRemovableWriterFeature)),
      expectedProtocol = Protocol(1, 7).withFeature(TestRemovableWriterFeature))

    testProtocolTransition(
      createTableProtocol = Some(
        Protocol(3, 7).withFeatures(Seq(TestRemovableWriterFeature, ColumnMappingTableFeature))),
      expectedProtocol =
        Protocol(2, 7).withFeatures(Seq(TestRemovableWriterFeature, ColumnMappingTableFeature)))

    /*
    testProtocolTransition(
      createTableProtocol = Some(Protocol(1, 7).withFeature(TestRemovableReaderWriterFeature)),
      expectedProtocol = Protocol(3, 7).withFeature(TestRemovableReaderWriterFeature))

    testProtocolTransition(
      createTableProtocol = Some(Protocol(2, 7).withFeature(TestRemovableReaderWriterFeature)),
      expectedProtocol = Protocol(3, 7).withFeature(TestRemovableReaderWriterFeature))
    */
  }
}