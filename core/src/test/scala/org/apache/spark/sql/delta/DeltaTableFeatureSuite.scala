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

package org.apache.spark.sql.delta

import java.io.File

import scala.collection.mutable

import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils._
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames.deltaFile

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class DeltaTableFeatureSuite
  extends QueryTest
  with SharedSparkSession  with DeltaSQLCommandTest {

  private lazy val testTableSchema = spark.range(1).schema

  // This is solely a test hook. Users cannot create new Delta tables with protocol lower than
  // that of their current version.
  protected def createTableWithProtocol(
      protocol: Protocol,
      path: File,
      schema: StructType = testTableSchema): DeltaLog = {
    val log = DeltaLog.forTable(spark, path)
    log.ensureLogDirectoryExist()
    log.store.write(
      deltaFile(log.logPath, 0),
      Iterator(Metadata(schemaString = schema.json).json, protocol.json),
      overwrite = false,
      log.newDeltaHadoopConf())
    log.update()
    log
  }

  test("all defined table features are registered") {
    import scala.reflect.runtime.{universe => ru}

    val subClassNames = mutable.Set[String]()
    def collect(clazz: ru.Symbol): Unit = {
      val collected = clazz.asClass.knownDirectSubclasses
      // add only table feature objects to the result set
      subClassNames ++= collected.filter(_.isModuleClass).map(_.name.toString)
      collected.filter(_.isAbstract).foreach(collect)
    }
    collect(ru.typeOf[TableFeature].typeSymbol)

    val registeredFeatures = TableFeature.allSupportedFeaturesMap.values
      .map(_.getClass.getSimpleName.stripSuffix("$")) // remove '$' from object names
      .toSet
    val notRegisteredFeatures = subClassNames.diff(registeredFeatures)

    assert(
      notRegisteredFeatures.isEmpty,
      "Expecting all defined table features are registered (either as prod or testing-only) " +
        s"but the followings are not: $notRegisteredFeatures")
  }

  test("adding feature requires supported protocol version") {
    assert(
      intercept[DeltaTableFeatureException] {
        Protocol(1, TABLE_FEATURES_MIN_WRITER_VERSION)
          .withFeature(TestLegacyReaderWriterFeature)
      }.getMessage.contains("Unable to enable table feature testLegacyReaderWriter because it " +
        "requires a higher reader protocol version"))

    assert(intercept[DeltaTableFeatureException] {
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, 6)
    }.getMessage.contains("Unable to upgrade only the reader protocol version"))

    assert(
      Protocol(2, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(AppendOnlyTableFeature)
        .readerAndWriterFeatureNames === Set(AppendOnlyTableFeature.name))

    assert(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterFeature)
        .readerAndWriterFeatureNames === Set(TestReaderWriterFeature.name))
  }

  test("implicitly-enabled features") {
    assert(
      Protocol(2, 6).implicitlyEnabledFeatures === Set(
        AppendOnlyTableFeature,
        ColumnMappingTableFeature,
        InvariantsTableFeature,
        CheckConstraintsTableFeature,
        ChangeDataFeedTableFeature,
        GeneratedColumnsTableFeature,
        IdentityColumnsTableFeature,
        TestLegacyWriterFeature,
        TestLegacyReaderWriterFeature))
    assert(
      Protocol(2, 5).implicitlyEnabledFeatures === Set(
        AppendOnlyTableFeature,
        ColumnMappingTableFeature,
        InvariantsTableFeature,
        CheckConstraintsTableFeature,
        ChangeDataFeedTableFeature,
        GeneratedColumnsTableFeature,
        TestLegacyWriterFeature))
    assert(Protocol(2, TABLE_FEATURES_MIN_WRITER_VERSION).implicitlyEnabledFeatures === Set())
    assert(
      Protocol(
        TABLE_FEATURES_MIN_READER_VERSION,
        TABLE_FEATURES_MIN_WRITER_VERSION).implicitlyEnabledFeatures === Set())
  }

  test("implicit feature listing") {
    assert(
      intercept[DeltaTableFeatureException] {
        Protocol(1, 5).withFeature(TestLegacyReaderWriterFeature)
      }.getMessage.contains(
        "Unable to enable table feature testLegacyReaderWriter because it requires a higher " +
          "reader protocol version (current 1)"))

    assert(
      intercept[DeltaTableFeatureException] {
        Protocol(2, 5).withFeature(TestLegacyReaderWriterFeature)
      }.getMessage.contains(
        "Unable to enable table feature testLegacyReaderWriter because it requires a higher " +
          "writer protocol version (current 5)"))

    assert(
      intercept[DeltaTableFeatureException] {
        Protocol(1, TABLE_FEATURES_MIN_WRITER_VERSION).withFeature(TestLegacyReaderWriterFeature)
      }.getMessage.contains(
        "Unable to enable table feature testLegacyReaderWriter because it requires a higher " +
          "reader protocol version (current 1)"))

    val protocol =
      Protocol(2, TABLE_FEATURES_MIN_WRITER_VERSION).withFeature(TestLegacyReaderWriterFeature)
    assert(!protocol.readerFeatures.isDefined)
    assert(
      protocol.writerFeatures.get === Set(TestLegacyReaderWriterFeature.name))
  }

  test("merge protocols") {
    val tfProtocol1 = Protocol(1, TABLE_FEATURES_MIN_WRITER_VERSION)
    val tfProtocol2 =
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)

    assert(
      tfProtocol1.merge(Protocol(1, 2)) ===
        tfProtocol1.withFeatures(Seq(AppendOnlyTableFeature, InvariantsTableFeature)))
    assert(
      tfProtocol2.merge(Protocol(2, 6)) ===
        tfProtocol2.withFeatures(Set(
          AppendOnlyTableFeature,
          InvariantsTableFeature,
          ColumnMappingTableFeature,
          ChangeDataFeedTableFeature,
          CheckConstraintsTableFeature,
          GeneratedColumnsTableFeature,
          IdentityColumnsTableFeature,
          TestLegacyWriterFeature,
          TestLegacyReaderWriterFeature)))
  }

  test("protocol upgrade compatibility") {
    assert(Protocol(1, 1).canUpgradeTo(Protocol(1, 1)))
    assert(Protocol(1, 1).canUpgradeTo(Protocol(2, 1)))
    assert(!Protocol(1, 2).canUpgradeTo(Protocol(1, 1)))
    assert(!Protocol(2, 2).canUpgradeTo(Protocol(2, 1)))
    assert(
      Protocol(1, 1).canUpgradeTo(
        Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)))
    assert(
      !Protocol(2, 3).canUpgradeTo(
        Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)))
    assert(
      !Protocol(2, 6).canUpgradeTo(
        Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
          .withFeatures(
            Seq(
              // With one feature not referenced, `canUpgradeTo` must be `false`.
              // AppendOnlyTableFeature,
              InvariantsTableFeature,
              CheckConstraintsTableFeature,
              ChangeDataFeedTableFeature,
              GeneratedColumnsTableFeature,
              ColumnMappingTableFeature,
              IdentityColumnsTableFeature,
              TestLegacyWriterFeature,
              TestLegacyReaderWriterFeature))))
    assert(
      Protocol(2, 6).canUpgradeTo(
        Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
          .withFeatures(Seq(
            AppendOnlyTableFeature,
            InvariantsTableFeature,
            CheckConstraintsTableFeature,
            ChangeDataFeedTableFeature,
            GeneratedColumnsTableFeature,
            ColumnMappingTableFeature,
            IdentityColumnsTableFeature,
            TestLegacyWriterFeature,
            TestLegacyReaderWriterFeature))))
    // Features are identical but protocol versions are lower, thus `canUpgradeTo` is `false`.
    assert(
      !Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .canUpgradeTo(Protocol(1, 1)))
  }

  test("add reader and writer feature descriptors") {
    var p = Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
    val name = AppendOnlyTableFeature.name
    p = p.withReaderFeatures(Seq(name))
    assert(p.readerFeatures === Some(Set(name)))
    assert(p.writerFeatures === Some(Set.empty))
    p = p.withWriterFeatures(Seq(name))
    assert(p.readerFeatures === Some(Set(name)))
    assert(p.writerFeatures === Some(Set(name)))
  }

  test("native automatically-enabled feature can't be implicitly enabled") {
    val p = Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
    assert(p.implicitlyEnabledFeatures.isEmpty)
  }
}
