/*
 * Copyright (2025) The Delta Lake Project Authors.
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

class DeltaVariantSuite
  extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest
    with DeltaSQLTestUtils
    with TestsStatistics {

  import testImplicits._

  test("variant shredding table property") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING, i INTEGER) USING DELTA")
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("tbl"))
      assert(!deltaLog.unsafeVolatileSnapshot.protocol
        .isFeatureSupported(VariantShreddingPreviewTableFeature),
        s"Table tbl contains ShreddedVariantTableFeature descriptor when its not supposed to"
      )
      sql(s"ALTER TABLE tbl " +
        s"SET TBLPROPERTIES('${DeltaConfigs.ENABLE_VARIANT_SHREDDING.key}' = 'true')")
      assert(getProtocolForTable("tbl")
        .readerAndWriterFeatures.contains(VariantShreddingPreviewTableFeature))
    }
    withTable("tbl") {
      sql(s"CREATE TABLE tbl(s STRING, i INTEGER) USING DELTA " +
        s"TBLPROPERTIES('${DeltaConfigs.ENABLE_VARIANT_SHREDDING.key}' = 'true')")
      assert(getProtocolForTable("tbl")
        .readerAndWriterFeatures.contains(VariantShreddingPreviewTableFeature))
    }
    assert(DeltaConfigs.ENABLE_VARIANT_SHREDDING.key == "delta.enableVariantShredding")
  }
}
