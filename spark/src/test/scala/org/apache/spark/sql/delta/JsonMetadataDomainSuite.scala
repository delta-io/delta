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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}

class JsonMetadataDomainSuite extends QueryTest
  with SharedSparkSession  with DeltaSQLCommandTest  with SQLTestUtils {

  test("Validate json metadata domain") {
    val table = "test_table"
    withTable(table) {
      sql(
        s"""
           | CREATE TABLE $table(id int) USING delta
           | tblproperties
           | ('${TableFeatureProtocolUtils.propertyKey(DomainMetadataTableFeature)}' = 'enabled')
           |""".stripMargin)

      val metadataDomain = TestMetadataDomain("key", 1000L, "test1" :: "value1" :: Nil)
      val domainMetadata = metadataDomain.toDomainMetadata
      assert(domainMetadata.configuration === JsonUtils.toJson[TestMetadataDomain](metadataDomain))
      assert(domainMetadata.domain == metadataDomain.domainName)
      assert(!domainMetadata.removed)

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
      deltaLog.startTransaction().commit(domainMetadata :: Nil, ManualUpdate)

      assert(TestMetadataDomain.fromSnapshot(deltaLog.update()).contains(metadataDomain))
    }

  }
}

case class TestMetadataDomain(
    key1: String,
    val1: Long,
    array: Seq[String]) extends JsonMetadataDomain[TestMetadataDomain] {
  override val domainName: String = TestMetadataDomain.domainName
}

object TestMetadataDomain extends JsonMetadataDomainUtils[TestMetadataDomain] {
  override protected val domainName = "delta.test.testDomain"
}
