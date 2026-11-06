/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.catalog.v2

import java.lang.reflect.Method

import org.apache.spark.sql.delta.DeltaAnalysisException
import org.apache.spark.sql.delta.catalog.{DeltaCatalog, DeltaCatalogLike}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.connector.catalog.TransactionalCatalogPlugin
import org.apache.spark.sql.connector.catalog.transactions.{Transaction, TransactionInfo}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end coverage of the transactional Delta catalog: a v2 write over a transactional Delta
 * session catalog begins a catalog-scoped [[DeltaV2SparkTransaction]].
 */
class DeltaV2CatalogTransactionSuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  import DeltaV2CatalogTransactionSuite._

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf.set(DeltaSQLConf.V2_TRANSACTIONS_ENABLED.key, "true")
    // A v2 transaction only begins when the catalog serves v2 tables.
    // Exercise these tests under STRICT.
    conf.set(DeltaSQLConf.V2_ENABLE_MODE.key, "STRICT")
    sessionCatalogImpl.foreach(impl =>
      conf.set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, impl))
    conf
  }

  /**
   * The session catalog implementation to register.
   * Defaults to a recording transactional catalog.
   */
  protected def sessionCatalogImpl: Option[String] =
    Some(classOf[DeltaV2TestTransactionalCatalog].getName)

  /** Runs `write` and asserts it began a [[DeltaV2SparkTransaction]] */
  protected def validate(write: => DataFrame): Unit = {
    DeltaV2TestTransactionalCatalog.lastTransaction = null
    write
    assert(Option(DeltaV2TestTransactionalCatalog.lastTransaction)
      .exists(_.isInstanceOf[DeltaV2SparkTransaction]),
      "a write over the transactional session catalog should begin a DeltaV2SparkTransaction")
  }

  test("the session Delta catalog is a TransactionalCatalogPlugin") {
    val sessionCatalog = spark.sessionState.catalogManager.v2SessionCatalog
    assert(sessionCatalog.isInstanceOf[TransactionalCatalogPlugin])
  }

  test("Writes begin a DeltaV2SparkTransaction") {
    withTable("t") {
      sql("CREATE TABLE t (id INT) USING delta")
      validate { sql("INSERT INTO t VALUES (1), (2)") }
      checkAnswer(sql("SELECT id FROM t ORDER BY id"), Seq(Row(1), Row(2)))
    }
  }

  test("a read+write over the same table begins a transaction") {
    withTable("t") {
      sql("CREATE TABLE t (id INT) USING delta")
      sql("INSERT INTO t VALUES (1), (2)")
      validate { sql("INSERT INTO t SELECT id + 10 FROM t") }
      checkAnswer(sql("SELECT id FROM t ORDER BY id"), Seq(Row(1), Row(2), Row(11), Row(12)))
    }
  }

  test("a path-based write begins a DeltaV2SparkTransaction") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(s"CREATE TABLE delta.`$path` (id INT) USING delta")
      validate { sql(s"INSERT INTO delta.`$path` VALUES (1), (2)") }
      checkAnswer(sql(s"SELECT id FROM delta.`$path` ORDER BY id"), Seq(Row(1), Row(2)))
    }
  }

  test("changes clause resolves through the transactional catalog inside a write") {
    withTable("src", "dst") {
      // The read-time (v2) CDF path needs row tracking on the source.
      sql(
        """CREATE TABLE src (id BIGINT) USING delta TBLPROPERTIES (
          |  'delta.enableChangeDataFeed' = 'false',
          |  'delta.enableRowTracking' = 'true',
          |  'delta.enableDeletionVectors' = 'false'
          |)""".stripMargin)
      sql("INSERT INTO src VALUES (1)") // v1
      sql("INSERT INTO src VALUES (2)") // v2
      sql("INSERT INTO src VALUES (3)") // v3
      sql("CREATE TABLE dst (id BIGINT) USING delta")

      withSQLConf(DeltaSQLConf.DELTA_CHANGELOG_V2_ENABLED.key -> "true") {
        // Only version 2 is in range, so a wrong range would put more rows in dst.
        validate { sql("INSERT INTO dst SELECT id FROM src CHANGES FROM VERSION 2 TO VERSION 2") }
      }
      checkAnswer(sql("SELECT id FROM dst"), Row(2L))
    }
  }

  test("DeltaV2SparkTransactionCatalog forwards every catalog method DeltaCatalog implements") {
    // Compare the concrete delegate `DeltaCatalog`against the wrapper. Anything the
    // real delegate implements must be forwarded by the DeltaV2SparkTransactionCatalog wrapper
    // to the delegate.
    val impl = classOf[DeltaCatalog]
    val wrapper = classOf[DeltaV2SparkTransactionCatalog]

    def isRealMethod(m: Method): Boolean =
      !m.isSynthetic && !m.isBridge && !m.getName.contains("$")

    def signature(m: Method): String =
      s"${m.getName}(${m.getParameterTypes.map(_.getSimpleName).mkString(", ")})"

    def declaringClassOn(clazz: Class[_], m: Method): Class[_] =
      clazz.getMethod(m.getName, m.getParameterTypes: _*).getDeclaringClass

    // Methods DeltaCatalog implements that the wrapper intentionally does not forward.
    // Each entry should be verified to be safe. Currently none.
    val allowedUnforwardedNames = Set.empty[String]

    // DeltaCatalogLike is the flattened catalog contract (the catalog interfaces plus the
    // Delta-specific declarations), so its method surface is the set the wrapper must forward.
    val missing = classOf[DeltaCatalogLike].getMethods.toSeq
      .filter(isRealMethod)
      .filterNot(m => allowedUnforwardedNames.contains(m.getName))
      // Keep only methods DeltaCatalog implements (not left as an interface default).
      .filter(m => !declaringClassOn(impl, m).isInterface)
      // Flag those the wrapper does not override to forward.
      .filter(m => declaringClassOn(wrapper, m) != wrapper)
      .map(signature)
      .toSet

    assert(missing.isEmpty,
      "The wrapper must forward every catalog method DeltaCatalog implements. Missing " +
        s"forwards: ${missing.toSeq.sorted.mkString(", ")}. Add each to `delegate`, or to " +
        "`allowedUnforwardedNames` if the inherited default already reaches the delegate.")
  }
}

abstract class DeltaV2CatalogTransactionConfigGuardTestBase
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  /** The inconsistent startup configuration the session is registered with. */
  protected def inconsistentConf: Seq[(String, String)]

  /** The error condition the guard is expected to raise when the catalog is registered. */
  protected def expectedErrorCondition: String

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf.set(
      SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaV2TransactionalCatalog].getName)
    inconsistentConf.foreach { case (key, value) => conf.set(key, value) }
    conf
  }

  test("registering a transactional catalog with an inconsistent config fails fast") {
    checkError(
      exception = intercept[DeltaAnalysisException] {
        sql("CREATE TABLE t (id INT) USING delta")
      },
      condition = expectedErrorCondition)
  }
}

/** v2 transactions disabled while a transactional session catalog is registered. */
class DeltaV2CatalogTransactionsDisabledGuardSuite
    extends DeltaV2CatalogTransactionConfigGuardTestBase {
  override protected def inconsistentConf: Seq[(String, String)] = Seq(
    DeltaSQLConf.V2_TRANSACTIONS_ENABLED.key -> "false",
    DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT")

  override protected def expectedErrorCondition: String =
    "DELTAV2_TRANSACTIONS_INCONSISTENT_CONFIG.CATALOG_MISMATCH"
}

/** v2 transactions enabled but the connector does not serve v2 tables (not STRICT). */
class DeltaV2CatalogTransactionsNonStrictGuardSuite
    extends DeltaV2CatalogTransactionConfigGuardTestBase {
  override protected def inconsistentConf: Seq[(String, String)] = Seq(
    DeltaSQLConf.V2_TRANSACTIONS_ENABLED.key -> "true",
    DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE")

  override protected def expectedErrorCondition: String =
    "DELTAV2_TRANSACTIONS_INCONSISTENT_CONFIG.REQUIRES_V2_CONNECTOR"
}

object DeltaV2CatalogTransactionSuite {
  /**
   * Transactional Delta session catalog that records the last transaction it begins. This allows
   * tests to inspect it. This mirrors Spark's `InMemoryRowLevelOperationTableCatalog`. Can be
   * registered via `spark.sql.catalog.spark_catalog`.
   */
  class DeltaV2TestTransactionalCatalog extends DeltaV2TransactionalCatalog {
    override def beginTransaction(info: TransactionInfo): Transaction = {
      val txn = super.beginTransaction(info)
      DeltaV2TestTransactionalCatalog.lastTransaction = txn
      txn
    }
  }

  object DeltaV2TestTransactionalCatalog {
    @volatile var lastTransaction: Transaction = _
  }
}
