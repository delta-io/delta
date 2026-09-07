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


package org.apache.spark.sql.delta

import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.immutable.NumericRange

import org.apache.spark.sql.delta.actions.{Metadata, Protocol, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf

/**
 * Verifies the IdentitySequenceService backend end-to-end:
 *
 *   - The service is invoked when the SparkConf is on.
 *   - CREATE TABLE stamps the `delta.identity.concurrent.sequenceId` pointer per identity
 *     column when the conf is on.
 *
 * Re-uses the suite scaffolding from [[ConcurrentIdentityColumnSuiteBase]] (table
 * properties enabling the CIC feature, MERGE helpers).
 */
class ConcurrentIdentityColumnServiceBackendSuite extends ConcurrentIdentityColumnSuiteBase {

  import testImplicits._

  // The injected backend (`localService`), its class-name conf, and the per-test reset all live
  // in ConcurrentIdentityColumnSuiteBase so every CIC suite shares them.

  test("MERGE routes through LocalIdentitySequenceService") {
    withTable("target") {
      withTempView("source") {
        val createdSequencesBeforeCreate = localService.createSequenceCount
        withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
          spark.sql(createTargetTableStatement(Seq(
            "ids BIGINT GENERATED ALWAYS AS IDENTITY",
            "values INT")))
        }
        // Single partition keeps the generator contiguous within its initial reserved range, so
        // this routing test exercises the happy path; the executor reserves a fresh range from
        // the driver only when a range is exhausted (see the reserve-continues tests below).
        val source = Seq(10, 20, 30, 40, 50)
        source.toDF("values").repartition(1).createOrReplaceTempView("source")

        val createdSequencesAfterCreate = localService.createSequenceCount
        assert(createdSequencesAfterCreate - createdSequencesBeforeCreate === 1L,
          s"CREATE TABLE with one identity column and the service backend on must allocate " +
            "exactly one sequence; createSequence counter went from " +
            s"$createdSequencesBeforeCreate to $createdSequencesAfterCreate.")

        val reservesBefore = localService.reserveIdsCount

        withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
          executeMerge(
            tgt = s"delta.`$tempPath` AS t",
            src = "source s",
            cond = "t.values = s.values",
            clauses = insert("(values) VALUES (s.values)"))
        }

        val reservesAfter = localService.reserveIdsCount
        assert(reservesAfter > reservesBefore,
          s"LocalIdentitySequenceService.reserveIds should have been invoked at least once; " +
            s"counter went from $reservesBefore to $reservesAfter. " +
            "This means the service-backed path is silently bypassed. Either the conf " +
            "isn't propagating to the MERGE-path reservation, or the override is " +
            "returning None.")

        // Sanity: the merged data still has correct identity values starting at 1.
        val targetDf = readDeltaTable(tempPath)
        assertCorrectIdentityColumn(
          targetDf.select("ids"),
          expectedSize = source.size,
          start = 1, step = 1,
          expectedMin = 1,
          expectedMax = Long.MaxValue)
      }
    }
  }

  private def syncedSequenceId: Option[String] =
    ConcurrentIdentityColumnSchema.getSequenceId(deltaLog.update().metadata.schema("ids"))

  /**
   * Fabricates an unconverted LEGACY table: a plain identity table whose protocol carries
   * the CIC feature but whose columns have no sequence pointer, optionally with rows
   * written through the stock identity path. This is unconverted on-disk shape; it can no
   * longer be produced through DDL (CREATE stamps on feature opt-in and ALTER converts), so
   * the protocol upgrade is committed raw, deliberately bypassing the conversion hook.
   */
  private def createUnconvertedLegacyTable(): Unit = {
    spark.sql(
      s"""CREATE TABLE target (
         |  ids BIGINT GENERATED ALWAYS AS IDENTITY,
         |  values INT)
         |USING DELTA
         |LOCATION '$tempPath'
         |tblproperties(
         |  ${TableFeatureProtocolUtils.propertyKey(IdentityColumnsTableFeature)} = 'enabled')
         |""".stripMargin)
    val txn = deltaLog.startTransaction()
    txn.updateProtocol(txn.protocol.merge(
      Protocol.forTableFeature(ConcurrentIdentityColumnsTableFeature)))
    txn.commit(Nil, DeltaOperations.ManualUpdate)
    assert(deltaLog.update().protocol.isFeatureSupported(ConcurrentIdentityColumnsTableFeature),
      "Legacy fixture: the raw protocol upgrade must land the CIC feature.")
    assert(syncedSequenceId.isEmpty,
      "Legacy fixture: the raw upgrade must NOT stamp a sequence pointer.")
  }

  test("service calls are keyed by the UC table id when the table carries one") {
    // Create must rely on the UC table id not the one stored in the Delta metadata.
    withTable("target") {
      val ucTableId = s"uc-${java.util.UUID.randomUUID()}"
      spark.sql(
        s"""CREATE TABLE target (
           |  ids BIGINT GENERATED ALWAYS AS IDENTITY,
           |  values INT)
           |USING DELTA
           |LOCATION '$tempPath'
           |tblproperties(
           |  '${UCCommitCoordinatorClient.UC_TABLE_ID_KEY}' = '$ucTableId',
           |  ${TableFeatureProtocolUtils.propertyKey(ConcurrentIdentityColumnsTableFeature)}
           |    = 'enabled',
           |  ${TableFeatureProtocolUtils.propertyKey(DomainMetadataTableFeature)} = 'enabled',
           |  ${TableFeatureProtocolUtils.propertyKey(IdentityColumnsTableFeature)} = 'enabled')
           |""".stripMargin)
      val metadataId = deltaLog.update().metadata.id
      assert(metadataId != ucTableId, "Setup: the two ids must differ for this test to bite.")
      val seqAfterCreate = syncedSequenceId.getOrElse(fail("CREATE must stamp a sequenceId."))
      assert(localService.hasSequence(ucTableId, seqAfterCreate),
        "CREATE registration must key the sequence under the UC table id.")
      assert(!localService.hasSequence(metadataId, seqAfterCreate),
        "CREATE registration must not key the sequence under the metadata.id.")

      // The write reservation resolves the same scope; a metadata.id-keyed reserve would
      // fail with SEQUENCE_NOT_FOUND instead of writing these rows.
      spark.sql(s"INSERT INTO delta.`$tempPath` (values) VALUES (10), (20), (30)")
      assertCorrectIdentityColumn(
        readDeltaTable(tempPath).select("ids"),
        expectedSize = 3L, start = 1L, step = 1L,
        expectedMin = Long.MinValue, expectedMax = Long.MaxValue)
    }
  }

  test("CREATE TABLE with multiple identity columns stamps each with a distinct sequence id") {
    withTable("target") {
      val numCreatesBefore = localService.createSequenceCount
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        spark.sql(createTargetTableStatement(Seq(
          "id_a BIGINT GENERATED ALWAYS AS IDENTITY",
          "id_b BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 2)",
          // GENERATED BY DEFAULT so allowExplicitInsert is stamped as true, and a
          // distinct start/step so we can confirm each column's values are set
          // independently rather than copied from a sibling.
          "id_c BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 50 INCREMENT BY 3)",
          "values INT")))
      }
      // One createSequence per identity column.
      assert(localService.createSequenceCount - numCreatesBefore === 3L,
        "Three identity columns must allocate exactly three sequences.")

      val reloadedSchema = deltaLog.update().metadata.schema
      val seqA = ConcurrentIdentityColumnSchema.getSequenceId(reloadedSchema("id_a"))
      val seqB = ConcurrentIdentityColumnSchema.getSequenceId(reloadedSchema("id_b"))
      val seqC = ConcurrentIdentityColumnSchema.getSequenceId(reloadedSchema("id_c"))
      assert(Seq(seqA, seqB, seqC).forall(_.isDefined),
        s"All columns must carry concurrent sequenceId metadata; " +
          s"got id_a=$seqA, id_b=$seqB, id_c=$seqC")
      assert(Set(seqA.get, seqB.get, seqC.get).size === 3,
        s"Each column must get a distinct sequence id; got $seqA, $seqB, $seqC")

      // Each column keeps its own start/step/allowExplicitInsert from its definition (in the
      // standard delta.identity.* keys; CIC does not duplicate them), not a sibling's.
      def assertStamp(
          col: String, start: Long, step: Long, allowExplicitInsert: Boolean): Unit = {
        val field = reloadedSchema(col)
        val info = IdentityColumn.getIdentityInfo(field)
        assert(info.start === start, s"$col start must be $start; got $info")
        assert(info.step === step, s"$col step must be $step; got $info")
        assert(IdentityColumn.allowExplicitInsert(field) === allowExplicitInsert,
          s"$col allowExplicitInsert must be $allowExplicitInsert; got metadata: ${field.metadata}")
      }
      assertStamp("id_a", start = 1L, step = 1L, allowExplicitInsert = false)
      assertStamp("id_b", start = 100L, step = 2L, allowExplicitInsert = false)
      assertStamp("id_c", start = 50L, step = 3L, allowExplicitInsert = true)
    }
  }

  // One identity column's stamped metadata. A CIC column's high-water mark lives in the service,
  // never the schema, so hasHighWaterMark must stay false.
  private case class ColumnStamp(
      sequenceId: Option[String],
      start: Long,
      step: Long,
      allowExplicitInsert: Boolean,
      hasHighWaterMark: Boolean)

  private def readColumnStamps(): Map[String, ColumnStamp] = {
    val schema = deltaLog.update().metadata.schema
    IdentityColumn.getIdentityColumns(schema).map { field =>
      val info = IdentityColumn.getIdentityInfo(field)
      field.name -> ColumnStamp(
        sequenceId = ConcurrentIdentityColumnSchema.getSequenceId(field),
        start = info.start,
        step = info.step,
        allowExplicitInsert = IdentityColumn.allowExplicitInsert(field),
        hasHighWaterMark = info.highWaterMark.isDefined)
    }.toMap
  }

  private def assertFullyStamped(stamps: Map[String, ColumnStamp], stage: String): Unit = {
    stamps.foreach { case (col, stamp) =>
      assert(stamp.sequenceId.exists(_.nonEmpty),
        s"$stage: $col must carry a non-empty concurrent sequenceId; got ${stamp.sequenceId}")
      assert(stamp.sequenceId.exists(id => scala.util.Try(java.util.UUID.fromString(id)).isSuccess),
        s"$stage: $col sequenceId must be a UUID; got ${stamp.sequenceId}")
      assert(!stamp.hasHighWaterMark,
        s"$stage: $col must NOT carry a schema high-water mark (the service owns it).")
    }
  }

  // (label, DDL, expected start/step/allowExplicitInsert). Boundary starts step away from the
  // boundary so value generation can't overflow.
  private val identityColumnStampShapes = Seq(
    ("default",
      "id BIGINT GENERATED ALWAYS AS IDENTITY", 1L, 1L, false),
    ("max start, negative step",
      s"id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH ${Long.MaxValue} INCREMENT BY -13)",
      Long.MaxValue, -13L, false),
    ("min start, positive step",
      s"id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH ${Long.MinValue} INCREMENT BY 23)",
      Long.MinValue, 23L, true))

  for ((label, ddl, start, step, allowExplicit) <- identityColumnStampShapes)
  test(s"stamped identity metadata is correct and fixed across CREATE, INSERT, MERGE ($label)") {
    // INSERT and MERGE advance the service, not the schema, so the stamp must be identical
    // after each write.
    withTable("target") {
      withTempView("source") {
        withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
          spark.sql(createTargetTableStatement(Seq(ddl, "plain STRING")))

          val afterCreate = readColumnStamps()
          assert(afterCreate.keySet === Set("id"),
            s"only the identity column must be stamped; got ${afterCreate.keySet}")
          val schema = deltaLog.update().metadata.schema
          assert(!ColumnWithDefaultExprUtils.isIdentityColumn(schema("plain")),
            "the plain column must not have become an identity column")
          assertFullyStamped(afterCreate, "after CREATE")
          val created = afterCreate("id")
          assert(created.start === start && created.step === step &&
              created.allowExplicitInsert === allowExplicit,
            s"id start/step/allowExplicitInsert must round-trip as " +
              s"($start, $step, $allowExplicit); got $created")

          spark.sql(s"INSERT INTO delta.`$tempPath` (plain) VALUES ('a'), ('b'), ('c')")
          val afterInsert = readColumnStamps()
          assertFullyStamped(afterInsert, "after INSERT")
          assert(afterInsert === afterCreate,
            s"INSERT must not rewrite identity stamping.\n" +
              s"  create=$afterCreate\n  insert=$afterInsert")

          Seq("d", "e").toDF("plain").repartition(1).createOrReplaceTempView("source")
          executeMerge(
            tgt = s"delta.`$tempPath` AS t",
            src = "source s",
            cond = "t.plain = s.plain",
            clauses = insert("(plain) VALUES (s.plain)"))
          val afterMerge = readColumnStamps()
          assertFullyStamped(afterMerge, "after MERGE")
          assert(afterMerge === afterCreate,
            s"MERGE must not rewrite identity stamping.\n" +
              s"  create=$afterCreate\n  merge=$afterMerge")
        }
      }
    }
  }

  test("maybeStampSequenceMetadata is idempotent across repeated invocations") {
    withTable("target") {
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        spark.sql(createTargetTableStatement(Seq(
          "ids BIGINT GENERATED ALWAYS AS IDENTITY",
          "values INT")))
      }
      val stampedMetadata = deltaLog.update().metadata
      val createsAfterFirstStamp =
        localService.createSequenceCount

      // Re-running the stamping hook over already-stamped metadata must be a no-op:
      // it returns the input metadata unchanged. The hook only stamps; it never calls
      // the service, so the createSequence counter must also be untouched.
      val result: Metadata = withSQLConf(
        DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        ConcurrentIdentityColumnCreateTableHook.maybeStampSequenceMetadata(
          spark, stampedMetadata)
      }
      assert(localService.createSequenceCount === createsAfterFirstStamp,
        "Idempotent stamping must NOT allocate a sequence (the hook never calls the service).")
      assert(result eq stampedMetadata,
        "Idempotent hook invocation must return the input metadata instance unchanged.")
      assert(result.schemaString === stampedMetadata.schemaString,
        "schemaString must round-trip unchanged on an idempotent invocation.")
    }
  }

  test("CREATE OR REPLACE TABLE re-stamps concurrent sequence metadata with a fresh sequence id") {
    withTable("target") {
      val createSql = createTargetTableStatement(Seq(
        "ids BIGINT GENERATED ALWAYS AS IDENTITY",
        "values INT"))
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        spark.sql(createSql)
      }
      val firstSeqId = ConcurrentIdentityColumnSchema
        .getSequenceId(deltaLog.update().metadata.schema("ids")).get
      val createsAfterFirstCreate =
        localService.createSequenceCount

      // CREATE OR REPLACE on a CIC table must mint a fresh sequence: the strip
      // helper at the REPLACE call site removes any carried-over concurrent keys
      // before the hook runs, so the idempotency short-circuit cannot reuse the
      // previous sequence id.
      val replaceSql = createSql.replaceFirst("CREATE TABLE", "CREATE OR REPLACE TABLE")
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        spark.sql(replaceSql)
      }
      assert(localService.createSequenceCount ===
          createsAfterFirstCreate + 1L,
        "REPLACE on a CIC table must allocate exactly one new sequence.")

      val secondSeqId = ConcurrentIdentityColumnSchema
        .getSequenceId(deltaLog.update().metadata.schema("ids")).get
      assert(secondSeqId != firstSeqId,
        s"REPLACE must re-stamp with a fresh sequence id; both are $secondSeqId")
    }
  }

  test("INSERT of an explicit value into a GENERATED ALWAYS CIC column is rejected") {
    withTable("target") {
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        spark.sql(createTargetTableStatement(Seq(
          "ids BIGINT GENERATED ALWAYS AS IDENTITY",
          "values INT")))
        val ex = intercept[DeltaAnalysisException] {
          spark.sql(s"INSERT INTO delta.`$tempPath` (ids, values) VALUES (5, 10)")
        }
        checkError(ex, "DELTA_IDENTITY_COLUMNS_EXPLICIT_INSERT_NOT_SUPPORTED", "42808",
          Map("colName" -> "ids"))
      }
    }
  }

  test("INSERT of an explicit value into a GENERATED BY DEFAULT CIC column is accepted") {
    withTable("target") {
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        spark.sql(createTargetTableStatement(Seq(
          "ids BIGINT GENERATED BY DEFAULT AS IDENTITY",
          "values INT")))
        spark.sql(s"INSERT INTO delta.`$tempPath` (ids, values) VALUES (5, 10)")
        checkAnswer(readDeltaTable(tempPath).select("ids", "values"), Row(5L, 10))
      }
    }
  }

  test("service-backed reservation throws when identity column lacks concurrent stamping") {
    // A legacy table (feature on, never stamped) cannot reserve: the pre-scan guard must
    // hit before any partial allocation happens, pointing at the conversion DDL.
    withTable("target") {
      createUnconvertedLegacyTable()
      val reservesBefore = localService.reserveIdsCount

      val reservation =
        new TestServiceBackedIdentityColumnReservation(deltaLog, None, spark)
      val ex = intercept[ConcurrentIdentityColumnReservationException] {
        reservation.reserveValuesForIdentityColumns(numRows = 1L)
      }
      checkError(
        ex,
        condition = "DELTA_CONCURRENT_IDENTITY_COLUMN_CONVERSION_INCOMPLETE",
        sqlState = "XXKDS",
        parameters = Map(
          "columnName" -> "ids",
          "tableId" -> deltaLog.update().metadata.id))

      assert(localService.reserveIdsCount === reservesBefore,
        "Pre-scan must throw BEFORE any reserveIds call (all-or-nothing contract).")
    }
  }

  test("INSERT generator selection rejects a stamped column with no reservation slot") {
    // Backstop for the INSERT path, mirroring the MERGE poison pill: a CIC-stamped column that
    // reaches addDefaultExprsOrReturnConstraints without a reservation slot must fail rather than
    // fall through to the legacy high-water-mark generator (which would emit values that could
    // collide with the service-managed sequence). Drive the selection directly with no reservation
    // so the stamped column hits the backstop instead of the service-backed branch.
    withTable("target") {
      spark.sql(createTargetTableStatement(Seq(
        "ids BIGINT GENERATED ALWAYS AS IDENTITY",
        "values INT")))
      assert(syncedSequenceId.isDefined, "Fixture: the CIC column must be stamped.")
      val snapshot = deltaLog.update()
      // Data missing the identity column, so `ids` needs generation and reaches the selection.
      val data = spark.range(0).toDF("values")
      val ex = intercept[ConcurrentIdentityColumnReservationException] {
        ColumnWithDefaultExprUtils.addDefaultExprsOrReturnConstraints(
          deltaLog,
          snapshot.protocol,
          data.queryExecution,
          snapshot.metadata.schema,
          data,
          nullAsDefault = false,
          identityColumnReservation = None)
      }
      checkError(
        ex,
        condition = "DELTA_CONCURRENT_IDENTITY_COLUMN_USING_WRONG_GENERATOR",
        sqlState = "XXKDS",
        parameters = Map(
          "columnNames" -> "ids",
          "tableId" -> snapshot.metadata.id))
    }
  }

  test("MERGE on a CIC table lacking concurrent stamping fails with the missing-stamp guard") {
    // End-to-end SQL counterpart of the trait-level test: a MERGE on an unstamped legacy
    // table must hit the missing-stamp guard.
    withTable("target") {
      withTempView("source") {
        createUnconvertedLegacyTable()
        Seq(1, 2, 3).toDF("values").createOrReplaceTempView("source")
        val reservesBefore = localService.reserveIdsCount

        val ex = intercept[Exception] {
          executeMerge(
            tgt = s"delta.`$tempPath` AS t",
            src = "source s",
            cond = "t.values = s.values",
            clauses = insert("(values) VALUES (s.values)"))
        }
        val cause = cicReservationCause(ex)
        checkError(
          cause,
          condition = "DELTA_CONCURRENT_IDENTITY_COLUMN_CONVERSION_INCOMPLETE",
          sqlState = "XXKDS",
          parameters = Map(
          "columnName" -> "ids",
          "tableId" -> deltaLog.update().metadata.id))
        assert(localService.reserveIdsCount === reservesBefore,
          "Guard must throw BEFORE any reserveIds call (all-or-nothing contract).")
      }
    }
  }

  test("CREATE TABLE without the CIC feature does NOT stamp") {
    // Plain identity-column table (no CIC table feature): the hook must return without
    // calling createSequence or stamping, because the feature opt-in is the gate.
    withTable("target") {
      val numCreatesBefore = localService.createSequenceCount
      spark.sql(
        s"""CREATE TABLE target (
           |  ids BIGINT GENERATED ALWAYS AS IDENTITY,
           |  values INT)
           |USING DELTA
           |LOCATION '$tempPath'
           |""".stripMargin)
      assert(localService.createSequenceCount === numCreatesBefore,
        "Without the CIC feature, CREATE TABLE must NOT allocate a sequence.")
      val reloadedSchema = deltaLog.update().metadata.schema
      assert(!ConcurrentIdentityColumnSchema.hasConcurrentSequenceMetadata(reloadedSchema("ids")),
        "Conf on + feature off must NOT stamp concurrent sequence metadata.")
    }
  }

  test("insert-only MERGE at the production-default routing reserves identity values " +
    "from the service") {
    // With MERGE_INSERT_ONLY_ENABLED at its production default (true; this suite's base
    // force-disables it), an insert-only MERGE uses InsertOnlyMergeExecutor.writeOnlyInserts.
    // That path counts the source, reserves from the service before the write, and rewrites the
    // insert clauses onto the reserved range. Two successive MERGEs must produce strictly distinct
    // values.
    withTable("target") {
      withTempView("source_a", "source_b") {
        withSQLConf(
          DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true",
          DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED.key -> "true",
          SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          spark.sql(createTargetTableStatement(Seq(
            "ids BIGINT GENERATED ALWAYS AS IDENTITY",
            "values INT")))
          Seq(10, 20, 30).toDF("values").repartition(1).createOrReplaceTempView("source_a")
          Seq(40, 50, 60).toDF("values").repartition(1).createOrReplaceTempView("source_b")

          val reservesBefore = localService.reserveIdsCount
          executeMerge(
            tgt = s"delta.`$tempPath` AS t",
            src = "source_a s",
            cond = "t.values = s.values",
            clauses = insert("(values) VALUES (s.values)"))
          executeMerge(
            tgt = s"delta.`$tempPath` AS t",
            src = "source_b s",
            cond = "t.values = s.values",
            clauses = insert("(values) VALUES (s.values)"))

          assert(localService.reserveIdsCount >= reservesBefore + 2L,
            "Each insert-only MERGE must reserve from the service.")
          val ids = readDeltaTable(tempPath).select("ids").collect().map(_.getLong(0))
          assert(ids.length === 6,
            s"both MERGEs must have inserted 3 rows each; got ${ids.length}")
          assert(ids.distinct.length === 6,
            "the two MERGEs must consume disjoint reserved ranges (distinct ids); " +
              s"got ${ids.sorted.mkString(", ")}")
          assert(ids.forall(_ >= 1L), s"ids must be at or above the start; got ${ids.min}")
        }
      }
    }
  }

  test("INSERT INTO routes through LocalIdentitySequenceService") {
    withTable("target") {
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        spark.sql(createTargetTableStatement(Seq(
          "ids BIGINT GENERATED ALWAYS AS IDENTITY",
          "values INT")))

        val reservesBefore = localService.reserveIdsCount

        spark.sql(s"INSERT INTO delta.`$tempPath` (values) VALUES (10), (20), (30), (40), (50)")

        val reservesAfter = localService.reserveIdsCount
        assert(reservesAfter > reservesBefore,
          s"LocalIdentitySequenceService.reserveIds should have been invoked at least once; " +
            s"counter went from $reservesBefore to $reservesAfter. " +
            "INSERT did not route through the service-backend write path.")

        val targetDf = readDeltaTable(tempPath)
        assertCorrectIdentityColumn(
          targetDf.select("ids"),
          expectedSize = 5L,
          start = 1, step = 1,
          expectedMin = 1,
          expectedMax = Long.MaxValue)
      }
    }
  }

  /**
   * Shared body for the MERGE reserve-continue family. The same skew recipe (6 source rows hashed
   * to one partition of eight, tiny reserves) forces reserve-more across all three MERGE routings,
   * which differ only by routing conf and the merge clauses (passed as a thunk so each call keeps
   * its exact clause form).
   */
  private def checkMergeReserveContinues(
      extraConfs: Seq[(String, String)], runMerge: () => Unit): Unit = {
    withTable("target") {
      withTempView("source") {
        val confs =
          Seq(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") ++ extraConfs
        withSQLConf(confs: _*) {
          spark.sql(createTargetTableStatement(Seq(
            "ids BIGINT GENERATED ALWAYS AS IDENTITY",
            "values INT")))
          (1 to 6).toDF("values").repartition(8, lit(0)).createOrReplaceTempView("source")
          val reservesBefore = localService.reserveIdsCount
          runMerge()
          val ids = readDeltaTable(tempPath).select("ids").collect().map(_.getLong(0))
          val sortedIds = ids.sorted.mkString(", ")
          assert(ids.length === 6, s"All 6 source rows must be inserted; got ${ids.length}.")
          assert(ids.distinct.length === 6,
            s"reserve-continue must not produce duplicate identity values; got $sortedIds")
          assert(ids.forall(_ >= 1L),
            s"identity values must be at or above the start; got $sortedIds")
          assert(localService.reserveIdsCount > reservesBefore + 2L,
            "Expected the service backend to be used (reserveIds invoked).")
        }
      }
    }
  }

  test("MERGE reserve continues past the initial range (insert-only routing)") {
    checkMergeReserveContinues(
      Seq(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED.key -> "true"),
      () => executeMerge(
        tgt = s"delta.`$tempPath` AS t",
        src = "source s",
        cond = "t.values = s.values",
        clauses = insert("(values) VALUES (s.values)")))
  }

  test("MERGE reserve continues past the initial range (classic routing)") {
    checkMergeReserveContinues(
      Seq.empty,
      () => executeMerge(
        tgt = s"delta.`$tempPath` AS t",
        src = "source s",
        cond = "t.values = s.values",
        clauses = update(set = "values = s.values"), insert("(values) VALUES (s.values)")))
  }

  test("MERGE into a CIC table with two identity columns both exhaust their initial range") {
    // MERGE counterpart of the two-column INSERT exhaustion test: a tiny initial reserve plus
    // enough rows on one partition forces both columns' generators past their initial range, so
    // each reserves more mid-write. Confirms the per-column reserve-more loop keeps each column's
    // sequence distinct (no cross-column bleed) on the MERGE path.
    withTable("target") {
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        spark.sql(createTargetTableStatement(Seq(
          "id_a BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)",
          "id_b BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1000 INCREMENT BY 10)",
          "values INT")))
        withTempView("source") {
          val numRows = 10
          // All rows on one partition so each column exhausts its small initial range and must
          // reserve more mid-write.
          spark.range(numRows).select($"id".cast("int").as("values"))
            .repartition(1, lit(0)).createOrReplaceTempView("source")
          val reservesBefore = localService.reserveIdsCount
          executeMerge(
            tgt = s"delta.`$tempPath` AS t",
            src = "source s",
            cond = "t.values = s.values",
            clauses = insert("(values) VALUES (s.values)"))
          val rows = readDeltaTable(tempPath).select("id_a", "id_b").collect()
          val aVals = rows.map(_.getLong(0)).sorted
          val bVals = rows.map(_.getLong(1)).sorted
          assert(aVals.length === numRows, s"id_a: expected $numRows rows; got ${aVals.length}")
          assert(bVals.length === numRows, s"id_b: expected $numRows rows; got ${bVals.length}")
          assert(aVals.distinct.length === numRows,
            s"id_a must be distinct; got ${aVals.mkString(",")}")
          assert(bVals.distinct.length === numRows,
            s"id_b must be distinct; got ${bVals.mkString(",")}")
          assert(aVals.head === 1L, s"id_a must start at 1; got ${aVals.head}")
          assert(bVals.head === 1000L, s"id_b must start at 1000; got ${bVals.head}")
          assert(localService.reserveIdsCount >= reservesBefore + 2L,
            "Both columns must reserve more than once (reserveIds invoked repeatedly).")
        }
      }
    }
  }

  test("UPDATE non-identity column on a CIC table does NOT touch the service") {
    // UPDATE of identity columns is blocked at analysis
    // (DELTA_IDENTITY_COLUMNS_UPDATE_NOT_SUPPORTED), so UPDATE never generates new
    // identity values. The reservation path must stay dormant, and no DomainMetadata
    // HWM may be written -- the existing identity values on PREIMAGE rows propagate
    // unchanged.
    withTable("target") {
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        spark.sql(createTargetTableStatement(Seq(
          "ids BIGINT GENERATED ALWAYS AS IDENTITY",
          "values INT")))
        spark.sql(s"INSERT INTO delta.`$tempPath` (values) VALUES (10), (20), (30)")
      }

      val numCreatesBefore = localService.createSequenceCount
      val reservesBefore = localService.reserveIdsCount

      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        spark.sql(s"UPDATE delta.`$tempPath` SET values = values + 100 WHERE values = 20")
      }

      assert(localService.createSequenceCount === numCreatesBefore,
        "UPDATE must not allocate a new sequence.")
      assert(localService.reserveIdsCount === reservesBefore,
        "UPDATE must not reserve identity values.")
    }
  }

  test("INSERT INTO a non-CIC identity table does NOT touch the service") {
    // Non-CIC identity table: legacy `identityColumns` feature only, no
    // ConcurrentIdentityColumnsTableFeature. Default conf (service backend off).
    // The reservation path must stay dormant; the legacy HWM path handles
    // identity generation as before.
    withTable("target") {
      spark.sql(
        s"""
           |CREATE TABLE target (
           |  ids BIGINT GENERATED ALWAYS AS IDENTITY,
           |  values INT)
           |USING DELTA
           |LOCATION '$tempPath'
           |tblproperties(
           |  ${TableFeatureProtocolUtils.propertyKey(IdentityColumnsTableFeature)} = 'enabled')
           |""".stripMargin)

      val numCreatesBefore = localService.createSequenceCount
      val reservesBefore = localService.reserveIdsCount

      spark.sql(s"INSERT INTO delta.`$tempPath` (values) VALUES (10), (20), (30)")

      assert(localService.createSequenceCount === numCreatesBefore,
        "Conf-off INSERT must not touch createSequence.")
      assert(localService.reserveIdsCount === reservesBefore,
        "Conf-off INSERT must not touch reserveIds.")

      // Identity values landed correctly via the legacy HWM path.
      val targetDf = readDeltaTable(tempPath)
      assertCorrectIdentityColumn(
        targetDf.select("ids"),
        expectedSize = 3L,
        start = 1, step = 1,
        expectedMin = 1,
        expectedMax = Long.MaxValue)
    }
  }

  test("INSERT succeeds when the identity column has no min/max stats " +
    "(column past the data-skipping limit)") {
    // Guards that a CIC write does not depend on the identity column's data-skipping min/max
    // stats. Upfront reservation depends on it.
    // Uses a distinct real Delta-table name (`deltaSource`) so it does not collide with the
    // `source` temp views the sibling tests register, and because the INSERT must read from a
    // Delta scan (which carries row-count stats). A `CREATE TABLE source` would otherwise be
    // shadowed by such a view and route the INSERT into the RDD-based view
    // (UNSUPPORTED_INSERT.RDD_BASED).
    withTable("target", "deltaSource") {
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        // Identity column declared AFTER `values`; with 1 indexed column, `ids` gets no min/max.
        spark.sql(createTargetTableStatement(Seq(
          "values INT",
          "ids BIGINT GENERATED ALWAYS AS IDENTITY")))
        spark.sql(
          s"ALTER TABLE delta.`$tempPath` SET TBLPROPERTIES " +
            s"('${DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.key}' = '1')")

        // Insert FROM a Delta table: a Delta scan carries a planning-time row-count estimate
        // (optimizedPlan.stats.rowCount) that the upfront reservation sizes from. A VALUES insert
        // is an exact LocalRelation with no such estimate and would not exercise that path.
        spark.sql("CREATE TABLE deltaSource (values INT) USING delta")
        spark.sql("INSERT INTO deltaSource VALUES (10), (20), (30), (40), (50)")

        val reservesBefore = localService.reserveIdsCount
        spark.sql(s"INSERT INTO delta.`$tempPath` (values) SELECT values FROM deltaSource")
        assert(localService.reserveIdsCount > reservesBefore,
          "INSERT must route through the service backend.")

        val targetDf = readDeltaTable(tempPath)
        assertCorrectIdentityColumn(
          targetDf.select("ids"),
          expectedSize = 5L, start = 1, step = 1, expectedMin = 1, expectedMax = Long.MaxValue)
      }
    }
  }

  /**
   * Shared body for the INSERT reserve-continue family. A tiny initial reserve + short rate window
   * force the busy partition to reserve several small ranges, so the generator must stitch
   * consecutive reserves into distinct, gap-tolerant identity values past its first reserved range.
   * Cases differ only by step direction and row count.
   */
  private def checkInsertReserveContinues(
      idColumnDdl: String, numRows: Int, boundDesc: String, withinBound: Long => Boolean): Unit = {
    withTable("target") {
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        spark.sql(createTargetTableStatement(Seq(idColumnDdl, "values INT")))
        val reservesBefore = localService.reserveIdsCount
        (1 to numRows).toDF("values").repartition(8, lit(0))
          .write.format("delta").mode("append").save(tempPath)
        val ids = readDeltaTable(tempPath).select("ids").collect().map(_.getLong(0))
        val sortedIds = ids.sorted.mkString(", ")
        assert(ids.length === numRows, s"All $numRows rows must be written; got ${ids.length}.")
        assert(ids.distinct.length === numRows,
          s"reserve-continue must not produce duplicate identity values; got $sortedIds")
        assert(ids.forall(withinBound),
          s"identity values must be $boundDesc; got $sortedIds")
        assert(localService.reserveIdsCount >= reservesBefore + 2L,
          "Expected the service backend to be used (reserveIds invoked).")
      }
    }
  }

  test("reserve continues past the initial range (service backend, ascending)") {
    checkInsertReserveContinues(
      "ids BIGINT GENERATED ALWAYS AS IDENTITY", 6, "at or above the start", _ >= 1L)
  }

  test("reserve continues past the initial range (service backend, descending)") {
    checkInsertReserveContinues(
      "ids BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY -3)", 6,
      "at or below the start", _ <= 100L)
  }

  test("reserve continues past the initial range (service backend, ascending, 1k rows)") {
    checkInsertReserveContinues(
      "ids BIGINT GENERATED ALWAYS AS IDENTITY", 1000, "at or above the start", _ >= 1L)
  }

  test("a mid-write reserve RPC failure aborts the write and commits no rows") {
    // The generator has no retry: a reserve that fails mid-write aborts the task, and a Delta
    // write commits atomically, so a failed refill commits no row and no out-of-range value. A
    // tiny initial reserve forces refills; arming the fault on the second reserve lets the first
    // succeed (write in flight) then fails a refill.
    withTable("target") {
      withSQLConf(
        DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true",
        SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        spark.sql(createTargetTableStatement(Seq(
          "ids BIGINT GENERATED ALWAYS AS IDENTITY",
          "values INT")))
        val reservesBefore = localService.reserveIdsCount
        // A floor, not an exact ordinal, so a task retry's reserve also fails.
        localService.failReserveIdsStartingAt(reservesBefore + 2L)

        val error = intercept[Exception] {
          (1 to 10).toDF("values").repartition(1)
            .write.format("delta").mode("append").save(tempPath)
        }
        assert(messageChainContains(error, "Injected reserveIds failure"),
          s"the write must fail with the injected reserve failure; " +
            s"got ${causeChainMessages(error)}")
        assert(localService.reserveIdsCount >= reservesBefore + 2L,
          s"expected the write to reserve at least twice before failing; counter went from " +
            s"$reservesBefore to ${localService.reserveIdsCount}")
        assert(readDeltaTable(tempPath).count() === 0L,
          "a write aborted by a mid-write reserve failure must not commit any rows")
      }
    }
  }

  test("INSERT into a negative-step CIC column commits (service backend, no overshoot)") {
    // Regression: a descending sequence reserves rangeEnd < rangeStart, so building the slot
    // with `start to end` (implicit +1 step) would yield an EMPTY range, zero the reserved
    // count, and make the row-count guard reject every row. A single-partition (no-skew)
    // write must commit the expected descending values.
    withTable("target") {
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        spark.sql(createTargetTableStatement(Seq(
          "ids BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY -3)",
          "values INT")))
        Seq(1, 2, 3).toDF("values").repartition(1)
          .write.format("delta").mode("append").save(tempPath)
        val ids = readDeltaTable(tempPath).select("ids").collect().map(_.getLong(0)).sorted
        assert(ids.toSeq === Seq(94L, 97L, 100L),
          s"Negative-step write must emit 100, 97, 94; got ${ids.mkString(", ")}")
      }
    }
  }

  test("multiple non-empty partitions reserve disjoint ranges (no cross-partition collision)") {
    // Each task reserves its OWN range from the driver, so concurrent non-empty partitions never
    // emit overlapping values.
    withTable("target") {
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        spark.sql(createTargetTableStatement(Seq(
          "ids BIGINT GENERATED ALWAYS AS IDENTITY",
          "values INT")))
        // Spread 100 rows across 4 partitions by a varying key, so several tasks generate at once.
        (1 to 100).toDF("values").repartition(4, $"values")
          .write.format("delta").mode("append").save(tempPath)
        val ids = readDeltaTable(tempPath).select("ids").collect().map(_.getLong(0))
        assert(ids.length === 100, s"all 100 rows must be written; got ${ids.length}")
        assert(ids.distinct.length === 100,
          s"distinct tasks must reserve disjoint ranges; " +
            s"got ${ids.length - ids.distinct.length} dups")
        assert(ids.forall(_ >= 1L), s"identity values start at 1; got min ${ids.min}")
      }
    }
  }


  test("INSERT into a CIC table with two identity columns reserves and writes both") {
    // Exercises the multi-column reservation loop (per-column slots + all-or-nothing
    // pre-scan) end-to-end through a write, which the CREATE-TABLE-only stamping test does
    // not cover.
    withTable("target") {
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        spark.sql(createTargetTableStatement(Seq(
          "id_a BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)",
          "id_b BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1000 INCREMENT BY -10)",
          "values INT")))
        Seq(1, 2, 3).toDF("values").repartition(1)
          .write.format("delta").mode("append").save(tempPath)
        val rows = readDeltaTable(tempPath).select("id_a", "id_b").collect()
        val aVals = rows.map(_.getLong(0)).sorted
        val bVals = rows.map(_.getLong(1)).sorted
        assert(aVals.toSeq === Seq(1L, 2L, 3L), s"id_a must be 1,2,3; got ${aVals.mkString(",")}")
        // bVals is sorted ascending; the descending sequence 1000,990,980 sorts to 980,990,1000.
        assert(bVals.toSeq === Seq(980L, 990L, 1000L),
          s"id_b must be 980,990,1000; got ${bVals.mkString(",")}")
        // The negative step must round-trip into the recorded schema with its sign.
        val idBInfo = IdentityColumn.getIdentityInfo(deltaLog.update().metadata.schema("id_b"))
        assert(idBInfo.step === -10L, s"id_b recorded step must be -10; got ${idBInfo.step}")
        assert(idBInfo.start === 1000L, s"id_b recorded start must be 1000; got ${idBInfo.start}")
      }
    }
  }

  test("INSERT into a CIC table with two identity columns both exhaust their initial range") {
    // Forces both columns to reserve more than once: tiny initial reserve + enough rows on one
    // partition. Exercises the per-column reserve-more loop in reserveValuesViaService and
    // confirms that column A's sequenceId is never crossed into column B's mid-write reserve.
    withTable("target") {
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        spark.sql(createTargetTableStatement(Seq(
          "id_a BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)",
          "id_b BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1000 INCREMENT BY -10)",
          "values INT")))
        val numRows = 10
        // All rows on one partition so each column's generator exhausts its small initial range
        // and must reserve more mid-write.
        spark.range(numRows).select($"id".cast("int").as("values")).repartition(1, lit(0))
          .write.format("delta").mode("append").save(tempPath)
        val rows = readDeltaTable(tempPath).select("id_a", "id_b").collect()
        val aVals = rows.map(_.getLong(0)).sorted
        val bVals = rows.map(_.getLong(1)).sorted
        assert(aVals.length === numRows, s"id_a: expected $numRows rows; got ${aVals.length}")
        assert(bVals.length === numRows, s"id_b: expected $numRows rows; got ${bVals.length}")
        assert(aVals.distinct.length === numRows,
          s"id_a must be distinct; got ${aVals.mkString(",")}")
        assert(bVals.distinct.length === numRows,
          s"id_b must be distinct; got ${bVals.mkString(",")}")
        val expectedAVals = (1 to numRows).map(_.toLong)
        assert(aVals.toSeq === expectedAVals,
          s"id_a must be ${expectedAVals.mkString(",")}; got ${aVals.mkString(",")}")
        val expectedBVals = (0 until numRows).map(i => 1000L - i * 10L).sorted
        assert(bVals.toSeq === expectedBVals,
          s"id_b must be ${expectedBVals.mkString(",")}; got ${bVals.mkString(",")}")
        // The negative step must round-trip into the recorded schema with its sign.
        val idBInfo = IdentityColumn.getIdentityInfo(deltaLog.update().metadata.schema("id_b"))
        assert(idBInfo.step === -10L, s"id_b recorded step must be -10; got ${idBInfo.step}")
        assert(idBInfo.start === 1000L, s"id_b recorded start must be 1000; got ${idBInfo.start}")
      }
    }
  }

  test("zero-count reservation is a no-op success (service backend)") {
    // An empty MERGE source reserves 0 values. The service contract requires count > 0, so
    // reserveValuesForIdentityColumns must NOT call reserveIds; it registers a degenerate
    // single-point slot (like the metadata-domain path) so the downstream generator rewrite
    // and the write-path guards still see a populated slot. No value is ever emitted from it.
    withTable("target") {
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        spark.sql(createTargetTableStatement(Seq(
          "ids BIGINT GENERATED ALWAYS AS IDENTITY",
          "values INT")))
        val reservation =
          new TestServiceBackedIdentityColumnReservation(deltaLog, None, spark)
        val reservesBefore = localService.reserveIdsCount

        reservation.reserveValuesForIdentityColumns(numRows = 0L)

        val slot = reservation.reservedSlots("ids")
        assert(slot.start === 1L && slot.end === 1L,
          s"zero-count reservation must register a degenerate [start, start] slot; got $slot")
        assert(localService.reserveIdsCount === reservesBefore,
          "zero-count reservation must NOT call the service")
      }
    }
  }

  test("empty-source MERGE commits cleanly (service backend, zero reservation)") {
    // End-to-end regression: an empty source means numSourceRows == 0, so the reservation
    // is for 0 values. Before the zero-count guard this threw "reserve count must be
    // positive"; now it must commit as a no-op.
    withTable("target") {
      withTempView("source") {
        withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
          spark.sql(createTargetTableStatement(Seq(
            "ids BIGINT GENERATED ALWAYS AS IDENTITY",
            "values INT")))
          Seq.empty[Int].toDF("values").createOrReplaceTempView("source")
          val reservesBefore = localService.reserveIdsCount
          executeMerge(
            tgt = s"delta.`$tempPath` AS t",
            src = "source s",
            cond = "t.values = s.values",
            clauses = insert("(values) VALUES (s.values)"))
          val reservesAfter = localService.reserveIdsCount
          assert(readDeltaTable(tempPath).count() === 0L,
            "empty-source MERGE must commit no rows")
          assert(reservesAfter === reservesBefore,
            "An empty-source MERGE must not reserve identity values from the service.")
        }
      }
    }
  }


  test("reserve continues over a larger skewed write (async refill + partial returns)") {
    // Larger skewed write through the always-on buffer / async refill / partial-return path: the
    // driver may hand out shorter-than-requested ranges and top up on a background thread. The
    // write must still commit distinct, complete identity values.
    withTable("target") {
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        spark.sql(createTargetTableStatement(Seq(
          "ids BIGINT GENERATED ALWAYS AS IDENTITY",
          "values INT")))
        (1 to 64).toDF("values").repartition(8, lit(0))
          .write.format("delta").mode("append").save(tempPath)
        val ids = readDeltaTable(tempPath).select("ids").collect().map(_.getLong(0))
        assert(ids.length === 64, s"all 64 rows must be written; got ${ids.length}")
        assert(ids.distinct.length === 64,
          s"reserve-continue must not produce duplicate ids; got ${ids.sorted.mkString(", ")}")
        assert(ids.forall(_ >= 1L), s"identity values start at 1; got min ${ids.min}")
      }
    }
  }


  test("a write against a deleted sequence fails and commits no rows (service backend)") {
    withTable("target") {
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
        spark.sql(createTargetTableStatement(Seq(
          "ids BIGINT GENERATED ALWAYS AS IDENTITY",
          "values INT")))
        // The schema now carries a sequenceId, but wipe the service so it no longer exists.
        localService.reset()
        intercept[Exception] {
          spark.sql(s"INSERT INTO delta.`$tempPath` (values) VALUES (10), (20), (30)")
        }
        assert(readDeltaTable(tempPath).count() === 0L,
          "a write that fails to reserve from a deleted sequence must commit no rows")
      }
    }
  }

  test("a MERGE against a deleted sequence fails and commits no rows (service backend)") {
    // MERGE counterpart of the deleted-sequence test: the reserve-more path is shared, but MERGE
    // routes through a different write path, so cover it too. Wiping the service makes the
    // executor's first reserve fail; the MERGE must abort and leave no rows.
    withTable("target") {
      withTempView("source") {
        withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "true") {
          spark.sql(createTargetTableStatement(Seq(
            "ids BIGINT GENERATED ALWAYS AS IDENTITY",
            "values INT")))
          Seq(1, 2, 3).toDF("values").repartition(1).createOrReplaceTempView("source")
          localService.reset()
          intercept[Exception] {
            executeMerge(
              tgt = s"delta.`$tempPath` AS t",
              src = "source s",
              cond = "t.values = s.values",
              clauses = insert("(values) VALUES (s.values)"))
          }
          assert(readDeltaTable(tempPath).count() === 0L,
            "a MERGE that fails to reserve from a deleted sequence must commit no rows")
        }
      }
    }
  }

  private val killSwitchError = "DELTA_CONCURRENT_IDENTITY_COLUMN_DISABLED"
  // The tableId is a per-table random UUID (and on the CREATE path the table never commits, so
  // there is no stable id to read); assert it as a regex via matchPVals and pin only the
  // operation, which is what the kill-switch error actually guarantees.
  private def genKillParameters(operation: String): Map[String, String] = Map(
    "operation" -> operation,
    "tableId" -> "[0-9a-f-]+"
  )

  test("kill switch blocks identity-generating writes to a service-backed table") {
    // identityColumn.concurrent.enabled = false is an emergency stop, not a router: every
    // identity-generating write to a stamped table must fail loud with the kill-switch
    // error and commit nothing.
    withTable("target") {
      withTempView("source") {
        spark.sql(createTargetTableStatement(Seq(
          "ids BIGINT GENERATED ALWAYS AS IDENTITY",
          "values INT")))
        Seq(1, 2, 3).toDF("values").repartition(1).createOrReplaceTempView("source")
        // Read the expected parameters AFTER the target table exists, so tableId is the
        // just-created table's id (not a stale id from a prior test's deltaLog state).
        val killParameters = genKillParameters("write to")

        withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "false") {
          val insertCause = cicReservationCause(intercept[Exception] {
            spark.sql(s"INSERT INTO delta.`$tempPath` (values) VALUES (10)")
          })
          checkError(
            insertCause,
            condition = killSwitchError,
            sqlState = Some("0A000"),
            parameters = killParameters,
            matchPVals = true)

          val mergeCause = cicReservationCause(intercept[Exception] {
            executeMerge(
              tgt = s"delta.`$tempPath` AS t",
              src = "source s",
              cond = "t.values = s.values",
              clauses = insert("(values) VALUES (s.values)"))
          })
          checkError(
            mergeCause,
            condition = killSwitchError,
            sqlState = Some("0A000"),
            parameters = killParameters,
            matchPVals = true)
        }
        assert(readDeltaTable(tempPath).count() === 0L,
          "Writes blocked by the kill switch must not commit rows.")
      }
    }
  }

  test("kill switch blocks CREATE TABLE with the CIC feature") {
    // Refusing (rather than skipping the stamp) avoids creating an unstamped legacy table
    // whose writes would later fail with a misleading missing-stamp error.
    withTable("target") {
      withSQLConf(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key -> "false") {
        val createdSequencesBeforeCreate = localService.createSequenceCount
        val cause = cicReservationCause(intercept[Exception] {
          spark.sql(createTargetTableStatement(Seq(
            "ids BIGINT GENERATED ALWAYS AS IDENTITY",
            "values INT")))
        })
        // The create is blocked, so its tableId is the id the (never-created) table would have
        // used, which is not observable via deltaLog here. Assert the condition/sqlState and the
        // operation exactly; take the tableId from the error itself.
        val actualParams = cause.getMessageParameters.asScala.toMap
        assert(actualParams.get("operation").contains("create"),
          s"kill-switch error must report operation=create; got ${actualParams.get("operation")}")
        checkError(
          cause,
          condition = killSwitchError,
          sqlState = "0A000",
          parameters = actualParams)
        assert(localService.createSequenceCount === createdSequencesBeforeCreate,
          "CREATE must not create a sequence when the kill switch is on.")
      }
    }
  }


}

/**
 * Test-only [[IdentityColumnReservation]] that exposes the reservation class directly without
 * going through a real DML executor. Wires the suite's shared
 * [[LocalIdentitySequenceService]] (the same one the injected backend forwards to) so the
 * service-backed reservation path is exercised in isolation, and its invocation counts line up
 * with the suite's assertions. Used to pin contracts that live on the class itself (e.g. the
 * [[reservedSlots]] cache-reuse / clear path).
 */
private class TestServiceBackedIdentityColumnReservation(
    deltaLog: DeltaLog,
    catalog: Option[CatalogTable],
    spark: SparkSession)
  extends IdentityColumnReservation(deltaLog, catalog, spark,
    Some(SharedLocalIdentitySequenceService.shared))
