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

package org.apache.spark.sql.delta.test

import java.io.File

import org.apache.spark.sql.delta.{
  DeltaColumnMappingEnableIdMode,
  DeltaColumnMappingEnableNameMode,
  DeltaLog,
  StreamingSchemaEvolutionSuiteBase
}
import org.apache.spark.sql.delta.sources.{
  DeltaSourceMetadataTrackingLog,
  DeltaSQLConf,
  PersistedMetadata
}

import org.apache.spark.sql.{DataFrame, Row}

/**
 * Verifies that a single stream's schema tracking log is interchangeable between the V1 (legacy
 * DeltaSource) and V2 (Kernel-based DeltaV2MicroBatchStream) connectors. Each test alternates the
 * connector across stream restarts that share the same checkpoint and schema location, exercising
 * schema log initialization, non-additive evolution, and additive-then-non-additive sequences.
 */
trait V1V2SourceSchemaLogCompatibilitySuiteBase
    extends StreamingSchemaEvolutionSuiteBase {

  import testImplicits._

  override protected def isCdcTest: Boolean = false

  override protected def runOnlyTests: Seq[String] = Seq(
    // Scenario tests — multi-step flows where each connector picks up what the other left.
    "alternating connectors with no schema evolution leaves the schema log untouched",
    "V1-initialized schema log can be read by V2",
    "V2-initialized schema log can be read by V1",
    "non-additive evolution (rename) written by V1 is consumed by V2",
    "non-additive evolution (drop) written by V1 is consumed by V2",
    "non-additive evolution (rename) written by V2 is consumed by V1",
    "non-additive evolution (drop) written by V2 is consumed by V1",
    "alternating connectors (V1-V2-V1-V2) across additive then non-additive evolution (rename)",
    "alternating connectors (V1-V2-V1-V2) across additive then non-additive evolution (drop)",
    "alternating connectors (V2-V1-V2-V1) across additive then non-additive evolution (rename)",
    "alternating connectors (V2-V1-V2-V1) across additive then non-additive evolution (drop)",

    // Equivalence tests — V1 and V2 produce/consume byte-identical log files for the same input.
    "V1 and V2 write equivalent schema tracking log entries (rename)",
    "V1 and V2 write equivalent schema tracking log entries (drop)",
    "V1 and V2 mergers produce equivalent merged schema log entries",
    "V1 and V2 read identical pre-seeded schema log to the same final state (rename)",
    "V1 and V2 read identical pre-seeded schema log to the same final state (drop)"
  )

  // V2_ENABLE_MODE is the single source of truth: `useDsv2` (read by `loadStreamWithOptions`)
  // is derived from the conf so withV1/withV2 just flip the conf via `withSQLConf`.
  override protected def useDsv2: Boolean =
    spark.conf.get(DeltaSQLConf.V2_ENABLE_MODE.key, "NONE") == "STRICT"

  protected def withConnector[T](useV2: Boolean)(f: => T): T =
    withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> (if (useV2) "STRICT" else "NONE"))(f)

  protected def withV1[T](f: => T): T = withConnector(useV2 = false)(f)
  protected def withV2[T](f: => T): T = withConnector(useV2 = true)(f)

  // SparkTable (V2) is read-only; route DDL/DML through V1 regardless of the active reader.
  override protected def executeDml(sqlText: String): Unit = {
    withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE") {
      sql(sqlText)
    }
  }

  // ===========================================================================================
  // Strict-equivalence helpers
  // ===========================================================================================

  /** Construct a tracking log instance pointing at an arbitrary root location. */
  private def schemaLogAt(rootLocation: String)(implicit log: DeltaLog)
      : DeltaSourceMetadataTrackingLog =
    DeltaSourceMetadataTrackingLog.create(
      spark,
      rootLocation,
      log.unsafeVolatileTableId,
      log.dataPath.toString,
      parameters = Map.empty)

  /** Strip `sourceMetadataPath` - the only field that legitimately differs across checkpoints. */
  private def normalize(metadata: PersistedMetadata): PersistedMetadata =
    metadata.copy(sourceMetadataPath = "")

  /**
   * Assert entry-for-entry equivalence between the V1- and V2-written tracking logs at the
   * given schema-log roots, after normalizing away the per-checkpoint `sourceMetadataPath`.
   * Returns the (now-confirmed equivalent) entries so call sites can apply tail assertions
   * such as `entries.last.deltaCommitVersion`.
   */
  private def assertEquivalentEntries(
      v1SchemaLocation: String,
      v2SchemaLocation: String,
      context: String)(implicit log: DeltaLog): Seq[PersistedMetadata] = {
    def entriesAt(location: String): Seq[PersistedMetadata] = {
      val schemaLog = schemaLogAt(location)
      (0L to schemaLog.getCurrentTrackedSeqNum).flatMap(schemaLog.getTrackedMetadataAtSeqNum)
    }
    val v1Entries = entriesAt(v1SchemaLocation)
    val v2Entries = entriesAt(v2SchemaLocation)
    assert(v1Entries.size == v2Entries.size,
      s"$context: V1 has ${v1Entries.size} entries, V2 has ${v2Entries.size}")
    v1Entries.zip(v2Entries).foreach { case (v1Entry, v2Entry) =>
      assert(normalize(v1Entry) == normalize(v2Entry),
        s"$context entry mismatch:\n  V1: $v1Entry\n  V2: $v2Entry")
    }
    v1Entries
  }

  /**
   * Parameterizes non-additive evolution over rename vs drop. `onStarter` runs against the
   * (a, b) starter; `afterAdditive` runs against the (a, b, c) schema after `addColumn("c")`.
   * The row builders describe the post-evolution row shape for `CheckAnswer`.
   */
  protected case class NonAdditiveChange(
      kind: String,
      onStarter: DeltaLog => Unit,
      afterAdditive: DeltaLog => Unit,
      rowAfterStarter: Int => Row,
      rowAfterAdditive: Int => Row)

  protected val nonAdditiveChanges: Seq[NonAdditiveChange] = Seq(
    NonAdditiveChange(
      kind = "rename",
      onStarter = log => renameColumn("b", "c")(log),
      afterAdditive = log => renameColumn("c", "d")(log),
      rowAfterStarter = i => Row(i.toString, i.toString),
      rowAfterAdditive = i => Row(i.toString, i.toString, i.toString)),
    NonAdditiveChange(
      kind = "drop",
      onStarter = log => dropColumn("b")(log),
      afterAdditive = log => dropColumn("c")(log),
      rowAfterStarter = i => Row(i.toString),
      rowAfterAdditive = i => Row(i.toString, i.toString))
  )

  // ===========================================================================================
  // Tests
  // ===========================================================================================

  test("alternating connectors with no schema evolution leaves the schema log untouched") {
    withStarterTable { implicit log =>
      def df: DataFrame = readStream(schemaLocation = Some(getDefaultSchemaLocation.toString))

      // V1 initializes the schema log.
      withV1 {
        testStream(df)(
          StartStream(checkpointLocation = getDefaultCheckpoint.toString),
          ProcessAllAvailable(),
          CheckAnswer((-1 until 5).map(i => (i.toString, i.toString)): _*)
        )
      }
      val initialLogVersion =
        getDefaultSchemaLog().getCurrentTrackedMetadata.get.deltaCommitVersion

      // Bounce V1 -> V2 -> V1 -> V2, appending data between each restart. With no evolution,
      // neither connector should write a new schema-log entry on restart: the current entry
      // stays at `initialLogVersion` and the previous entry remains absent.
      Seq((true, 5 until 10), (false, 10 until 15), (true, 15 until 20)).foreach {
        case (useV2, dataRange) =>
          addData(dataRange)
          withConnector(useV2) {
            testStream(df)(
              StartStream(checkpointLocation = getDefaultCheckpoint.toString),
              ProcessAllAvailable(),
              CheckAnswer(dataRange.map(i => (i.toString, i.toString)): _*)
            )
          }
      }
      assert(getDefaultSchemaLog().getCurrentTrackedMetadata.get.deltaCommitVersion ==
        initialLogVersion)
      assert(getDefaultSchemaLog().getPreviousTrackedMetadata.isEmpty)
    }
  }

  Seq(("V1", "V2"), ("V2", "V1")).foreach { case (initName, resumeName) =>
    val initUseV2 = initName == "V2"
    val resumeUseV2 = resumeName == "V2"
    test(s"$initName-initialized schema log can be read by $resumeName") {
      withStarterTable { implicit log =>
        def df: DataFrame = readStream(schemaLocation = Some(getDefaultSchemaLocation.toString))

        // Initiator initializes the schema log on first run.
        withConnector(initUseV2) {
          testStream(df)(
            StartStream(checkpointLocation = getDefaultCheckpoint.toString),
            ProcessAllAvailable(),
            CheckAnswer((-1 until 5).map(i => (i.toString, i.toString)): _*)
          )
        }
        val v0 = log.update().version
        assert(getDefaultSchemaLog().getCurrentTrackedMetadata.get.deltaCommitVersion == v0)

        addData(5 until 10)

        // Resumer reads from the initiator's checkpoint and schema log.
        withConnector(resumeUseV2) {
          testStream(df)(
            StartStream(checkpointLocation = getDefaultCheckpoint.toString),
            ProcessAllAvailable(),
            CheckAnswer((5 until 10).map(i => (i.toString, i.toString)): _*)
          )
        }
      }
    }
  }

  nonAdditiveChanges.foreach { change =>
    test(s"non-additive evolution (${change.kind}) written by V1 is consumed by V2") {
      withStarterTable { implicit log =>
        def df: DataFrame = readStream(schemaLocation = Some(getDefaultSchemaLocation.toString))

        withV1 {
          testStream(df)(
            StartStream(checkpointLocation = getDefaultCheckpoint.toString),
            ProcessAllAvailable(),
            CheckAnswer((-1 until 5).map(i => (i.toString, i.toString)): _*)
          )
        }

        change.onStarter(log)
        val evolutionVersion = log.update().version
        addData(5 until 10)

        // V1 hits the change and writes the new metadata into the schema log.
        withV1 {
          testStream(df)(
            StartStream(checkpointLocation = getDefaultCheckpoint.toString),
            ProcessAllAvailableIgnoreError,
            CheckAnswer(Nil: _*),
            ExpectMetadataEvolutionException
          )
        }
        assert(getDefaultSchemaLog().getCurrentTrackedMetadata.get.deltaCommitVersion ==
          evolutionVersion)

        // V2 starts under the V1-evolved schema and processes the post-evolution data.
        withV2 {
          testStream(df)(
            StartStream(checkpointLocation = getDefaultCheckpoint.toString),
            ProcessAllAvailable(),
            CheckAnswer((5 until 10).map(change.rowAfterStarter): _*)
          )
        }
      }
    }
  }

  nonAdditiveChanges.foreach { change =>
    test(s"non-additive evolution (${change.kind}) written by V2 is consumed by V1") {
      withStarterTable { implicit log =>
        def df: DataFrame = readStream(schemaLocation = Some(getDefaultSchemaLocation.toString))

        withV2 {
          testStream(df)(
            StartStream(checkpointLocation = getDefaultCheckpoint.toString),
            ProcessAllAvailable(),
            CheckAnswer((-1 until 5).map(i => (i.toString, i.toString)): _*)
          )
        }

        change.onStarter(log)
        val evolutionVersion = log.update().version
        addData(5 until 10)

        // V2 hits the change and writes the new metadata into the schema log.
        withV2 {
          testStream(df)(
            StartStream(checkpointLocation = getDefaultCheckpoint.toString),
            ProcessAllAvailableIgnoreError,
            CheckAnswer(Nil: _*),
            ExpectMetadataEvolutionException
          )
        }
        assert(getDefaultSchemaLog().getCurrentTrackedMetadata.get.deltaCommitVersion ==
          evolutionVersion)

        // V1 starts under the V2-evolved schema and processes the post-evolution data.
        withV1 {
          testStream(df)(
            StartStream(checkpointLocation = getDefaultCheckpoint.toString),
            ProcessAllAvailable(),
            CheckAnswer((5 until 10).map(change.rowAfterStarter): _*)
          )
        }
      }
    }
  }

  // Connector order for the alternating-evolution test. `initUseV2` runs init + non-additive
  // stages; `pickupUseV2` runs additive + final-drain stages.
  protected case class ConnectorAlternation(
      name: String,
      initUseV2: Boolean,
      pickupUseV2: Boolean)

  protected val connectorAlternations: Seq[ConnectorAlternation] = Seq(
    ConnectorAlternation("V1-V2-V1-V2", initUseV2 = false, pickupUseV2 = true),
    ConnectorAlternation("V2-V1-V2-V1", initUseV2 = true, pickupUseV2 = false)
  )

  connectorAlternations.foreach { alt =>
    nonAdditiveChanges.foreach { change =>
      test(s"alternating connectors (${alt.name}) " +
          s"across additive then non-additive evolution (${change.kind})") {
        withStarterTable { implicit log =>
          def df: DataFrame = readStream(schemaLocation = Some(getDefaultSchemaLocation.toString))

          // 1) Initiator initializes the schema log on the starter schema.
          withConnector(alt.initUseV2) {
            testStream(df)(
              StartStream(checkpointLocation = getDefaultCheckpoint.toString),
              ProcessAllAvailable(),
              CheckAnswer((-1 until 5).map(i => (i.toString, i.toString)): _*)
            )
          }

          // 2) Add column, then the pickup connector writes the additive evolution entry.
          addColumn("c")
          val additiveVersion = log.update().version
          addData(5 until 10)

          withConnector(alt.pickupUseV2) {
            testStream(df)(
              StartStream(checkpointLocation = getDefaultCheckpoint.toString),
              ProcessAllAvailableIgnoreError,
              ExpectMetadataEvolutionException
            )
          }
          assert(getDefaultSchemaLog().getCurrentTrackedMetadata.get.deltaCommitVersion ==
            additiveVersion)

          withConnector(alt.pickupUseV2) {
            testStream(df)(
              StartStream(checkpointLocation = getDefaultCheckpoint.toString),
              ProcessAllAvailable(),
              CheckAnswer((5 until 10).map(i => (i.toString, i.toString, i.toString)): _*)
            )
          }

          // 3) Apply the non-additive change; the initiator picks it up.
          change.afterAdditive(log)
          val nonAdditiveVersion = log.update().version
          addData(10 until 15)

          withConnector(alt.initUseV2) {
            testStream(df)(
              StartStream(checkpointLocation = getDefaultCheckpoint.toString),
              ProcessAllAvailableIgnoreError,
              ExpectMetadataEvolutionException
            )
          }
          assert(getDefaultSchemaLog().getCurrentTrackedMetadata.get.deltaCommitVersion ==
            nonAdditiveVersion)

          // 4) Flip back to the pickup connector to drain the post-evolution batch.
          withConnector(alt.pickupUseV2) {
            testStream(df)(
              StartStream(checkpointLocation = getDefaultCheckpoint.toString),
              ProcessAllAvailable(),
              CheckAnswer((10 until 15).map(change.rowAfterAdditive): _*)
            )
          }
        }
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // Strict equivalence: write path
  // -------------------------------------------------------------------------------------------

  nonAdditiveChanges.foreach { change =>
    test(s"V1 and V2 write equivalent schema tracking log entries (${change.kind})") {
      withStarterTable { implicit log =>
        // Set up a deterministic evolution sequence: additive add column "c", then the
        // parameterized non-additive change applied to that 3-column schema.
        addColumn("c")
        addData(5 until 10)
        change.afterAdditive(log)
        val evolutionVersion = log.update().version
        addData(10 until 15)

        withTempDir { ckpt1 =>
          withTempDir { ckpt2 =>
            def schemaLocation(ckptDir: File): String =
              new File(ckptDir, "_schema_location").getCanonicalPath

            // Drive `useV2` from a fresh checkpoint past both evolutions.
            def driveStream(useV2: Boolean, ckptDir: File): Unit = {
              val checkpointPath = ckptDir.getCanonicalPath
              val schemaPath = schemaLocation(ckptDir)
              def df: DataFrame =
                readStream(schemaLocation = Some(schemaPath), startingVersion = Some(0L))

              // 1) Init the schema log.
              withConnector(useV2) {
                testStream(df)(
                  StartStream(checkpointLocation = checkpointPath),
                  ProcessAllAvailableIgnoreError,
                  ExpectMetadataEvolutionExceptionFromInitialization
                )
              }
              // 2) Additive change.
              withConnector(useV2) {
                testStream(df)(
                  StartStream(checkpointLocation = checkpointPath),
                  ProcessAllAvailableIgnoreError,
                  ExpectMetadataEvolutionException
                )
              }
              // 3) Non-additive change.
              withConnector(useV2) {
                testStream(df)(
                  StartStream(checkpointLocation = checkpointPath),
                  ProcessAllAvailableIgnoreError,
                  ExpectMetadataEvolutionException
                )
              }
              // 4) Final drain.
              withConnector(useV2) {
                testStream(df)(
                  StartStream(checkpointLocation = checkpointPath),
                  ProcessAllAvailable(),
                  CheckAnswer((10 until 15).map(change.rowAfterAdditive): _*)
                )
              }
            }

            driveStream(useV2 = false, ckpt1)
            driveStream(useV2 = true, ckpt2)

            val entries = assertEquivalentEntries(
              schemaLocation(ckpt1), schemaLocation(ckpt2), context = "write path")
            assert(entries.last.deltaCommitVersion == evolutionVersion)
          }
        }
      }
    }
  }

  test("V1 and V2 mergers produce equivalent merged schema log entries") {
    withStarterTable { implicit log =>
      // Three back-to-back metadata-only commits (rename + drop + add) so the merger has to
      // fold a mixed sequence, not just a stream of renames.
      val firstMetadataVersion = log.update().version + 1L
      renameColumn("b", "c")  // (a, b) -> (a, c)
      dropColumn("c")         // (a, c) -> (a)
      addColumn("e")          // (a)    -> (a, e)
      val mergedTargetVersion = log.update().version
      // Post-merge data so the final stage can prove the merged schema is actually usable.
      addData(5 until 10)

      withTempDir { ckpt1 =>
        withTempDir { ckpt2 =>
          def schemaLocation(ckptDir: File): String =
            new File(ckptDir, "_schema_location").getCanonicalPath

          // startingVersion skips starter rows so the merged-schema drain has a clean (a, e)
          // row shape unrelated to the (a, b) starter.
          def driveStream(useV2: Boolean, ckptDir: File): Unit = {
            val checkpointPath = ckptDir.getCanonicalPath
            val schemaPath = schemaLocation(ckptDir)
            def df: DataFrame =
              readStream(
                schemaLocation = Some(schemaPath),
                startingVersion = Some(firstMetadataVersion))

            // 1) Init the schema log at the first metadata version.
            withConnector(useV2) {
              testStream(df)(
                StartStream(checkpointLocation = checkpointPath),
                ProcessAllAvailableIgnoreError,
                ExpectMetadataEvolutionExceptionFromInitialization
              )
            }
            // 2) Merger folds the remaining consecutive commits; stream drains under merged schema.
            withConnector(useV2) {
              testStream(df)(
                StartStream(checkpointLocation = checkpointPath),
                ProcessAllAvailable(),
                CheckAnswer((5 until 10).map(i => (i.toString, i.toString)): _*)
              )
            }
          }

          driveStream(useV2 = false, ckpt1)
          driveStream(useV2 = true, ckpt2)

          val entries = assertEquivalentEntries(
            schemaLocation(ckpt1), schemaLocation(ckpt2), context = "merger")
          // Merged entry lands at the last metadata commit, with the replaceCurrent marker.
          assert(entries.last.deltaCommitVersion == mergedTargetVersion,
            s"expected merger to land at $mergedTargetVersion, got ${entries.last}")
          assert(entries.last.previousMetadataSeqNum.isDefined,
            s"missing replaceCurrent marker on merged entry: ${entries.last}")
        }
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // Strict equivalence: read path
  // -------------------------------------------------------------------------------------------

  nonAdditiveChanges.foreach { change =>
    test(s"V1 and V2 read identical pre-seeded schema log to the same final state " +
        s"(${change.kind})") {
      withStarterTable { implicit log =>
        // Apply the parameterized change after the seed point so each connector has work to do.
        val seedSnapshot = log.update()
        val seedVersion = seedSnapshot.version
        change.onStarter(log)
        val evolutionVersion = log.update().version
        addData(5 until 10)

        withTempDir { ckpt1 =>
          withTempDir { ckpt2 =>
            def schemaLocation(ckptDir: File): String =
              new File(ckptDir, "_schema_location").getCanonicalPath

            // The metadata path check requires the seed's sourceMetadataPath to match what the
            // executing stream computes; disable it so we can pre-seed without prediction.
            val seedConfs =
              DeltaSQLConf.DELTA_STREAMING_SCHEMA_TRACKING_METADATA_PATH_CHECK_ENABLED.key ->
                "false"

            // Pre-seed both schema-log directories with byte-identical content: the pre-evolution
            // schema (a, b) at the starter table's latest version.
            Seq(ckpt1, ckpt2).foreach { ckptDir =>
              withSQLConf(seedConfs) {
                val seed = PersistedMetadata(
                  log.unsafeVolatileTableId,
                  seedVersion,
                  makeMetadata(seedSnapshot.schema, seedSnapshot.metadata.partitionSchema),
                  seedSnapshot.protocol,
                  sourceMetadataPath = "")
                schemaLogAt(schemaLocation(ckptDir)).writeNewMetadata(seed)
              }
            }

            // Confirm the seeds are identical on disk, modulo sourceMetadataPath (empty in both).
            assertEquivalentEntries(schemaLocation(ckpt1), schemaLocation(ckpt2), context = "seed")

            // Each connector reads from its own checkpoint, hits the evolution barrier (which
            // writes the new entry to its log), then re-starts to drain the post-evolution
            // data through the freshly-adopted schema.
            def runThroughEvolution(useV2: Boolean, ckptDir: File): Unit = {
              val checkpointPath = ckptDir.getCanonicalPath
              val schemaPath = schemaLocation(ckptDir)
              def df: DataFrame =
                readStream(schemaLocation = Some(schemaPath), startingVersion = Some(0L))

              // 1) Hit the evolution barrier; the new entry gets written to the log.
              withConnector(useV2) {
                withSQLConf(seedConfs) {
                  testStream(df)(
                    StartStream(checkpointLocation = checkpointPath),
                    ProcessAllAvailableIgnoreError,
                    ExpectMetadataEvolutionException
                  )
                }
              }
              // 2) Post-evolution drain: prove the freshly-adopted schema is usable for reads.
              withConnector(useV2) {
                withSQLConf(seedConfs) {
                  testStream(df)(
                    StartStream(checkpointLocation = checkpointPath),
                    ProcessAllAvailable(),
                    CheckAnswer((5 until 10).map(change.rowAfterStarter): _*)
                  )
                }
              }
            }

            runThroughEvolution(useV2 = false, ckpt1)
            runThroughEvolution(useV2 = true, ckpt2)

            // Final tracking log state must match entry-for-entry.
            val entries = assertEquivalentEntries(
              schemaLocation(ckpt1), schemaLocation(ckpt2), context = "read path")
            // Both connectors converged on the evolution version.
            assert(entries.last.deltaCommitVersion == evolutionVersion)
          }
        }
      }
    }
  }
}

class V1V2SourceSchemaLogCompatibilityNameColumnMappingSuite
  extends V1V2SourceSchemaLogCompatibilitySuiteBase
    with DeltaColumnMappingEnableNameMode

class V1V2SourceSchemaLogCompatibilityIdColumnMappingSuite
  extends V1V2SourceSchemaLogCompatibilitySuiteBase
    with DeltaColumnMappingEnableIdMode
