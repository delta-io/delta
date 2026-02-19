import scala.util.hashing.MurmurHash3

import sbt.Keys._
import sbt._

object TestParallelization {

  lazy val numShardsOpt = sys.env.get("NUM_SHARDS").map(_.toInt)
  lazy val shardIdOpt = sys.env.get("SHARD_ID").map(_.toInt)
  lazy val testParallelismOpt = sys.env.get("TEST_PARALLELISM_COUNT").map(_.toInt)

  lazy val settings = {
    println(
      s"Test parallelization settings: numShardsOpt=$numShardsOpt, " +
          s"shardIdOpt=$shardIdOpt, testParallelismOpt=$testParallelismOpt"
    )
    if ((numShardsOpt.exists(_ > 1) && shardIdOpt.exists(_ >= 0)) ||
          testParallelismOpt.exists(_ > 1)) {
      customTestGroupingSettings ++ simpleGroupingStrategySettings
    } else {
      Seq.empty[Setting[_]]
    }
  }

  /**
   * Replace the default value for Test / testGrouping settingKey and set it to a new value
   * calculated by using the custom Task [[testGroupingStrategy]].
   *
   * Adding these settings to the build will require us to separately provide a value for the
   * TaskKey [[testGroupingStrategy]]
   */
  lazy val customTestGroupingSettings = {
    Seq(
      Test / testGrouping := {
        val tests = (Test / definedTests).value
        val groupingStrategy = (Test / testGroupingStrategy).value
        val grouping = tests.foldLeft(groupingStrategy) {
          case (strategy, testDefinition) => strategy.add(testDefinition)
        }
        val logger = streams.value.log
        logger.info(s"Tests will be grouped in ${grouping.testGroups.size} groups")
        val groups = grouping.testGroups
        groups.foreach { group => logger.info(s"${group.name} contains ${group.tests.size} tests") }
        logger.info(groupingStrategy.toString)
        groups
      }
    )
  }

  /**
   * Sets the Test / testGroupingStrategy Task to an instance of the MinShardGroupDurationStrategy
   */
  lazy val simpleGroupingStrategySettings = Seq(
    Test / forkTestJVMCount := {
      testParallelismOpt.getOrElse(java.lang.Runtime.getRuntime.availableProcessors)
    },
    Test / shardId := { shardIdOpt.getOrElse(0) },
    Test / testGroupingStrategy := {
      val groupsCount = (Test / forkTestJVMCount).value
      val shard = (Test / shardId).value
      val baseJvmDir = baseDirectory.value
      MinShardGroupDurationStrategy(groupsCount, baseJvmDir, shard, defaultForkOptions.value)
    },
    Test / parallelExecution := true,
    Global / concurrentRestrictions := {
      Seq(Tags.limit(Tags.ForkedTestGroup, (Test / forkTestJVMCount).value))
    }
  )

  val shardId = SettingKey[Int]("shard id", "The shard id assigned")

  val forkTestJVMCount = SettingKey[Int](
    "fork test jvm count",
    "The number of separate JVM to use for tests"
  )

  val testGroupingStrategy = TaskKey[GroupingStrategy](
    "test grouping strategy",
    "The strategy to allocate different tests into groups," +
        "potentially using multiple JVMS for their execution"
  )

  private val defaultForkOptions = Def.task {
    ForkOptions(
      javaHome = javaHome.value,
      outputStrategy = outputStrategy.value,
      bootJars = Vector.empty,
      // Use Test/baseDirectory instead of baseDirectory to support modules where these differ
      // (e.g. spark-combined module where Test/baseDirectory points to spark/ source directory)
      workingDirectory = Some((Test / baseDirectory).value),
      runJVMOptions = (Test / javaOptions).value.toVector,
      connectInput = connectInput.value,
      envVars = (Test / envVars).value
    )
  }

  /**
   * Base trait to group tests.
   *
   * By default, SBT will run all tests as if they belong to a single group, but allows tests to be
   * grouped. Setting [[sbt.Keys.testGrouping]] to a list of groups replaces the default
   * single-group definition.
   *
   * When creating an instance of [[sbt.Tests.Group]] it is possible to specify an
   * [[sbt.Tests.TestRunPolicy]]: this parameter can be used to use multiple subprocesses for test
   * execution
   */
  sealed trait GroupingStrategy {

    /**
     * Adds an [[sbt.TestDefinition]] to this GroupingStrategy and returns an updated Grouping
     * Strategy
     */
    def add(testDefinition: TestDefinition): GroupingStrategy

    /** Returns the test groups built from this GroupingStrategy */
    def testGroups: List[Tests.Group]
  }

  /**
   * GreedyHashStrategy is a grouping strategy used to distribute test suites across multiple shards
   * and groups (threads) based on their estimated duration. It aims to balance the test load across
   * the shards and groups by utilizing a greedy assignment algorithm that assigns test suites to
   * the group with the smallest estimated runtime.
   *
   * @param groups The initial mapping of group indices to their respective [[sbt.Tests.Group]]
   *               objects, which hold test definitions.
   * @param shardId The shard ID that this instance is responsible for.
   * @param highDurationTestAssignment Precomputed assignments of high-duration test suites to
   *                                   specific groups within the shard.
   * @param groupRuntimes Array holding the current total runtime for each group within the shard.
   */
  class MinShardGroupDurationStrategy private(
      groups: scala.collection.mutable.Map[Int, Tests.Group],
      shardId: Int,
      highDurationTestAssignment: Array[Set[String]],
      var groupRuntimes: Array[Double]
  ) extends GroupingStrategy {
    import TestParallelization.MinShardGroupDurationStrategy._

    if (shardId < 0 || shardId >= NUM_SHARDS) {
      throw new IllegalArgumentException(
        s"Assigned shard ID $shardId is not between 0 and ${NUM_SHARDS - 1} inclusive")
    }

    lazy val testGroups = groups.values.toList

    override def add(testDefinition: TestDefinition): GroupingStrategy = {
      val testSuiteName = testDefinition.name
      val isHighDurationTest = TOP_N_HIGH_DURATION_TEST_SUITES.exists(_._1 == testSuiteName)

      if (isHighDurationTest) {
        val highDurationTestGroupIndex =
          highDurationTestAssignment.indexWhere(_.contains(testSuiteName))

        if (highDurationTestGroupIndex >= 0) {
          // Case 1: this is a high duration test that was pre-computed in the optimal assignment to
          // belong to this shard. Assign it.
          val duration = TOP_N_HIGH_DURATION_TEST_SUITES.find(_._1 == testSuiteName).get._2

          val currentGroup = groups(highDurationTestGroupIndex)
          val updatedGroup = currentGroup.withTests(currentGroup.tests :+ testDefinition)
          groups(highDurationTestGroupIndex) = updatedGroup

          // Do NOT update groupRuntimes -- this was already included in the initial value of
          // groupRuntimes

          this
        } else {
          // Case 2: this is a high duration test that does NOT belong to this shard. Skip it.
          this
        }
      } else if (math.abs(MurmurHash3.stringHash(testDefinition.name) % NUM_SHARDS) == shardId) {
        // Case 3: this is a normal test that belongs to this shard. Assign it.

        val minDurationGroupIndex = groupRuntimes.zipWithIndex.minBy(_._1)._2
        val currentGroup = groups(minDurationGroupIndex)
        val updatedGroup = currentGroup.withTests(currentGroup.tests :+ testDefinition)
        groups(minDurationGroupIndex) = updatedGroup

        groupRuntimes(minDurationGroupIndex) += AVG_TEST_SUITE_DURATION_EXCLUDING_TOP_N

        this
      } else {
        // Case 4: this is a normal test that does NOT belong to this shard. Skip it.
        this
      }
    }

    override def toString: String = {
      val actualDurationsStr = groupRuntimes.zipWithIndex.map {
        case (actualDuration, groupIndex) =>
          f"  Group $groupIndex: Estimated Duration = $actualDuration%.2f mins, " +
              f"Count = ${groups(groupIndex).tests.size}"
      }.mkString("\n")

      s"""
         |Shard ID: $shardId
         |Suite Group Assignments:
         |$actualDurationsStr
      """.stripMargin
    }
  }

  object MinShardGroupDurationStrategy {

    val NUM_SHARDS = numShardsOpt.getOrElse(1)

    val AVG_TEST_SUITE_DURATION_EXCLUDING_TOP_N = 0.83

    /**
     * 100 slowest test suites and their durations (in minutes).
     *
     * Generated from JUnit XML test reports using:
     *   python3 project/scripts/collect_test_durations.py --pr <N> --top-n 100 --output-scala
     */
    val TOP_N_HIGH_DURATION_TEST_SUITES: List[(String, Double)] = List(
      ("org.apache.spark.sql.delta.DeltaRetentionWithCatalogOwnedBatch1Suite", 22.66),
      ("org.apache.spark.sql.delta.DeltaRetentionSuite", 21.46),
      ("org.apache.spark.sql.delta.deletionvectors.DeletionVectorsSuite", 16.85),
      ("org.apache.spark.sql.delta.stats.DataSkippingDeltaV1WithCatalogOwnedBatch100Suite", 12.48),
      ("org.apache.spark.sql.delta.stats.DataSkippingDeltaV1WithCatalogOwnedBatch2Suite", 11.68),
      ("org.apache.spark.sql.delta.DeltaFastDropFeatureSuite", 11.04),
      ("org.apache.spark.sql.delta.stats.DataSkippingDeltaV1ParquetCheckpointV2Suite", 10.29),
      ("io.delta.sharing.spark.DeltaSharingDataSourceDeltaSuite", 10.28),
      ("org.apache.spark.sql.delta.DeltaSourceLargeLogWithCoordinatedCommitsBatch1Suite", 9.89),
      ("org.apache.spark.sql.delta.DeltaSourceWithCoordinatedCommitsBatch100Suite", 9.2),
      ("org.apache.spark.sql.delta.DeltaSourceLargeLogSuite", 8.86),
      ("org.apache.spark.sql.delta.DeltaInsertIntoSchemaEvolutionSuite", 8.7),
      ("org.apache.spark.sql.delta.stats.DataSkippingDeltaV1WithCatalogOwnedBatch1Suite", 8.68),
      ("org.apache.spark.sql.delta.DeltaSourceWithCoordinatedCommitsBatch1Suite", 8.64),
      ("org.apache.spark.sql.delta.DeltaSourceWithCoordinatedCommitsBatch10Suite", 8.61),
      ("org.apache.spark.sql.delta.CheckpointsWithCatalogOwnedBatch1Suite", 8.55),
      ("org.apache.spark.sql.delta.DeltaSourceLargeLogWithCoordinatedCommitsBatch100Suite", 8.39),
      ("org.apache.spark.sql.delta.DeltaVacuumSuite", 8.16),
      ("org.apache.spark.sql.delta.ImplicitMergeCastingSuite", 8.12),
      ("org.apache.spark.sql.delta.stats.DataSkippingDeltaV1JsonCheckpointV2Suite", 7.87),
      ("org.apache.spark.sql.delta.DescribeDeltaHistoryWithCatalogOwnedBatch100Suite", 7.73),
      ("org.apache.spark.sql.delta.stats.DataSkippingDeltaV1NameColumnMappingSuite", 7.58),
      ("org.apache.spark.sql.delta.typewidening.TypeWideningInsertSchemaEvolutionExtendedSuite", 7.3),
      ("org.apache.spark.sql.delta.DeltaCDCScalaWithCatalogOwnedBatch2Suite", 7.24),
      ("org.apache.spark.sql.delta.DeltaInsertIntoMissingColumnSuite", 7.08),
      ("org.apache.spark.sql.delta.DescribeDeltaHistorySuite", 6.76),
      ("org.apache.spark.sql.delta.generatedsuites.DeltaInsertIntoImplicitCastSuite", 6.63),
      ("io.delta.sharing.spark.DeltaSharingDataSourceCMSuite", 6.43),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoNestedMapStructEvolutionNullnessSQLNameBasedPreserveNullSourceOffPreserveNullSourceUpd7CQVRRQSuite", 6.38),
      ("io.delta.sharing.spark.DeltaFormatSharingSourceSuite", 6.26),
      ("org.apache.spark.sql.delta.DeltaCDCScalaWithCatalogOwnedBatch1Suite", 6.14),
      ("org.apache.spark.sql.delta.commands.backfill.RowTrackingBackfillConflictsDVSuite", 6.04),
      ("org.apache.spark.sql.delta.ImplicitStreamingMergeCastingSuite", 5.99),
      ("io.delta.tables.DeltaTableSuite", 5.93),
      ("org.apache.spark.sql.delta.DeltaWithCatalogOwnedBatch2Suite", 5.89),
      ("org.apache.spark.sql.delta.stats.DataSkippingDeltaV1Suite", 5.88),
      ("org.apache.spark.sql.delta.DeltaWithCatalogOwnedBatch1Suite", 5.8),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoNestedStructEvolutionInsertSQLPathBasedCDCOnDVsPredPushOffSuite", 5.77),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoNestedArrayStructEvolutionNullnessSQLNameBasedPreserveNullSourceOnPreserveNullSourceUpR36OX5ISuite", 5.7),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoSuiteBaseMiscSQLPathBasedCDCOnDVsPredPushOnSuite", 5.69),
      ("org.apache.spark.sql.delta.GenerateIdentityValuesSuite", 5.59),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoSchemaEvolutionBaseExistingColumnSQLPathBasedCDCOnDVsPredPushOffSuite", 5.51),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoNestedArrayStructEvolutionNullnessSQLNameBasedPreserveNullSourceOffPreserveNullSourceU6MQ3SIISuite", 5.51),
      ("org.apache.spark.sql.delta.stats.StatsCollectionSuite", 5.42),
      ("org.apache.spark.sql.delta.DeltaInsertIntoColumnOrderSuite", 5.38),
      ("org.apache.spark.sql.delta.columnmapping.RemoveColumnMappingCDCSuite", 5.36),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoSchemaEvolutionBaseNewColumnSQLPathBasedCDCOnDVsPredPushOffSuite", 5.07),
      ("org.apache.spark.sql.delta.DeltaLiteVacuumSuite", 5.05),
      ("org.apache.spark.sql.delta.DeltaProtocolVersionSuite", 5.04),
      ("org.apache.spark.sql.delta.CheckpointsWithCatalogOwnedBatch2Suite", 4.97),
      ("org.apache.spark.sql.delta.GeneratedColumnSuite", 4.95),
      ("org.apache.spark.sql.delta.DeltaSourceSuite", 4.87),
      ("org.apache.spark.sql.connect.delta.DeltaConnectPlannerSuite", 4.86),
      ("org.apache.spark.sql.delta.DeltaSuite", 4.83),
      ("org.apache.spark.sql.delta.deletionvectors.DeletionVectorsWithPredicatePushdownSuite", 4.83),
      ("org.apache.spark.sql.delta.DeltaWithCatalogOwnedBatch100Suite", 4.8),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoSchemaEvolutionBaseNewColumnSQLPathBasedDVsPredPushOffSuite", 4.68),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoNestedMapStructEvolutionNullnessSQLNameBasedPreserveNullSourceOnPreserveNullSourceUpdaH76RPFYSuite", 4.66),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoSuiteBaseMiscSQLPathBasedDVsPredPushOffSuite", 4.62),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoSuiteBaseMiscSQLPathBasedCDCOnDVsPredPushOffSuite", 4.59),
      ("org.apache.spark.sql.delta.CheckpointsSuite", 4.55),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoNestedArrayStructEvolutionNullnessSQLNameBasedPreserveNullSourceOnPreserveNullSourceUpFQ7PINASuite", 4.46),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoNestedMapStructEvolutionNullnessSQLNameBasedPreserveNullSourceOnPreserveNullSourceUpda5FZ34QYSuite", 4.46),
      ("org.apache.spark.sql.delta.test.DeltaV2SourceSuite", 4.42),
      ("org.apache.spark.sql.delta.IdentityColumnSyncScalaSuite", 4.39),
      ("org.apache.spark.sql.delta.commands.backfill.RowTrackingBackfillConflictsSuite", 4.33),
      ("org.apache.spark.sql.delta.typewidening.TypeWideningTableFeatureDropSuite", 4.32),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoSchemaEvolutionBaseNewColumnSQLPathBasedCDCOnSuite", 4.32),
      ("org.apache.spark.sql.delta.DeltaSinkImplicitCastWithCoordinatedCommitsBatch100Suite", 4.29),
      ("org.apache.spark.sql.delta.DeltaCDCScalaWithDeletionVectorsSuite", 4.27),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoNestedStructEvolutionNullnessSQLNameBasedPreserveNullSourceOnPreserveNullSourceUpdateStarOnSuite", 4.25),
      ("org.apache.spark.sql.delta.CheckpointsWithCatalogOwnedBatch100Suite", 4.24),
      ("org.apache.spark.sql.delta.generatedsuites.UpdateBaseMiscSQLPathBasedCDCOnSuite", 4.23),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoNestedStructEvolutionInsertSQLPathBasedCDCOnSuite", 4.21),
      ("org.apache.spark.sql.delta.ChecksumDVMetricsSuite", 4.21),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoNestedStructEvolutionNullnessSQLNameBasedPreserveNullSourceOffPreserveNullSourceUpdateStarOffSuite", 4.15),
      ("org.apache.spark.sql.delta.DeltaSourceNameColumnMappingSuite", 4.14),
      ("org.apache.spark.sql.delta.DeltaInsertIntoSQLByPathSuite", 4.12),
      ("org.apache.spark.sql.delta.hudi.ConvertToHudiSuite", 4.07),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoExtendedSyntaxSQLPathBasedDVsPredPushOnSuite", 4.03),
      ("org.apache.spark.sql.delta.schema.InvariantEnforcementSuite", 3.97),
      ("org.apache.spark.sql.delta.DeltaLogSuite", 3.96),
      ("org.apache.spark.sql.delta.IdentityColumnIngestionScalaSuite", 3.96),
      ("org.apache.spark.sql.delta.typewidening.TypeWideningAlterTableSuite", 3.92),
      ("org.apache.spark.sql.delta.generatedsuites.UpdateBaseMiscSQLPathBasedCDCOnDVSuite", 3.92),
      ("org.apache.spark.sql.delta.generatedsuites.RowTrackingMergeCommonNameBasedRowTrackingMergeDVSuite", 3.91),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoNestedStructEvolutionNullnessSQLNameBasedPreserveNullSourceOffPreserveNullSourceUpdateStarOnSuite", 3.88),
      ("org.apache.spark.sql.delta.DeltaProtocolTransitionsSuite", 3.85),
      ("org.apache.spark.sql.delta.stats.PartitionLikeDataSkippingSuite", 3.8),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoSchemaEvolutionBaseNewColumnScalaSuite", 3.8),
      ("org.apache.spark.sql.delta.DeltaSourceIdColumnMappingSuite", 3.78),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoBasicSQLPathBasedCDCOnDVsPredPushOnSuite", 3.71),
      ("org.apache.spark.sql.delta.DeltaTimeTravelWithCatalogOwnedBatch1Suite", 3.64),
      ("org.apache.spark.sql.delta.ConvertToDeltaScalaSuite", 3.63),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoNestedStructEvolutionUpdateOnlySQLPathBasedSuite", 3.63),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoNestedStructEvolutionUpdateOnlyScalaSuite", 3.59),
      ("org.apache.spark.sql.delta.InCommitTimestampWithCatalogOwnedBatch2Suite", 3.58),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoTopLevelStructEvolutionNullnessSQLNameBasedPreserveNullSourceOnPreserveNullSourceUpdateStarOnSuite", 3.57),
      ("org.apache.spark.sql.delta.generatedsuites.MergeIntoNotMatchedBySourceCDCPart1SQLPathBasedCDCOnDVsPredPushOnSuite", 3.55),
      ("org.apache.spark.sql.delta.DeltaLogWithCatalogOwnedBatch1Suite", 3.54)
    )

    /**
     * Generates the optimal test assignment across shards and groups for high duration test suites.
     *
     * Will assign the high duration test suites in descending order, always assigning to the
     * group with the smallest total duration. In case of ties (e.g. early on when some group
     * durations are still 0, will assign to the shard with the smallest total duration).
     *
     * Here's a simple example using 3 shards and 2 groups per shard:
     *
     * Test 1: DeltaRetentionWithCatalogOwnedBatch1Suite (22.66 mins) --> Shard 0, Group 0
     * - Shard 0: Group 0 = 22.66 mins, Group 1 = 0.0 mins
     * - Shard 1: Group 0 = 0.0 mins, Group 1 = 0.0 mins
     * - Shard 2: Group 0 = 0.0 mins, Group 1 = 0.0 mins
     *
     * Test 2: DeltaRetentionSuite (21.46 mins) --> Shard 1, Group 0
     * - Shard 0: Group 0 = 22.66 mins, Group 1 = 0.0 mins
     * - Shard 1: Group 0 = 21.46 mins, Group 1 = 0.0 mins
     * - Shard 2: Group 0 = 0.0 mins, Group 1 = 0.0 mins
     *
     * Test 3: DeletionVectorsSuite (16.85 mins) --> Shard 2, Group 0
     * - Shard 0: Group 0 = 22.66 mins, Group 1 = 0.0 mins
     * - Shard 1: Group 0 = 21.46 mins, Group 1 = 0.0 mins
     * - Shard 2: Group 0 = 16.85 mins, Group 1 = 0.0 mins
     *
     * Test 4: DataSkippingDeltaV1WithCatalogOwnedBatch100Suite (12.48 mins) --> Shard 2, Group 1
     * - Shard 0: Group 0 = 22.66 mins, Group 1 = 0.0 mins
     * - Shard 1: Group 0 = 21.46 mins, Group 1 = 0.0 mins
     * - Shard 2: Group 0 = 16.85 mins, Group 1 = 12.48 mins
     *
     * Test 5: DataSkippingDeltaV1WithCatalogOwnedBatch2Suite (11.68 mins) --> Shard 1, Group 1
     * - Shard 0: Group 0 = 22.66 mins, Group 1 = 0.0 mins
     * - Shard 1: Group 0 = 21.46 mins, Group 1 = 11.68 mins
     * - Shard 2: Group 0 = 16.85 mins, Group 1 = 12.48 mins
     *
     * Test 6: DeltaFastDropFeatureSuite (11.04 mins) --> Shard 0, Group 1
     * - Shard 0: Group 0 = 22.66 mins, Group 1 = 11.04 mins
     * - Shard 1: Group 0 = 21.46 mins, Group 1 = 11.68 mins
     * - Shard 2: Group 0 = 16.85 mins, Group 1 = 12.48 mins
     */
    def highDurationOptimalAssignment(numGroups: Int):
        (Array[Array[Set[String]]], Array[Array[Double]]) = {
      val assignment = Array.fill(NUM_SHARDS)(Array.fill(numGroups)(List.empty[String]))
      val groupDurations = Array.fill(NUM_SHARDS)(Array.fill(numGroups)(0.0))
      val shardDurations = Array.fill(NUM_SHARDS)(0.0)
      val sortedTestSuites = TOP_N_HIGH_DURATION_TEST_SUITES.sortBy(-_._2)

      sortedTestSuites.foreach { case (testSuiteName, duration) =>
        val (shardIdx, groupIdx) =
          findShardAndGroupWithLowestDuration(numGroups, shardDurations, groupDurations)

        assignment(shardIdx)(groupIdx) = assignment(shardIdx)(groupIdx) :+ testSuiteName
        groupDurations(shardIdx)(groupIdx) += duration
        shardDurations(shardIdx) += duration
      }

      (assignment.map(_.map(_.toSet)), groupDurations)
    }

    /**
     * Finds the best shard and group to assign the next test suite.
     *
     * Selects the group with the smallest total duration, and in case of ties, selects the shard
     * with the smallest total duration.
     *
     * @param numShards      Number of shards
     * @param numGroups      Number of groups per shard
     * @param shardDurations Total duration per shard
     * @param groupDurations Total duration per group in each shard
     * @return Tuple of (shard index, group index) for the optimal assignment
     */
    private def findShardAndGroupWithLowestDuration(
        numGroups: Int,
        shardDurations: Array[Double],
        groupDurations: Array[Array[Double]]): (Int, Int) = {
      var bestShardIdx = -1
      var bestGroupIdx = -1
      var minGroupDuration = Double.MaxValue
      var minShardDuration = Double.MaxValue

      for (shardIdx <- 0 until NUM_SHARDS) {
        for (groupIdx <- 0 until numGroups) {
          val currentGroupDuration = groupDurations(shardIdx)(groupIdx)
          val currentShardDuration = shardDurations(shardIdx)

          if (currentGroupDuration < minGroupDuration ||
              (currentGroupDuration == minGroupDuration &&
                currentShardDuration < minShardDuration)) {
            minGroupDuration = currentGroupDuration
            minShardDuration = currentShardDuration
            bestShardIdx = shardIdx
            bestGroupIdx = groupIdx
          }
        }
      }

      (bestShardIdx, bestGroupIdx)
    }

    def apply(
        groupCount: Int,
        baseDir: File,
        shardId: Int,
        forkOptionsTemplate: ForkOptions): GroupingStrategy = {
      val testGroups = scala.collection.mutable.Map((0 until groupCount).map {
        groupIdx =>
          val tmpDir = s"$baseDir/target/tmp/$groupIdx"
          java.nio.file.Files.createDirectories(java.nio.file.Paths.get(tmpDir))

          val forkOptions = forkOptionsTemplate.withRunJVMOptions(
            runJVMOptions = forkOptionsTemplate.runJVMOptions ++
                Seq(s"-Djava.io.tmpdir=$tmpDir")
          )
          val group = Tests.Group(
            name = s"Test group $groupIdx",
            tests = Nil,
            runPolicy = Tests.SubProcess(forkOptions)
          )
          groupIdx -> group
      }: _*)

      val (allShardsTestAssignments, allShardsGroupDurations) =
        highDurationOptimalAssignment(groupCount)

      new MinShardGroupDurationStrategy(
        testGroups,
        shardId,
        allShardsTestAssignments(shardId),
        allShardsGroupDurations(shardId)
      )
    }
  }
}
