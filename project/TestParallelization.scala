import scala.util.hashing.MurmurHash3

import sbt.Keys._
import sbt._

object TestParallelization {

  lazy val numShardsOpt = sys.env.get("NUM_SHARDS").map(_.toInt)
  lazy val shardIdOpt = sys.env.get("SHARD_ID").map(_.toInt)
  lazy val testParallelismOpt = sys.env.get("TEST_PARALLELISM_COUNT").map(_.toInt)

  lazy val settings = {
    if (numShardsOpt.exists(_ > 1) && testParallelismOpt.exists(_ > 1) &&
        shardIdOpt.exists(_ >= 0)) {
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
    Test / shardId := { shardIdOpt.get },
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
      workingDirectory = Some(baseDirectory.value),
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
      val isHighDurationTest = TOP_50_HIGH_DURATION_TEST_SUITES.exists(_._1 == testSuiteName)

      if (isHighDurationTest) {
        val highDurationTestGroupIndex =
          highDurationTestAssignment.indexWhere(_.contains(testSuiteName))

        if (highDurationTestGroupIndex >= 0) {
          // Case 1: this is a high duration test that was pre-computed in the optimal assignment to
          // belong to this shard. Assign it.
          val duration = TOP_50_HIGH_DURATION_TEST_SUITES.find(_._1 == testSuiteName).get._2

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

        groupRuntimes(minDurationGroupIndex) += AVG_TEST_SUITE_DURATION_EXCLUDING_SLOWEST_50

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

    val NUM_SHARDS = numShardsOpt.get

    val AVG_TEST_SUITE_DURATION_EXCLUDING_SLOWEST_50 = 0.71

    /** 50 slowest test suites and their durations. */
    val TOP_50_HIGH_DURATION_TEST_SUITES: List[(String, Double)] = List(
      ("org.apache.spark.sql.delta.MergeIntoDVsWithPredicatePushdownCDCSuite", 36.09),
      ("org.apache.spark.sql.delta.MergeIntoDVsSuite", 33.26),
      ("org.apache.spark.sql.delta.MergeIntoDVsCDCSuite", 27.39),
      ("org.apache.spark.sql.delta.cdc.MergeCDCSuite", 26.24),
      ("org.apache.spark.sql.delta.MergeIntoDVsWithPredicatePushdownSuite", 23.58),
      ("org.apache.spark.sql.delta.MergeIntoSQLSuite", 23.01),
      ("org.apache.spark.sql.delta.MergeIntoScalaSuite", 16.67),
      ("org.apache.spark.sql.delta.deletionvectors.DeletionVectorsSuite", 11.55),
      ("org.apache.spark.sql.delta.stats.DataSkippingDeltaV1ParquetCheckpointV2Suite", 8.26),
      ("org.apache.spark.sql.delta.DescribeDeltaHistorySuite", 7.16),
      ("org.apache.spark.sql.delta.ImplicitMergeCastingSuite", 7.14),
      ("org.apache.spark.sql.delta.stats.DataSkippingDeltaV1JsonCheckpointV2Suite", 7.0),
      ("org.apache.spark.sql.delta.UpdateSQLWithDeletionVectorsSuite", 6.03),
      ("org.apache.spark.sql.delta.commands.backfill.RowTrackingBackfillConflictsDVSuite", 5.97),
      ("org.apache.spark.sql.delta.DeltaSourceSuite", 5.86),
      ("org.apache.spark.sql.delta.cdc.UpdateCDCWithDeletionVectorsSuite", 5.67),
      ("org.apache.spark.sql.delta.stats.DataSkippingDeltaV1Suite", 5.63),
      ("org.apache.spark.sql.delta.DeltaSourceLargeLogSuite", 5.61),
      ("org.apache.spark.sql.delta.stats.DataSkippingDeltaV1NameColumnMappingSuite", 5.43),
      ("org.apache.spark.sql.delta.GenerateIdentityValuesSuite", 5.4),
      ("org.apache.spark.sql.delta.commands.backfill.RowTrackingBackfillConflictsSuite", 5.02),
      ("org.apache.spark.sql.delta.ImplicitStreamingMergeCastingSuite", 4.77),
      ("org.apache.spark.sql.delta.DeltaVacuumWithCoordinatedCommitsBatch100Suite", 4.73),
      ("org.apache.spark.sql.delta.CoordinatedCommitsBatchBackfill1DeltaLogSuite", 4.64),
      ("org.apache.spark.sql.delta.DeltaLogSuite", 4.6),
      ("org.apache.spark.sql.delta.IdentityColumnIngestionScalaSuite", 4.36),
      ("org.apache.spark.sql.delta.DeltaVacuumSuite", 4.22),
      ("org.apache.spark.sql.delta.columnmapping.RemoveColumnMappingCDCSuite", 4.12),
      ("org.apache.spark.sql.delta.DeltaSuite", 4.05),
      ("org.apache.spark.sql.delta.UpdateSQLSuite", 3.99),
      ("org.apache.spark.sql.delta.typewidening.TypeWideningInsertSchemaEvolutionSuite", 3.92),
      ("org.apache.spark.sql.delta.cdc.DeleteCDCSuite", 3.9),
      ("org.apache.spark.sql.delta.CoordinatedCommitsBatchBackfill100DeltaLogSuite", 3.86),
      ("org.apache.spark.sql.delta.rowid.UpdateWithRowTrackingCDCSuite", 3.83),
      ("org.apache.spark.sql.delta.expressions.HilbertIndexSuite", 3.75),
      ("org.apache.spark.sql.delta.DeltaProtocolVersionSuite", 3.71),
      ("org.apache.spark.sql.delta.CoordinatedCommitsBatchBackfill2DeltaLogSuite", 3.68),
      ("org.apache.spark.sql.delta.CheckpointsWithCoordinatedCommitsBatch100Suite", 3.59),
      ("org.apache.spark.sql.delta.ConvertToDeltaScalaSuite", 3.59),
      ("org.apache.spark.sql.delta.typewidening.TypeWideningTableFeatureSuite", 3.49),
      ("org.apache.spark.sql.delta.cdc.UpdateCDCSuite", 3.42),
      ("org.apache.spark.sql.delta.CloneTableScalaDeletionVectorSuite", 3.41),
      ("org.apache.spark.sql.delta.IdentityColumnSyncScalaSuite", 3.33),
      ("org.apache.spark.sql.delta.DeleteSQLSuite", 3.31),
      ("org.apache.spark.sql.delta.CheckpointsWithCoordinatedCommitsBatch2Suite", 3.19),
      ("org.apache.spark.sql.delta.DeltaSourceIdColumnMappingSuite", 3.18),
      ("org.apache.spark.sql.delta.rowid.RowTrackingMergeCDFDVSuite", 3.18),
      ("org.apache.spark.sql.delta.rowid.UpdateWithRowTrackingTableFeatureCDCSuite", 3.12),
      ("org.apache.spark.sql.delta.UpdateSQLWithDeletionVectorsAndPredicatePushdownSuite", 3.01),
      ("org.apache.spark.sql.delta.rowid.RowTrackingMergeDVSuite", 2.97)
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
     * Test 1: MergeIntoDVsWithPredicatePushdownCDCSuite (36.09 mins) --> Shard 0, Group 0
     * - Shard 0: Group 0 = 36.09 mins, Group 1 = 0.0 mins
     * - Shard 1: Group 0 = 0.0 mins, Group 1 = 0.0 mins
     * - Shard 2: Group 0 = 0.0 mins, Group 1 = 0.0 mins
     *
     * Test 2: MergeIntoDVsSuite (33.26 mins) --> Shard 1, Group 0
     * - Shard 0: Group 0 = 36.09 mins, Group 1 = 0.0 mins
     * - Shard 1: Group 0 = 33.26 mins, Group 1 = 0.0 mins
     * - Shard 2: Group 0 = 0.0 mins, Group 1 = 0.0 mins
     *
     * Test 3: MergeIntoDVsCDCSuite (27.39 mins) --> Shard 2, Group 0
     * - Shard 0: Group 0 = 36.09 mins, Group 1 = 0.0 mins
     * - Shard 1: Group 0 = 33.26 mins, Group 1 = 0.0 mins
     * - Shard 2: Group 0 = 27.39 mins, Group 1 = 0.0 mins
     *
     * Test 4: MergeCDCSuite (26.24 mins) --> Shard 2, Group 1
     * - Shard 0: Group 0 = 36.09 mins, Group 1 = 0.0 mins
     * - Shard 1: Group 0 = 33.26 mins, Group 1 = 0.0 mins
     * - Shard 2: Group 0 = 27.39 mins, Group 1 = 26.24 mins
     *
     * Test 5: MergeIntoDVsWithPredicatePushdownSuite (23.58 mins) -> Shard 1, Group 1
     * - Shard 0: Group 0 = 36.09 mins, Group 1 = 0.0 mins
     * - Shard 1: Group 0 = 33.26 mins, Group 1 = 23.58 mins
     * - Shard 2: Group 0 = 27.39 mins, Group 1 = 26.24 mins
     *
     * Test 6: MergeIntoSQLSuite (23.01 mins) --> Shard 0, Group 1
     * - Shard 0: Group 0 = 36.09 mins, Group 1 = 23.01 mins
     * - Shard 1: Group 0 = 33.26 mins, Group 1 = 23.58 mins
     * - Shard 2: Group 0 = 27.39 mins, Group 1 = 26.24 mins
     *
     * Test 7: MergeIntoScalaSuite (16.67 mins) --> Shard 0, Group 1
     * - Shard 0: Group 0 = 36.09 mins, Group 1 = 39.68 mins
     * - Shard 1: Group 0 = 33.26 mins, Group 1 = 23.58 mins
     * - Shard 2: Group 0 = 27.39 mins, Group 1 = 26.24 mins
     *
     * Test 8: DeletionVectorsSuite (11.55 mins) --> Shard 1, Group 1
     * - Shard 0: Group 0 = 36.09 mins, Group 1 = 39.68 mins
     * - Shard 1: Group 0 = 33.26 mins, Group 1 = 35.13 mins
     * - Shard 2: Group 0 = 27.39 mins, Group 1 = 26.24 mins
     */
    def highDurationOptimalAssignment(numGroups: Int):
        (Array[Array[Set[String]]], Array[Array[Double]]) = {
      val assignment = Array.fill(NUM_SHARDS)(Array.fill(numGroups)(List.empty[String]))
      val groupDurations = Array.fill(NUM_SHARDS)(Array.fill(numGroups)(0.0))
      val shardDurations = Array.fill(NUM_SHARDS)(0.0)
      val sortedTestSuites = TOP_50_HIGH_DURATION_TEST_SUITES.sortBy(-_._2)

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
          val forkOptions = forkOptionsTemplate.withRunJVMOptions(
            runJVMOptions = forkOptionsTemplate.runJVMOptions ++
                Seq(s"-Djava.io.tmpdir=$baseDir/target/tmp/$groupIdx")
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
