import sbt.Keys._
import sbt._

// scalastyle:off println

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

  /** Sets the Test / testGroupingStrategy Task to an instance of the SimpleHashStrategy */
  lazy val simpleGroupingStrategySettings = Seq(
    Test / forkTestJVMCount := {
      testParallelismOpt.getOrElse(java.lang.Runtime.getRuntime.availableProcessors)
    },
    Test / shardId := { shardIdOpt.get },
    Test / testGroupingStrategy := {
      val groupsCount = (Test / forkTestJVMCount).value
      val shard = (Test / shardId).value
      val baseJvmDir = baseDirectory.value
      GreedyHashStrategy(groupsCount, baseJvmDir, shard, defaultForkOptions.value)
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
   * @param groups The current mapping of group indices to their respective [[sbt.Tests.Group]]
   *               objects, which hold test definitions.
   * @param shardId The shard ID that this instance is responsible for.
   * @param highDurationTestAssignment Precomputed assignments of high-duration test suites to
   *                                   specific groups within the shard.
   * @param groupRuntimes Array holding the current total runtime for each group within the shard.
   * @param groupCounts Array holding the number of test suites assigned to each group within the
   *                    shard.
   */
  class GreedyHashStrategy private(
      groups: scala.collection.mutable.Map[Int, Tests.Group],
      shardId: Int,
      highDurationTestAssignment: Array[List[String]],
      var groupRuntimes: Array[Double]
  ) extends GroupingStrategy {
    import TestParallelization.GreedyHashStrategy._

    if (shardId < 0 || shardId >= NUM_SHARDS) {
      throw new IllegalArgumentException(
        s"Assigned shard ID $shardId is not between 0 and ${NUM_SHARDS - 1} inclusive")
    }

    lazy val testGroups = groups.values.toList

    override def add(testDefinition: TestDefinition): GroupingStrategy = {
      val testSuiteName = testDefinition.name
      val isHighDurationTest = HIGH_DURATION_TEST_SUITES.exists(_._1 == testSuiteName)
      val highDurationTestGroupIndex =
        highDurationTestAssignment.indexWhere(_.contains(testSuiteName))

      // println(s"Trying to assign test suite: $testSuiteName. This is shardId: $shardId.")
      // println(s"isHighDurationTest: $isHighDurationTest")
      // println(s"highDurationTestGroupIndex: $highDurationTestGroupIndex")

      if (isHighDurationTest) {
        if (highDurationTestGroupIndex >= 0) {
          // Case 1: this is a high duration test that belongs to this shard. Assign it.
          val duration = HIGH_DURATION_TEST_SUITES.find(_._1 == testSuiteName).get._2
          // println(s"[High] Assigning suite $testSuiteName ($duration mins) to shard $shardId")

          val currentGroup = groups(highDurationTestGroupIndex)
          val updatedGroup = currentGroup.withTests(currentGroup.tests :+ testDefinition)
          groups(highDurationTestGroupIndex) = updatedGroup

          // Do NOT update groupRuntimes -- this was already included in the initial value of
          // groupRuntimes

          this
        } else {
          // Case 2: this is a high duration test that does NOT belong to this shard. Skip it.
          // println(s"[High] NOT assigning suite $testSuiteName to shard $shardId")
          this
        }
      } else if (math.abs(testDefinition.name.hashCode % NUM_SHARDS) == shardId) {
        // Case 3: this is a normal test that belongs to this shard. Assign it.
        // println(s"[Low] Assigning suite $testSuiteName to shard $shardId")

        val minDurationGroupIndex = groupRuntimes.zipWithIndex.minBy(_._1)._2

        // println(s"groupRuntimes: ${groupRuntimes.mkString("Array(", ", ", ")")}")
        // println(s"minDurationGroupIndex: $minDurationGroupIndex")

        val currentGroup = groups(minDurationGroupIndex)
        val updatedGroup = currentGroup.withTests(currentGroup.tests :+ testDefinition)
        groups(minDurationGroupIndex) = updatedGroup

        groupRuntimes(minDurationGroupIndex) += 1.0 // assume test duration of 1 minute

        this
      } else {
        // Case 4: this is a normal test that does NOT belong to this shard. Skip it.
        // println(s"[Low] NOT assigning suite $testSuiteName to shard $shardId")
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

  object GreedyHashStrategy {

    val NUM_SHARDS = numShardsOpt.get

    val HIGH_DURATION_TEST_SUITES: List[(String, Double)] = List(
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
     * Generate the optimal test assignment across shards and groups.
     *
     * @param numShards Number of shards
     * @param numGroups Number of groups per shard
     */
    def generateOptimalAssignment(numShards: Int, numGroups: Int):
        (Array[Array[List[String]]], Array[Array[Double]]) = {
      val assignment = Array.fill(numShards)(Array.fill(numGroups)(List.empty[String]))
      val groupDurations = Array.fill(numShards)(Array.fill(numGroups)(0.0))
      val shardDurations = Array.fill(numShards)(0.0)
      val sortedTestSuites = HIGH_DURATION_TEST_SUITES.sortBy(-_._2)

      sortedTestSuites.foreach { case (testSuiteName, duration) =>
        val (shardIdx, groupIdx) =
          findBestShardAndGroup(numShards, numGroups, shardDurations, groupDurations)

        assignment(shardIdx)(groupIdx) = assignment(shardIdx)(groupIdx) :+ testSuiteName
        groupDurations(shardIdx)(groupIdx) += duration
        shardDurations(shardIdx) += duration
      }

      (assignment, groupDurations)
    }

    /**
     * Finds the best shard and group to assign the next test suite.
     * Selects the group with the smallest total duration, and in case of ties, selects the shard
     * with the smallest total duration.
     *
     * @param numShards      Number of shards
     * @param numGroups      Number of groups per shard
     * @param shardDurations Total duration per shard
     * @param groupDurations Total duration per group in each shard
     * @return Tuple of (shard index, group index) for the optimal assignment
     */
    def findBestShardAndGroup(
        numShards: Int,
        numGroups: Int,
        shardDurations: Array[Double],
        groupDurations: Array[Array[Double]]
    ): (Int, Int) = {
      var bestShardIdx = -1
      var bestGroupIdx = -1
      var minGroupDuration = Double.MaxValue
      var minShardDuration = Double.MaxValue

      for (shardIdx <- 0 until numShards) {
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

      val (allTestAssignments, allGroupDurations) =
        generateOptimalAssignment(NUM_SHARDS, groupCount)

      new GreedyHashStrategy(
        testGroups,
        shardId,
        allTestAssignments(shardId),
        allGroupDurations(shardId)
      )
    }
  }
}
