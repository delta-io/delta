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
     * High-duration test suites loaded from project/test-durations.csv.
     *
     * To update, run: python3 project/scripts/collect_test_durations.py
     */
    val TOP_N_HIGH_DURATION_TEST_SUITES: List[(String, Double)] = {
      val csvFile = new java.io.File("project/test-durations.csv")
      if (!csvFile.exists()) {
        println(s"Warning: ${csvFile.getPath} not found, using empty test durations")
        List.empty
      } else {
        val source = scala.io.Source.fromFile(csvFile)
        try {
          source.getLines().drop(1).filter(_.trim.nonEmpty).map { line =>
            val idx = line.lastIndexOf(',')
            (line.substring(0, idx), line.substring(idx + 1).toDouble)
          }.toList
        } finally {
          source.close()
        }
      }
    }

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
