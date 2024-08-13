import sbt.Keys._
import sbt._

object TestParallelization {

  lazy val numShards = sys.env.get("NUM_SHARDS").map(_.toInt)

  lazy val settings = {
    val parallelismCount = sys.env.get("TEST_PARALLELISM_COUNT")
    if (parallelismCount.exists( _.toInt > 1)) {
      customTestGroupingSettings ++ simpleGroupingStrategySettings
    }
    else {
      Seq.empty[Setting[_]]
    }
  }

  /**
    Replace the default value for Test / testGrouping settingKey
    and set it to a new value calculated by using the custom Task
   [[testGroupingStrategy]]. Adding these settings to the build
    will require to separately provide a value for the TaskKey
    [[testGroupingStrategy]]
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
        groups.foreach{
          group =>
            logger.info(s"${group.name} contains ${group.tests.size} tests")
        }
        groups
      }
    )
  }



  /**
      Sets the Test / testGroupingStrategy Task to an instance of the
      SimpleHashStrategy
   */
  lazy val simpleGroupingStrategySettings = Seq(
    Test / forkTestJVMCount := {
      sys.env.get("TEST_PARALLELISM_COUNT").map(_.toInt).getOrElse(4)
    },
    Test / shardId := {
      sys.env.get("SHARD_ID").map(_.toInt)
    },
    Test / testGroupingStrategy := {
      val groupsCount = (Test / forkTestJVMCount).value
      val shard = (Test / shardId).value
      val baseJvmDir = baseDirectory.value
      SimpleHashStrategy(groupsCount, baseJvmDir, shard, defaultForkOptions.value)
    },
    Test / parallelExecution := true,
    Global / concurrentRestrictions := {
      Seq(Tags.limit(Tags.ForkedTestGroup, (Test / forkTestJVMCount).value))
    }
  )

  val shardId = SettingKey[Option[Int]]("shard id",
    "The shard id assigned"
  )

  val forkTestJVMCount = SettingKey[Int]("fork test jvm count",
    "The number of separate JVM to use for tests"
  )

  val testGroupingStrategy = TaskKey[GroupingStrategy]("test grouping strategy",
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
   * By default SBT will run all tests as if they belong to a single group,
   * but allows tests to be grouped. Setting [[sbt.Keys.testGrouping]] to
   * a list of groups replace the default single-group definition.
   *
   * When creating an instance of [[sbt.Tests.Group]] it is possible to specify
   * an [[sbt.Tests.TestRunPolicy]]: this parameter can be used to use multiple
   * subprocesses for test execution
   *
   */
  sealed trait GroupingStrategy {

    /**
     * Adds an [[sbt.TestDefinition]] to this GroupingStrategy and
     * returns an updated Grouping Strategy
     */
    def add(testDefinition: TestDefinition): GroupingStrategy

    /**
     * Returns the test groups built from this GroupingStrategy
     */
    def testGroups: List[Tests.Group]
  }

  class SimpleHashStrategy private(
      groups: Map[Int, Tests.Group],
      shardId: Option[Int]) extends GroupingStrategy {

    lazy val testGroups = groups.values.toList
    val groupCount = groups.size

    override def add(testDefinition: TestDefinition): GroupingStrategy = {

      /**
       * The way test sharding works is that every action task is assigned a shard ID
       * in the range [0, numShards - 1].
       * Tests where the (test name hash % number of shards) is equal to the
       * shard ID for this action are added to the set of tests to run.
       * All other tests will be assigned to other shards.
       * We are guaranteed coverage since the result of the modulo math is guaranteed
       * be within [0, numShards - 1] and there will be numShards actions
       * that are triggered.
       *
       * Note: This is a simple grouping strategy which doesn't consider test
       * complexity, duration or other factors.
       */
      if (shardId.isDefined && numShards.isDefined) {
        if (shardId.get < 0 || shardId.get >= numShards.get) {
          throw new IllegalArgumentException(
            s"Assigned shard ID $shardId is not between 0 and ${numShards.get - 1} inclusive")
        }

        val testIsAssignedToShard =
          math.abs(testDefinition.name.hashCode % numShards.get) == shardId.get
        if(!testIsAssignedToShard) {
          return new SimpleHashStrategy(groups, shardId)
        }
      }

      val groupIdx = math.abs(testDefinition.name.hashCode % groupCount)
      val currentGroup = groups(groupIdx)
      val updatedGroup = currentGroup.withTests(
        currentGroup.tests :+ testDefinition
      )
      new SimpleHashStrategy(groups + (groupIdx -> updatedGroup), shardId)
    }
  }

  object SimpleHashStrategy {

    def apply(
        groupCount: Int,
        baseDir: File,
        shard: Option[Int],
        forkOptionsTemplate: ForkOptions): GroupingStrategy = {
      val testGroups = (0 until groupCount).map {
        groupIdx =>
          val forkOptions = forkOptionsTemplate.withRunJVMOptions(
            runJVMOptions = forkOptionsTemplate.runJVMOptions ++
              Seq(s"-Djava.io.tmpdir=${baseDir}/target/tmp/$groupIdx")
          )
          val group = Tests.Group(
            name = s"Test group ${groupIdx}",
            tests = Nil,
            runPolicy = Tests.SubProcess(forkOptions)
          )
          groupIdx -> group
      }
      new SimpleHashStrategy(testGroups.toMap, shard)
    }
  }

}
