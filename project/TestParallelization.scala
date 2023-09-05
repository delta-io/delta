import sbt.Keys._
import sbt._

object TestParallelization {

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
    Test / testGroupingStrategy := {
      val groupsCount = (Test / forkTestJVMCount).value
      val baseJvmDir = baseDirectory.value
      SimpleHashStrategy(groupsCount, baseJvmDir, defaultForkOptions.value)
    },
    Test / parallelExecution := true,
    Global / concurrentRestrictions := {
      Seq(Tags.limit(Tags.ForkedTestGroup, (Test / forkTestJVMCount).value))
    }
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

  class SimpleHashStrategy private(groups: Map[Int, Tests.Group]) extends GroupingStrategy {

    lazy val testGroups = groups.values.toList
    val groupCount = groups.size

    override def add(testDefinition: TestDefinition): GroupingStrategy = {
      val groupIdx = math.abs(testDefinition.name.hashCode % groupCount)
      val currentGroup = groups(groupIdx)
      val updatedGroup = currentGroup.withTests(
        currentGroup.tests :+ testDefinition
      )
      new SimpleHashStrategy(groups + (groupIdx -> updatedGroup))
    }
  }

  object SimpleHashStrategy {

    def apply(groupCount: Int,
              baseDir: File, forkOptionsTemplate: ForkOptions): GroupingStrategy = {
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
      new SimpleHashStrategy(testGroups.toMap)
    }
  }

}
