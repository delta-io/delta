import sbt.Keys._
import sbt._

object TestParallelization {

  lazy val testGroupingInDifferentJVMSettings = Seq(
    Test / testGrouping := {
      val tests = (Test / definedTests).value
      val groupingStrategy = (Test / testGroupingStrategy).value
      val grouping = tests.foldLeft(groupingStrategy) {
        case (strategy, testDefinition) => strategy.add(testDefinition)
      }
      grouping.testGroups
    }
  )

  lazy val simpleGroupingStrategySettings = Seq(
    Test / forkTestJVMCount := {
      sys.env.get("DELTA_TEST_JVM_COUNT").map(_.toInt).getOrElse(4)
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
    "The strategy to allocate different groups to different jvms"
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

  sealed trait GroupingStrategy {

    def add(testDefinition: TestDefinition): GroupingStrategy

    def testGroups: List[Tests.Group]

  }

  class SimpleHashStrategy(groups: Map[Int, Tests.Group]) extends GroupingStrategy {

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
      val testGroups = (0 to groupCount).map {
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
