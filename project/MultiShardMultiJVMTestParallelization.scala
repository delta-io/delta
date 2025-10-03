import scala.util.hashing.MurmurHash3
import sbt.Keys._
import sbt._

// scalastyle:off println
/** Provides SBT test settings for sharding and parallelizing multi-JVM tests. */
object MultiShardMultiJVMTestParallelization {

  /**
   * Total number of shards (machines) to split tests across.
   * E.g., NUM_SHARDS=4 means tests will be split across 4 machines.
   * Each test is assigned to exactly one shard based on hash(testName) % NUM_SHARDS.
   */
  lazy val numShardsOpt = sys.env.get("NUM_SHARDS").map(_.toInt)

  /**
   * The ID of the current shard (0-indexed).
   * E.g., SHARD_ID=0 means this is shard 0 out of NUM_SHARDS total shards.
   * This shard will only run tests where hash(testName) % NUM_SHARDS == SHARD_ID.
   */
  lazy val shardIdOpt = sys.env.get("SHARD_ID").map(_.toInt)

  /**
   * Number of parallel JVMs to use within this shard.
   * E.g., TEST_PARALLELISM_COUNT=2 means tests in this shard will run across 2 JVMs in parallel.
   * Tests are distributed across JVMs using hash(testName + "group") % TEST_PARALLELISM_COUNT.
   */
  lazy val testParallelismOpt = sys.env.get("TEST_PARALLELISM_COUNT").map(_.toInt)

  lazy val settings = {
    println(s"numShardsOpt: $numShardsOpt")
    println(s"shardIdOpt: $shardIdOpt")
    println(s"testParallelismOpt: $testParallelismOpt")

    (numShardsOpt, shardIdOpt, testParallelismOpt) match {
      case (Some(numShards), Some(shardId), Some(testParallelism))
        if numShards >= 1 && shardId >= 0 && testParallelism >= 1 =>
        println("Test parallelization enabled.")

        Seq(
          Test / testGrouping := {
            val tests = (Test / definedTests).value

            // Create default fork options that inherit all the project's settings
            val defaultForkOptions = ForkOptions(
              javaHome = javaHome.value,
              outputStrategy = outputStrategy.value,
              bootJars = Vector.empty,
              workingDirectory = Some(baseDirectory.value),
              runJVMOptions = (Test / javaOptions).value.toVector,
              connectInput = connectInput.value,
              envVars = (Test / envVars).value
            )

            // Filter tests for this shard
            val testsForThisShard = tests.filter { testDef =>
              math.abs(MurmurHash3.stringHash(testDef.name) % numShards) == shardId
            }

            println(s"[Shard $shardId] # tests: ${testsForThisShard.size}")

            // Distribute tests across groups (JVMs) within this shard
            (0 until testParallelism).map { groupId =>
              val testsForThisGroup = testsForThisShard.filter { testDef =>
                // Add "group" suffix to create a different hash than shard assignment,
                // ensuring even distribution across groups independent of shard assignment
                val groupHash = MurmurHash3.stringHash(testDef.name + "group")
                math.abs(groupHash % testParallelism) == groupId
              }

              println(s"[Group $groupId] # tests: ${testsForThisGroup.size}")

              Tests.Group(
                name = s"Shard $shardId - Group $groupId",
                tests = testsForThisGroup,
                runPolicy = Tests.SubProcess(defaultForkOptions)
              )
            }
          },
          Test / parallelExecution := true,
          Global / concurrentRestrictions := Seq(
            Tags.limit(Tags.ForkedTestGroup, testParallelism)
          )
        )

      case _ =>
        println("Test parallelization disabled.")
        Seq.empty[Setting[_]] // Run tests normally
    }
  }
}
