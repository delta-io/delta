package io.delta.sources

import java.io.File
import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.DeltaSourceSuiteBase
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

class DeltaSourcesSuite extends DeltaSourceSuiteBase with DeltaSQLCommandTest {

  import testImplicits._

  private def withTempDirs(f: (File, File, File) => Unit): Unit = {
    withTempDir { file1 =>
      withTempDir { file2 =>
        withTempDir { file3 =>
          f(file1, file2, file3)
        }
      }
    }
  }

  test("make sure that the delta sources works fine") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      Seq(1, 2, 3).toDF().write.delta(inputDir.toString)

      val df = spark.readStream.delta(inputDir.toString)

      val stream = df.writeStream
        .option("checkpointLocation", checkpointDir.toString)
        .delta(outputDir.toString)

      stream.processAllAvailable()
      stream.stop()

      val writtenStreamDf = spark.read.delta(outputDir.toString)
      val expectedRows = Seq(Row(1), Row(2), Row(3))

      checkAnswer(writtenStreamDf, expectedRows)
    }
  }
}
