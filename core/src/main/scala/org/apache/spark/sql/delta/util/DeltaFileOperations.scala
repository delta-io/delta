/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.util

import java.io.{FileNotFoundException, IOException}
import java.net.URI
import java.util.Locale

import scala.util.Random
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.{Footer, ParquetFileReader}

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.{SerializableConfiguration, ThreadUtils}

/**
 * Some utility methods on files, directories, and paths.
 */
object DeltaFileOperations extends DeltaLogging {
  /**
   * Create an absolute path from `child` using the `basePath` if the child is a relative path.
   * Return `child` if it is an absolute path.
   *
   * @param basePath Base path to prepend to `child` if child is a relative path.
   *                 Note: It is assumed that the basePath do not have any escaped characters and
   *                 is directly readable by Hadoop APIs.
   * @param child    Child path to append to `basePath` if child is a relative path.
   *                 Note: t is assumed that the child is escaped, that is, all special chars that
   *                 need escaping by URI standards are already escaped.
   * @return Absolute path without escaped chars that is directly readable by Hadoop APIs.
   */
  def absolutePath(basePath: String, child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      p
    } else {
      val merged = new Path(basePath, p)
      // URI resolution strips the final `/` in `p` if it exists
      val mergedUri = merged.toUri.toString
      if (child.endsWith("/") && !mergedUri.endsWith("/")) {
        new Path(new URI(mergedUri + "/"))
      } else {
        merged
      }
    }
  }

  /**
   * Given a path `child`:
   *   1. Returns `child` if the path is already relative
   *   2. Tries relativizing `child` with respect to `basePath`
   *      a) If the `child` doesn't live within the same base path, returns `child` as is
   *      b) If `child` lives in a different FileSystem, throws an exception
   * Note that `child` may physically be pointing to a path within `basePath`, but may logically
   * belong to a different FileSystem, e.g. DBFS mount points and direct S3 paths.
   */
  def tryRelativizePath(fs: FileSystem, basePath: Path, child: Path): Path = {
    // Child Paths can be absolute and use a separate fs
    val childUri = child.toUri
    // We can map multiple schemes to the same `FileSystem` class, but `FileSystem.getScheme` is
    // usually just a hard-coded string. Hence, we need to use the scheme of the URI that we use to
    // create the FileSystem here.
    if (child.isAbsolute) {
      try {
        new Path(fs.makeQualified(basePath).toUri.relativize(fs.makeQualified(child).toUri))
      } catch {
        case e: IllegalArgumentException =>
          throw new IllegalStateException(
            s"""Failed to relativize the path ($child). This can happen when absolute paths make
               |it into the transaction log, which start with the scheme s3://, wasbs:// or adls://.
               |This is a bug that has existed before DBR 5.0. To fix this issue, please upgrade
               |your writer jobs to DBR 5.0 and please run:
               |%scala com.databricks.delta.Delta.fixAbsolutePathsInLog("$child")
             """.stripMargin)
      }
    } else {
      child
    }
  }

  /** Check if the thrown exception is a throttling error. */
  private def isThrottlingError(t: Throwable): Boolean = {
    Option(t.getMessage).exists(_.toLowerCase(Locale.ROOT).contains("slow down"))
  }

  private def randomBackoff(
      opName: String,
      t: Throwable,
      base: Int = 100,
      jitter: Int = 1000): Unit = {
    val sleepTime = Random.nextInt(jitter) + base
    logWarning(s"Sleeping for $sleepTime ms to rate limit $opName", t)
    Thread.sleep(sleepTime)
  }

  /** Iterate through the contents of directories.
   *
   * If `listAsDirectories` is enabled, then we consider each path in `subDirs` to be directories,
   * and we list files under that path. If, for example, "a/b" is provided, we would attempt to
   * list "a/b/1.txt", "a/b/c/2.txt", and so on. We would not list "a/c", since it's not the same
   * directory as "a/b".
   * If not, we consider that path to be a filename, and we list paths in the same directory with
   * names after that path. So, if "a/b" is provided, we would list "a/b/1.txt", "a/c", "a/d", and
   * so on. However a file like "a/a.txt" would not be listed, because lexically it appears before
   * "a/b".
   */
  private def listUsingLogStore(
      logStore: LogStore,
      subDirs: Iterator[String],
      recurse: Boolean,
      hiddenFileNameFilter: String => Boolean,
      listAsDirectories: Boolean = true): Iterator[SerializableFileStatus] = {

    def list(dir: String, tries: Int): Iterator[SerializableFileStatus] = {
      logInfo(s"Listing $dir")
      try {

        val path = if (listAsDirectories) new Path(dir, "\u0000") else new Path(dir + "\u0000")
        logStore.listFrom(path)
          .filterNot(f => hiddenFileNameFilter(f.getPath.getName))
          .map(SerializableFileStatus.fromStatus)
      } catch {
        case NonFatal(e) if isThrottlingError(e) && tries > 0 =>
          randomBackoff("listing", e)
          list(dir, tries - 1)
        case e: FileNotFoundException =>
          // Can happen when multiple GCs are running concurrently or due to eventual consistency
          Iterator.empty
      }
    }

    val filesAndDirs = subDirs.flatMap { dir =>
      list(dir, tries = 10)
    }

    if (recurse) {
      recurseDirectories(logStore, filesAndDirs, hiddenFileNameFilter)
    } else {
      filesAndDirs
    }
  }

  /** Given an iterator of files and directories, recurse directories with its contents. */
  private def recurseDirectories(
      logStore: LogStore,
      filesAndDirs: Iterator[SerializableFileStatus],
      hiddenFileNameFilter: String => Boolean): Iterator[SerializableFileStatus] = {
    filesAndDirs.flatMap {
      case dir: SerializableFileStatus if dir.isDir =>
        Iterator.single(dir) ++ listUsingLogStore(
          logStore, Iterator.single(dir.path), recurse = true, hiddenFileNameFilter)
      case file =>
        Iterator.single(file)
    }
  }

  /**
   * The default filter for hidden files. Files names beginning with _ or . are considered hidden.
   * @param fileName
   * @return true if the file is hidden
   */
  def defaultHiddenFileFilter(fileName: String): Boolean = {
    fileName.startsWith("_") || fileName.startsWith(".")
  }

  /**
   * Recursively lists all the files and directories for the given `subDirs` in a scalable manner.
   *
   * @param spark The SparkSession
   * @param subDirs Absolute path of the subdirectories to list
   * @param hadoopConf The Hadoop Configuration to get a FileSystem instance
   * @param hiddenFileNameFilter A function that returns true when the file should be considered
   *                             hidden and excluded from results. Defaults to checking for prefixes
   *                             of "." or "_".
   * @param listAsDirectories Whether to treat the paths in subDirs as directories, where all files
   *                          that are children to the path will be listed. If false, the paths are
   *                          treated as filenames, and files under the same folder with filenames
   *                          after the path will be listed instead.
   */
  def recursiveListDirs(
      spark: SparkSession,
      subDirs: Seq[String],
      hadoopConf: Broadcast[SerializableConfiguration],
      hiddenFileNameFilter: String => Boolean = defaultHiddenFileFilter,
      fileListingParallelism: Option[Int] = None,
      listAsDirectories: Boolean = true): Dataset[SerializableFileStatus] = {
    import spark.implicits._
    if (subDirs.isEmpty) return spark.emptyDataset[SerializableFileStatus]
    val listParallelism = fileListingParallelism.getOrElse(spark.sparkContext.defaultParallelism)
    val dirsAndFiles = spark.sparkContext.parallelize(subDirs).mapPartitions { dirs =>
      val logStore = LogStore(SparkEnv.get.conf, hadoopConf.value.value)
      listUsingLogStore(logStore, dirs, recurse = false, hiddenFileNameFilter, listAsDirectories)
    }.repartition(listParallelism) // Initial list of subDirs may be small

    val allDirsAndFiles = dirsAndFiles.mapPartitions { firstLevelDirsAndFiles =>
      val logStore = LogStore(SparkEnv.get.conf, hadoopConf.value.value)
      recurseDirectories(logStore, firstLevelDirsAndFiles, hiddenFileNameFilter)
    }
    spark.createDataset(allDirsAndFiles)
  }

  /**
   * Recursively and incrementally lists files with filenames after `listFilename` by alphabetical
   * order. Helpful if you only want to list new files instead of the entire directory.
   *
   * Files located within `topDir` with filenames lexically after `listFilename` will be included,
   * even if they may be located in parent/sibling folders of `listFilename`.
   *
   * @param spark The SparkSession
   * @param listFilename Absolute path to a filename from which new files are listed (exclusive)
   * @param topDir Absolute path to the original starting directory
   * @param hadoopConf The Hadoop Configuration to get a FileSystem instance
   * @param hiddenFileNameFilter A function that returns true when the file should be considered
   *                             hidden and excluded from results. Defaults to checking for prefixes
   *                             of "." or "_".
   */
  def recursiveListFrom(
    spark: SparkSession,
    listFilename: String,
    topDir: String,
    hadoopConf: Broadcast[SerializableConfiguration],
    hiddenFileNameFilter: String => Boolean = defaultHiddenFileFilter,
    fileListingParallelism: Option[Int] = None): Dataset[SerializableFileStatus] = {

    // Add folders from `listPath` to the depth before `topPath`, so as to ensure new folders/files
    // in the parent directories are also included in the listing.
    // If there are no new files, listing from parent directories are expected to be constant time.
    val subDirs = getAllTopComponents(new Path(listFilename), new Path(topDir))

    recursiveListDirs(spark, subDirs, hadoopConf, hiddenFileNameFilter,
      fileListingParallelism, listAsDirectories = false)
  }

  /**
   * Lists the directory locally using LogStore without launching a spark job. Returns an iterator
   * from LogStore.
   */
  def localListDirs(
      spark: SparkSession,
      dirs: Seq[String],
      recursive: Boolean = true,
      fileFilter: String => Boolean = defaultHiddenFileFilter): Iterator[SerializableFileStatus] = {
    val logStore = LogStore(SparkEnv.get.conf, spark.sessionState.newHadoopConf)
    listUsingLogStore(logStore, dirs.toIterator, recurse = recursive, fileFilter)
  }

  /**
   * Incrementally lists files with filenames after `listDir` by alphabetical order. Helpful if you
   * only want to list new files instead of the entire directory.
   * Listed locally using LogStore without launching a spark job. Returns an iterator from LogStore.
   */
  def localListFrom(
    spark: SparkSession,
    listFilename: String,
    topDir: String,
    recursive: Boolean = true,
    fileFilter: String => Boolean = defaultHiddenFileFilter): Iterator[SerializableFileStatus] = {
    val logStore = LogStore(SparkEnv.get.conf, spark.sessionState.newHadoopConf)
    val listDirs = getAllTopComponents(new Path(listFilename), new Path(topDir))
    listUsingLogStore(logStore, listDirs.toIterator, recurse = recursive, fileFilter,
      listAsDirectories = false)
  }

  /**
   * Tries deleting a file or directory non-recursively. If the file/folder doesn't exist,
   * that's fine, a separate operation may be deleting files/folders. If a directory is non-empty,
   * we shouldn't delete it. FileSystem implementations throw an `IOException` in those cases,
   * which we return as a "we failed to delete".
   *
   * Listing on S3 is not consistent after deletes, therefore in case the `delete` returns `false`,
   * because the file didn't exist, then we still return `true`. Retries on S3 rate limits up to 3
   * times.
   */
  def tryDeleteNonRecursive(fs: FileSystem, path: Path, tries: Int = 3): Boolean = {
    try fs.delete(path, false) catch {
      case _: FileNotFoundException => true
      case _: IOException => false
      case NonFatal(e) if isThrottlingError(e) && tries > 0 =>
        randomBackoff("deletes", e)
        tryDeleteNonRecursive(fs, path, tries - 1)
    }
  }

  /**
   * Returns all the levels of sub directories that `path` has with respect to `base`. For example:
   * getAllSubDirectories("/base", "/base/a/b/c") =>
   *   (Iterator("/base/a", "/base/a/b"), "/base/a/b/c")
   */
  def getAllSubDirectories(base: String, path: String): (Iterator[String], String) = {
    val baseSplits = base.split(Path.SEPARATOR)
    val pathSplits = path.split(Path.SEPARATOR).drop(baseSplits.length)
    val it = Iterator.tabulate(pathSplits.length - 1) { i =>
      (baseSplits ++ pathSplits.take(i + 1)).mkString(Path.SEPARATOR)
    }
    (it, path)
  }

  /** Register a task failure listener to delete a temp file in our best effort. */
  def registerTempFileDeletionTaskFailureListener(
      conf: Configuration,
      tempPath: Path): Unit = {
    val tc = TaskContext.get
    if (tc == null) {
      throw new IllegalStateException("Not running on a Spark task thread")
    }
    tc.addTaskFailureListener { (_, _) =>
      // Best effort to delete the temp file
      try {
        tempPath.getFileSystem(conf).delete(tempPath, false /* = recursive */)
      } catch {
        case NonFatal(e) =>
          logError(s"Failed to delete $tempPath", e)
      }
      () // Make the compiler happy
    }
  }

  /**
   * Reads Parquet footers in multi-threaded manner.
   * If the config "spark.sql.files.ignoreCorruptFiles" is set to true, we will ignore the corrupted
   * files when reading footers.
   */
  def readParquetFootersInParallel(
      conf: Configuration,
      partFiles: Seq[FileStatus],
      ignoreCorruptFiles: Boolean): Seq[Footer] = {
    ThreadUtils.parmap(partFiles, "readingParquetFooters", 8) { currentFile =>
      try {
        // Skips row group information since we only need the schema.
        // ParquetFileReader.readFooter throws RuntimeException, instead of IOException,
        // when it can't read the footer.
        Some(new Footer(currentFile.getPath(),
          ParquetFileReader.readFooter(
            conf, currentFile, SKIP_ROW_GROUPS)))
      } catch { case e: RuntimeException =>
        if (ignoreCorruptFiles) {
          logWarning(s"Skipped the footer in the corrupted file: $currentFile", e)
          None
        } else {
          throw new IOException(s"Could not read footer for file: $currentFile", e)
        }
      }
    }.flatten
  }

  /**
   * Get all parent directory paths from `listDir` until `topDir` (exclusive).
   * For example, if `topDir` is "/folder/" and `currDir` is "/folder/a/b/c", we would return
   * "/folder/a/b/c", "/folder/a/b" and "/folder/a".
   */
  def getAllTopComponents(listDir: Path, topDir: Path): List[String] = {
    var ret: List[String] = List()
    var currDir = listDir
    while (currDir.depth() > topDir.depth()) {
      ret = ret :+ currDir.toString
      val parent = currDir.getParent
      currDir = parent
    }
    ret
  }
}
