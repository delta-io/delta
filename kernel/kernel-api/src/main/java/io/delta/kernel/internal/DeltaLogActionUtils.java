/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal;

import static io.delta.kernel.internal.DeltaErrors.*;
import static io.delta.kernel.internal.fs.Path.getName;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.InvalidTableException;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.ListUtils;
import io.delta.kernel.internal.replay.ActionsIterator;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.FileNames.DeltaLogFileType;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes APIs to read the raw actions within the *commit files* of the _delta_log. This is used
 * for CDF, streaming, and more.
 */
public class DeltaLogActionUtils {

  private DeltaLogActionUtils() {}

  private static final Logger logger = LoggerFactory.getLogger(DeltaLogActionUtils.class);

  /////////////////
  // Public APIs //
  /////////////////

  /**
   * Represents a Delta action. This is used to request which actions to read from the commit files
   * in {@link TableImpl#getChanges(Engine, long, long, Set)}.
   *
   * <p>See the Delta protocol for more details
   * https://github.com/delta-io/delta/blob/master/PROTOCOL.md#actions
   */
  public enum DeltaAction {
    REMOVE("remove", RemoveFile.FULL_SCHEMA),
    ADD("add", AddFile.FULL_SCHEMA),
    METADATA("metaData", Metadata.FULL_SCHEMA),
    PROTOCOL("protocol", Protocol.FULL_SCHEMA),
    COMMITINFO("commitInfo", CommitInfo.FULL_SCHEMA),
    CDC("cdc", AddCDCFile.FULL_SCHEMA);

    public final String colName;
    public final StructType schema;

    DeltaAction(String colName, StructType schema) {
      this.colName = colName;
      this.schema = schema;
    }
  }

  /**
   * For a table get the list of commit log files for the provided version range.
   *
   * @param tablePath path for the given table
   * @param startVersion start version of the range (inclusive)
   * @param endVersion end version of the range (inclusive)
   * @return the list of commit files in increasing order between startVersion and endVersion
   * @throws TableNotFoundException if the table does not exist or if it is not a delta table
   * @throws KernelException if a commit file does not exist for any of the versions in the provided
   *     range
   * @throws KernelException if provided an invalid version range
   */
  public static List<FileStatus> getCommitFilesForVersionRange(
      Engine engine, Path tablePath, long startVersion, long endVersion) {
    // Validate arguments
    if (startVersion < 0 || endVersion < startVersion) {
      throw invalidVersionRange(startVersion, endVersion);
    }

    // Get any available commit files within the version range
    final List<FileStatus> commitFiles =
        listDeltaLogFiles(
            engine,
            Collections.singleton(DeltaLogFileType.COMMIT),
            tablePath,
            startVersion,
            Optional.of(endVersion),
            false /* mustBeRecreatable */);

    // There are no available commit files within the version range.
    // This can be due to (1) an empty directory, (2) no valid delta files in the directory,
    // (3) only delta files less than startVersion prefix (4) only delta files after endVersion
    if (commitFiles.isEmpty()) {
      throw noCommitFilesFoundForVersionRange(tablePath.toString(), startVersion, endVersion);
    }

    // Verify commit files found
    // (check that they are continuous and start with startVersion and end with endVersion)
    verifyDeltaVersions(commitFiles, startVersion, endVersion, tablePath);

    return commitFiles;
  }

  /**
   * Read the given commitFiles and return the contents as an iterator of batches. Also adds two
   * columns "version" and "timestamp" that store the commit version and timestamp for the commit
   * file that the batch was read from. The "version" and "timestamp" columns are the first and
   * second columns in the returned schema respectively and both of {@link LongType}
   *
   * @param commitFiles list of delta commit files to read
   * @param readSchema JSON schema to read
   * @return an iterator over the contents of the files in the same order as the provided files
   */
  public static CloseableIterator<ColumnarBatch> readCommitFiles(
      Engine engine, List<FileStatus> commitFiles, StructType readSchema) {

    return new ActionsIterator(engine, commitFiles, readSchema, Optional.empty())
        .map(
            actionWrapper -> {
              long timestamp =
                  actionWrapper
                      .getTimestamp()
                      .orElseThrow(
                          () ->
                              new RuntimeException("Commit files should always have a timestamp"));
              ExpressionEvaluator commitVersionGenerator =
                  wrapEngineException(
                      () ->
                          engine
                              .getExpressionHandler()
                              .getEvaluator(
                                  readSchema,
                                  Literal.ofLong(actionWrapper.getVersion()),
                                  LongType.LONG),
                      "Get the expression evaluator for the commit version");
              ExpressionEvaluator commitTimestampGenerator =
                  wrapEngineException(
                      () ->
                          engine
                              .getExpressionHandler()
                              .getEvaluator(readSchema, Literal.ofLong(timestamp), LongType.LONG),
                      "Get the expression evaluator for the commit timestamp");
              ColumnVector commitVersionVector =
                  wrapEngineException(
                      () -> commitVersionGenerator.eval(actionWrapper.getColumnarBatch()),
                      "Evaluating the commit version expression");
              ColumnVector commitTimestampVector =
                  wrapEngineException(
                      () -> commitTimestampGenerator.eval(actionWrapper.getColumnarBatch()),
                      "Evaluating the commit timestamp expression");

              return actionWrapper
                  .getColumnarBatch()
                  .withNewColumn(0, COMMIT_VERSION_STRUCT_FIELD, commitVersionVector)
                  .withNewColumn(1, COMMIT_TIMESTAMP_STRUCT_FIELD, commitTimestampVector);
            });
  }

  /**
   * Returns the list of files of type $fileTypes in the _delta_log directory of the given
   * $tablePath, in increasing order from $startVersion to the optional $endVersion.
   *
   * @throws TableNotFoundException if the table or its _delta_log does not exist
   * @throws KernelException if mustBeRecreatable is true, endVersionOpt is present, and the
   *     _delta_log history has been truncated so that we cannot load the desired end version
   */
  public static List<FileStatus> listDeltaLogFiles(
      Engine engine,
      Set<DeltaLogFileType> fileTypes,
      Path tablePath,
      long startVersion,
      Optional<Long> endVersionOpt,
      boolean mustBeRecreatable) {
    checkArgument(!fileTypes.isEmpty(), "At least one file type must be provided");

    endVersionOpt.ifPresent(
        endVersion -> {
          checkArgument(
              endVersion >= startVersion,
              "endVersion=%s provided is less than startVersion=%s",
              endVersion,
              startVersion);
        });

    final Path logPath = new Path(tablePath, "_delta_log");

    logger.info(
        "Listing log files types={} in path={} starting from {} and ending with {}",
        fileTypes,
        logPath,
        startVersion,
        endVersionOpt);

    final List<FileStatus> output = new ArrayList<>();
    final long startTimeMillis = System.currentTimeMillis();

    try (CloseableIterator<FileStatus> fsIter = listLogDir(engine, tablePath, startVersion)) {
      while (fsIter.hasNext()) {
        final FileStatus fs = fsIter.next();

        if (fileTypes.contains(DeltaLogFileType.COMMIT)
            && FileNames.isCommitFile(getName(fs.getPath()))) {
          // Here, we do nothing (we will consume this file).
        } else if (fileTypes.contains(DeltaLogFileType.CHECKPOINT)
            && FileNames.isCheckpointFile(getName(fs.getPath()))
            && fs.getSize() > 0) {
          // Checkpoint files of 0 size are invalid but may be ignored silently when read, hence we
          // ignore them so that we never pick up such checkpoints.
          // Here, we do nothing (we will consume this file).
        } else {
          logger.debug("Ignoring file {} as it is not of the desired type", fs.getPath());
          continue; // Here, we continue and skip this file.
        }

        final long fileVersion = FileNames.getFileVersion(new Path(fs.getPath()));

        if (fileVersion < startVersion) {
          throw new RuntimeException(
              String.format(
                  "Listing files in %s with startVersion %s yet found file %s with version %s",
                  logPath, startVersion, fs.getPath(), fileVersion));
        }

        if (endVersionOpt.isPresent()) {
          final long endVersion = endVersionOpt.get();

          if (fileVersion > endVersion) {
            if (mustBeRecreatable && output.isEmpty()) {
              final long earliestVersion =
                  DeltaHistoryManager.getEarliestRecreatableCommit(engine, logPath);
              throw DeltaErrors.versionBeforeFirstAvailableCommit(
                  tablePath.toString(), endVersion, earliestVersion);
            } else {
              logger.debug(
                  "Stopping listing; found file {} with version > {}=endVersion",
                  fs.getPath(),
                  endVersion);
              break;
            }
          }
        }

        output.add(fs);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to close resource", e);
    }

    logger.info(
        "{}: Took {} ms to list the commit files for versions [{}, {}]",
        tablePath,
        System.currentTimeMillis() - startTimeMillis,
        startVersion,
        endVersionOpt);

    return output;
  }

  //////////////////////
  // Private helpers //
  /////////////////////

  /** Column name storing the commit version for a given file action */
  private static final String COMMIT_VERSION_COL_NAME = "version";

  private static final DataType COMMIT_VERSION_DATA_TYPE = LongType.LONG;
  private static final StructField COMMIT_VERSION_STRUCT_FIELD =
      new StructField(COMMIT_VERSION_COL_NAME, COMMIT_VERSION_DATA_TYPE, false /* nullable */);

  /** Column name storing the commit timestamp for a given file action */
  private static final String COMMIT_TIMESTAMP_COL_NAME = "timestamp";

  private static final DataType COMMIT_TIMESTAMP_DATA_TYPE = LongType.LONG;
  private static final StructField COMMIT_TIMESTAMP_STRUCT_FIELD =
      new StructField(COMMIT_TIMESTAMP_COL_NAME, COMMIT_TIMESTAMP_DATA_TYPE, false /* nullable */);

  /**
   * Given a list of delta versions, verifies that they are (1) contiguous (2) versions starts with
   * expectedStartVersion and (3) end with expectedEndVersion. Throws an exception if any of these
   * are not true.
   *
   * <p>Public to expose for testing only.
   *
   * @param commitFiles in sorted increasing order according to the commit version
   */
  static void verifyDeltaVersions(
      List<FileStatus> commitFiles,
      long expectedStartVersion,
      long expectedEndVersion,
      Path tablePath) {

    List<Long> commitVersions =
        commitFiles.stream()
            .map(fs -> FileNames.deltaVersion(new Path(fs.getPath())))
            .collect(Collectors.toList());

    for (int i = 1; i < commitVersions.size(); i++) {
      if (commitVersions.get(i) != commitVersions.get(i - 1) + 1) {
        throw new InvalidTableException(
            tablePath.toString(),
            String.format(
                "Missing delta files: versions are not contiguous: (%s)", commitVersions));
      }
    }

    if (commitVersions.isEmpty() || !Objects.equals(commitVersions.get(0), expectedStartVersion)) {
      throw startVersionNotFound(
          tablePath.toString(),
          expectedStartVersion,
          commitVersions.isEmpty() ? Optional.empty() : Optional.of(commitVersions.get(0)));
    }

    if (!Objects.equals(ListUtils.getLast(commitVersions), expectedEndVersion)) {
      throw endVersionNotFound(
          tablePath.toString(), expectedEndVersion, ListUtils.getLast(commitVersions));
    }
  }

  /**
   * Gets an iterator of files in the _delta_log directory starting with the startVersion.
   *
   * @throws TableNotFoundException if the directory does not exist
   */
  private static CloseableIterator<FileStatus> listLogDir(
      Engine engine, Path tablePath, long startVersion) {
    final Path logPath = new Path(tablePath, "_delta_log");
    try {
      return wrapEngineExceptionThrowsIO(
          () ->
              engine.getFileSystemClient().listFrom(FileNames.listingPrefix(logPath, startVersion)),
          "Listing from %s",
          FileNames.listingPrefix(logPath, startVersion));
    } catch (FileNotFoundException e) {
      // Did not find the _delta_log directory.
      throw new TableNotFoundException(tablePath.toString());
    } catch (IOException io) {
      throw new UncheckedIOException("Failed to list the files in delta log", io);
    }
  }
}
