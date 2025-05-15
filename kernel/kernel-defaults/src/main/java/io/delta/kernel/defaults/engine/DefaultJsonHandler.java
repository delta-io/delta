/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.engine;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.lang.String.format;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.kernel.data.*;
import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;
import io.delta.kernel.engine.JsonHandler;
import io.delta.kernel.exceptions.KernelEngineException;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.LogStore;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Default implementation of {@link JsonHandler} based on Hadoop APIs. */
public class DefaultJsonHandler implements JsonHandler {
  private static final ObjectMapper mapper = new ObjectMapper();
  // by default BigDecimals are truncated and read as floats
  private static final ObjectReader objectReaderReadBigDecimals =
      new ObjectMapper().reader(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

  private final FileIO fileIO;
  private final int maxBatchSize;

  public DefaultJsonHandler(FileIO fileIO) {
    this.fileIO = fileIO;
    this.maxBatchSize =
        fileIO
            .getConf("delta.kernel.default.json.reader.batch-size")
            .map(Integer::valueOf)
            .orElse(1024);
    checkArgument(maxBatchSize > 0, "invalid JSON reader batch size: %d", maxBatchSize);
  }

  @Override
  public ColumnarBatch parseJson(
      ColumnVector jsonStringVector,
      StructType outputSchema,
      Optional<ColumnVector> selectionVector) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < jsonStringVector.getSize(); i++) {
      boolean isSelected =
          !selectionVector.isPresent()
              || (!selectionVector.get().isNullAt(i) && selectionVector.get().getBoolean(i));
      if (isSelected && !jsonStringVector.isNullAt(i)) {
        rows.add(parseJson(jsonStringVector.getString(i), outputSchema));
      } else {
        rows.add(null);
      }
    }
    return new io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch(
        outputSchema, rows);
  }

  @Override
  public CloseableIterator<ColumnarBatch> readJsonFiles(
      CloseableIterator<FileStatus> scanFileIter,
      StructType physicalSchema,
      Optional<Predicate> predicate)
      throws IOException {
    return new CloseableIterator<ColumnarBatch>() {
      private FileStatus currentFile;
      private BufferedReader currentFileReader;
      private String nextLine;

      @Override
      public void close() throws IOException {
        Utils.closeCloseables(currentFileReader, scanFileIter);
      }

      @Override
      public boolean hasNext() {
        if (nextLine != null) {
          return true; // we have un-consumed last read line
        }

        // There is no file in reading or the current file being read has no more data
        // initialize the next file reader or return false if there are no more files to
        // read.
        try {
          if (currentFileReader == null || (nextLine = currentFileReader.readLine()) == null) {
            // `nextLine` will initially be null because `currentFileReader` is guaranteed
            // to be null
            if (tryOpenNextFile()) {
              return hasNext();
            }
          }
          return nextLine != null;
        } catch (IOException ex) {
          throw new KernelEngineException(
              format("Error reading JSON file: %s", currentFile.getPath()), ex);
        }
      }

      @Override
      public ColumnarBatch next() {
        if (nextLine == null) {
          throw new NoSuchElementException();
        }

        List<Row> rows = new ArrayList<>();
        int currentBatchSize = 0;
        do {
          // hasNext already reads the next one and keeps it in member variable `nextLine`
          rows.add(parseJson(nextLine, physicalSchema));
          nextLine = null;
          currentBatchSize++;
        } while (currentBatchSize < maxBatchSize && hasNext());

        return new io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch(
            physicalSchema, rows);
      }

      private boolean tryOpenNextFile() throws IOException {
        Utils.closeCloseables(currentFileReader); // close the current opened file
        currentFileReader = null;

        if (scanFileIter.hasNext()) {
          currentFile = scanFileIter.next();
          SeekableInputStream stream = null;
          try {
            stream = fileIO.newInputFile(currentFile.getPath(), currentFile.getSize()).newStream();
            currentFileReader =
                new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
          } catch (Exception e) {
            Utils.closeCloseablesSilently(stream); // close it avoid leaking resources
            throw e;
          }
        }
        return currentFileReader != null;
      }
    };
  }

  /**
   * Makes use of {@link LogStore} implementations in `delta-storage` to atomically write the data
   * to a file depending upon the destination filesystem.
   *
   * @param filePath Destination file path
   * @param data Data to write as Json
   * @throws IOException
   */
  @Override
  public void writeJsonFileAtomically(
      String filePath, CloseableIterator<Row> data, boolean overwrite) throws IOException {
    fileIO
        .newOutputFile(filePath)
        .writeAtomically(
            data.map(io.delta.kernel.defaults.internal.json.JsonUtils::rowToJson), overwrite);
  }

  private Row parseJson(String json, StructType readSchema) {
    try {
      final JsonNode jsonNode = objectReaderReadBigDecimals.readTree(json);
      return new io.delta.kernel.defaults.internal.data.DefaultJsonRow(
          (ObjectNode) jsonNode, readSchema);
    } catch (JsonProcessingException ex) {
      throw new KernelEngineException(format("Could not parse JSON: %s", json), ex);
    }
  }
}
