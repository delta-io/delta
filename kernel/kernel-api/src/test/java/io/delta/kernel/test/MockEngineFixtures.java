/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.kernel.test;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.engine.FileReadRequest;
import io.delta.kernel.engine.FileSystemClient;
import io.delta.kernel.engine.JsonHandler;
import io.delta.kernel.engine.ParquetHandler;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

/**
 * Java counterpart of the {@code MockEngineUtils} Scala trait, for use by JUnit 5 tests.
 *
 * <p>Each handler base implements every method as unsupported; a test overrides only the calls it
 * exercises, and any unexpected call fails loudly rather than returning a fake value.
 */
public final class MockEngineFixtures {

  private MockEngineFixtures() {}

  private static final String UNSUPPORTED = "not supported in this test suite";

  /**
   * Creates a mock {@link Engine} from the given components. Accessing a component that was not
   * provided throws {@link UnsupportedOperationException}.
   */
  public static Engine mockEngine(
      FileSystemClient fileSystemClient,
      JsonHandler jsonHandler,
      ParquetHandler parquetHandler,
      ExpressionHandler expressionHandler) {
    return new Engine() {
      @Override
      public ExpressionHandler getExpressionHandler() {
        return require(expressionHandler);
      }

      @Override
      public JsonHandler getJsonHandler() {
        return require(jsonHandler);
      }

      @Override
      public FileSystemClient getFileSystemClient() {
        return require(fileSystemClient);
      }

      @Override
      public ParquetHandler getParquetHandler() {
        return require(parquetHandler);
      }

      private <T> T require(T component) {
        if (component == null) {
          throw new UnsupportedOperationException(UNSUPPORTED);
        }
        return component;
      }
    };
  }

  /** A mock {@link Engine} whose only capability is listing an empty log directory. */
  public static Engine emptyListFromEngine() {
    return mockEngine(
        new BaseMockFileSystemClient() {
          @Override
          public CloseableIterator<FileStatus> listFrom(String filePath) {
            return toCloseableIterator(Collections.<FileStatus>emptyList().iterator());
          }

          @Override
          public String resolvePath(String path) {
            return path;
          }
        },
        null,
        null,
        null);
  }

  private static <T> CloseableIterator<T> toCloseableIterator(java.util.Iterator<T> iterator) {
    return new CloseableIterator<T>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public T next() {
        return iterator.next();
      }

      @Override
      public void close() {}
    };
  }

  /** Java counterpart of the {@code BaseMockJsonHandler} Scala trait. */
  public abstract static class BaseMockJsonHandler implements JsonHandler {
    @Override
    public ColumnarBatch parseJson(
        ColumnVector jsonStringVector,
        StructType outputSchema,
        Optional<ColumnVector> selectionVector) {
      throw new UnsupportedOperationException(UNSUPPORTED);
    }

    @Override
    public CloseableIterator<ColumnarBatch> readJsonFiles(
        CloseableIterator<FileStatus> fileIter,
        StructType physicalSchema,
        Optional<Predicate> predicate)
        throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED);
    }

    @Override
    public void writeJsonFileAtomically(
        String filePath, CloseableIterator<Row> data, boolean overwrite) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED);
    }
  }

  /** Java counterpart of the {@code BaseMockFileSystemClient} Scala trait. */
  public abstract static class BaseMockFileSystemClient implements FileSystemClient {
    @Override
    public CloseableIterator<FileStatus> listFrom(String filePath) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED);
    }

    @Override
    public String resolvePath(String path) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED);
    }

    @Override
    public CloseableIterator<ByteArrayInputStream> readFiles(
        CloseableIterator<FileReadRequest> readRequests) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED);
    }

    @Override
    public boolean mkdirs(String path) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED);
    }

    @Override
    public boolean delete(String path) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED);
    }

    @Override
    public FileStatus getFileStatus(String path) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED);
    }

    @Override
    public void copyFileAtomically(String srcPath, String destPath, boolean overwrite)
        throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED);
    }
  }
}
