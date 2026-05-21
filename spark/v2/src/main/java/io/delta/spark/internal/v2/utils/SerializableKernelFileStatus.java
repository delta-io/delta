/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.utils;

import io.delta.kernel.utils.FileStatus;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Serializable wrapper for Kernel's {@link FileStatus}, which is a public API class that we avoid
 * modifying. This wrapper is used to serialize snapshot state (LogSegment contents) from the driver
 * to executors.
 */
public final class SerializableKernelFileStatus implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String path;
  private final long size;
  private final long modificationTime;

  public SerializableKernelFileStatus(String path, long size, long modificationTime) {
    this.path = Objects.requireNonNull(path, "path is null");
    this.size = size;
    this.modificationTime = modificationTime;
  }

  public static SerializableKernelFileStatus from(FileStatus fs) {
    return new SerializableKernelFileStatus(fs.getPath(), fs.getSize(), fs.getModificationTime());
  }

  public FileStatus toFileStatus() {
    return FileStatus.of(path, size, modificationTime);
  }

  public static List<SerializableKernelFileStatus> fromList(List<FileStatus> list) {
    return list.stream().map(SerializableKernelFileStatus::from).collect(Collectors.toList());
  }

  public static List<FileStatus> toFileStatusList(List<SerializableKernelFileStatus> list) {
    return list.stream()
        .map(SerializableKernelFileStatus::toFileStatus)
        .collect(Collectors.toList());
  }
}
