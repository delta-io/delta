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
import java.util.stream.Collectors;

/** Serializable wrapper for Kernel's {@link FileStatus}. */
public class SerializableFileStatus implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String path;
  private final long size;
  private final long modificationTime;

  private SerializableFileStatus(String path, long size, long modificationTime) {
    this.path = path;
    this.size = size;
    this.modificationTime = modificationTime;
  }

  public static SerializableFileStatus from(FileStatus fs) {
    return new SerializableFileStatus(fs.getPath(), fs.getSize(), fs.getModificationTime());
  }

  public FileStatus toFileStatus() {
    return FileStatus.of(path, size, modificationTime);
  }

  public static List<SerializableFileStatus> fromList(List<FileStatus> list) {
    return list.stream().map(SerializableFileStatus::from).collect(Collectors.toList());
  }

  public static List<FileStatus> toFileStatusList(List<SerializableFileStatus> list) {
    return list.stream().map(SerializableFileStatus::toFileStatus).collect(Collectors.toList());
  }
}
