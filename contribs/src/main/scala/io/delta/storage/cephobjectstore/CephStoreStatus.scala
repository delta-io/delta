/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.storage.cephobjectstore

import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileStatus, Path}

/**
 * This class is used by listStatus for Ceph RGW files.
 */
class CephStoreStatus(length: Long, isdir: Boolean, block_replication: Int,
                      blocksize: Long, modification_time: Long, access_time: Long,
                      permission: FsPermission, owner: String, group: String, symlink: Path,
                      path: Path)
  extends FileStatus(length: Long, isdir: Boolean, block_replication: Int,
    blocksize: Long, modification_time: Long, access_time: Long,
    permission: FsPermission, owner: String, group: String, symlink: Path,
    path: Path) {


  def this(modification_time: Long, path: Path, owner: String) {
    this(0, true, 1, 0, modification_time,
      0, null, owner, null, null, path)
    setOwner(owner)
    setGroup(owner)
  }

  def this(length: Long, modification_time: Long, path: Path,
           blockSize: Long, owner: String) {
    this(length, false, 1, blockSize, modification_time,
      0, null, owner, null, null, path)
    setOwner(owner)
    setGroup(owner)
  }
}
