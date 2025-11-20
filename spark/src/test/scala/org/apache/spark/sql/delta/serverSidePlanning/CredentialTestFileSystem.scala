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

package org.apache.spark.sql.delta.serverSidePlanning

import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, LocalFileSystem, Path}

/**
 * Base class for credential testing filesystems.
 * Wraps local filesystem to simulate cloud storage with credential validation.
 *
 * Each cloud-specific implementation validates that credentials from server-side
 * planning were correctly injected into Hadoop configuration.
 *
 * TODO: In production, cloud-specific filesystem implementations (S3CredentialFileSystem,
 * AzureCredentialFileSystem, GCSCredentialFileSystem) should implement credential refresh:
 * 1. Monitor credential expiration times
 * 2. Call table service endpoint (using catalogName/ucUri/ucToken from Hadoop config)
 * 3. Obtain refreshed credentials before expiration
 * 4. Update Hadoop configuration with new credentials
 * This test infrastructure validates the initial credential injection; production filesystems
 * will extend this to handle refresh for long-running queries.
 */
abstract class CredentialTestFileSystem extends LocalFileSystem {

  /**
   * Returns the scheme this filesystem handles (e.g., "s3a", "abfs", "gs").
   */
  def scheme(): String

  /**
   * Check that required credentials are present in configuration.
   * Throws assertion error if credentials are missing or incorrect.
   *
   * @param path The path being accessed (credentials may be path/bucket-specific)
   */
  def checkCredentials(path: Path): Unit

  /**
   * Convert remote cloud path to local path for actual file operations.
   * Format: scheme://bucket/key -> file:///bucket/key
   */
  protected def toLocalPath(remotePath: Path): Path = {
    // Validate credentials before allowing access
    checkCredentials(remotePath)

    // Convert remote path to local path
    val uri = remotePath.toUri
    val localPath = if (uri.getHost != null) {
      s"/${uri.getHost}${uri.getPath}"
    } else {
      uri.getPath
    }
    new Path(s"file://$localPath")
  }

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    super.open(toLocalPath(f), bufferSize)
  }

  override def getFileStatus(f: Path): FileStatus = {
    val localStatus = super.getFileStatus(toLocalPath(f))
    // Restore original remote path in the status
    new FileStatus(
      localStatus.getLen,
      localStatus.isDirectory,
      localStatus.getReplication,
      localStatus.getBlockSize,
      localStatus.getModificationTime,
      localStatus.getAccessTime,
      localStatus.getPermission,
      localStatus.getOwner,
      localStatus.getGroup,
      f  // Use original remote path
    )
  }

  override def getScheme(): String = scheme()
}
