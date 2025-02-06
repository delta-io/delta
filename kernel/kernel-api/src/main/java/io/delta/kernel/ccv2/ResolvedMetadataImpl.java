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

package io.delta.kernel.ccv2;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.snapshot.LogSegment;
import java.util.Optional;


public abstract class ResolvedMetadataImpl implements ResolvedMetadata {

  private final String path;
  private final long version;
  private Optional<LogSegment> logSegment;
  private final Optional<Protocol> protocol;
  private final Optional<Metadata> metadata;
  private final Optional<String> schemaString;

  public ResolvedMetadataImpl(
      String path,
      long version,
      Optional<LogSegment> logSegment,
      Optional<Protocol> protocol,
      Optional<Metadata> metadata,
      Optional<String> schemaString) {
    this.path = path;
    this.version = version;
    this.logSegment = logSegment;
    this.protocol = protocol;
    this.metadata = metadata;
    this.schemaString = schemaString;
  }

  /////////////
  // Getters //
  /////////////

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public Optional<LogSegment> getLogSegment() {
    return logSegment;
  }

  @Override
  public Optional<Protocol> getProtocol() {
    return protocol;
  }

  @Override
  public Optional<Metadata> getMetadata() {
    return metadata;
  }

  @Override
  public Optional<String> getSchemaString() {
    return schemaString;
  }

  /////////////
  // Setters //
  /////////////

  public void setLogSegment(Optional<LogSegment> logSegment) {
    this.logSegment = logSegment;
  }
}
