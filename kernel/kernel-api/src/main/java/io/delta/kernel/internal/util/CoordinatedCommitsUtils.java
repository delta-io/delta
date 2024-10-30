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
package io.delta.kernel.internal.util;

import io.delta.kernel.engine.coordinatedcommits.actions.AbstractCommitInfo;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractMetadata;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractProtocol;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import java.util.*;

public class CoordinatedCommitsUtils {
  public static AbstractMetadata convertMetadataToAbstractMetadata(Metadata metadata) {
    return new AbstractMetadata() {
      @Override
      public String getId() {
        return metadata.getId();
      }

      @Override
      public String getName() {
        return metadata.getName().orElse(null);
      }

      @Override
      public String getDescription() {
        return metadata.getDescription().orElse(null);
      }

      @Override
      public String getProvider() {
        return metadata.getFormat().getProvider();
      }

      @Override
      public Map<String, String> getFormatOptions() {
        // Assuming Format class has a method to get format options
        return metadata.getFormat().getOptions();
      }

      @Override
      public String getSchemaString() {
        // Assuming Metadata class has a method to get schema string
        return metadata.getSchemaString();
      }

      @Override
      public List<String> getPartitionColumns() {
        // Assuming Metadata class has a method to get partition columns
        return VectorUtils.toJavaList(metadata.getPartitionColumnsRaw());
      }

      @Override
      public Map<String, String> getConfiguration() {
        return metadata.getConfiguration();
      }

      @Override
      public Optional<Long> getCreatedTime() {
        return metadata.getCreatedTime();
      }
    };
  }

  public static AbstractProtocol convertProtocolToAbstractProtocol(Protocol protocol) {
    return new AbstractProtocol() {
      @Override
      public int getMinReaderVersion() {
        return protocol.getMinReaderVersion();
      }

      @Override
      public int getMinWriterVersion() {
        return protocol.getMinWriterVersion();
      }

      @Override
      public Set<String> getReaderFeatures() {
        return new HashSet<>(protocol.getReaderFeatures());
      }

      @Override
      public Set<String> getWriterFeatures() {
        return new HashSet<>(protocol.getWriterFeatures());
      }
    };
  }

  public static AbstractCommitInfo convertCommitInfoToAbstractCommitInfo(CommitInfo commitInfo) {
    return () -> commitInfo.getInCommitTimestamp().orElse(commitInfo.getTimestamp());
  }
}
