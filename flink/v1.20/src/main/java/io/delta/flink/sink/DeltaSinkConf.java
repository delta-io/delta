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

package io.delta.flink.sink;

import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.types.StructType;
import java.io.Serializable;
import java.util.Map;

public class DeltaSinkConf implements Serializable {

  public static String FILE_ROLLING_SIZE_KEY = "file-rolling-size";
  public static String SCHEMA_EVOLUTION_MODE_KEY = "schema-evolution-mode";

  private final String sinkSchemaString;
  private final long fileRollingSize;
  private final SchemaEvolutionPolicy schemaEvolutionPolicy;

  private transient StructType sinkSchema;

  public DeltaSinkConf(StructType sinkSchema, Map<String, String> conf) {
    this.sinkSchemaString = sinkSchema.toJson();
    this.sinkSchema = sinkSchema;

    this.fileRollingSize = Long.parseLong(conf.getOrDefault(FILE_ROLLING_SIZE_KEY, "104857600"));
    switch (conf.getOrDefault(SCHEMA_EVOLUTION_MODE_KEY, "no")) {
      case "allow":
        this.schemaEvolutionPolicy = new NewColumnEvolution();
        break;
      case "no":
      default:
        this.schemaEvolutionPolicy = new NoEvolution();
    }
  }

  public StructType getSinkSchema() {
    if (this.sinkSchema == null) {
      this.sinkSchema = DataTypeJsonSerDe.deserializeStructType(this.sinkSchemaString);
    }
    return this.sinkSchema;
  }

  public long getFileRollingSize() {
    return fileRollingSize;
  }

  public SchemaEvolutionPolicy getSchemaEvolutionPolicy() {
    return schemaEvolutionPolicy;
  }

  public interface SchemaEvolutionPolicy extends Serializable {
    boolean allowEvolve(StructType tableSchema, StructType sinkSchema);
  }

  static class NoEvolution implements SchemaEvolutionPolicy {
    @Override
    public boolean allowEvolve(StructType tableSchema, StructType sinkSchema) {
      return tableSchema.equivalent(sinkSchema);
    }
  }

  static class NewColumnEvolution implements SchemaEvolutionPolicy {
    @Override
    public boolean allowEvolve(StructType tableSchema, StructType sinkSchema) {
      // TODO Not implemented
      return false;
    }
  }
}
