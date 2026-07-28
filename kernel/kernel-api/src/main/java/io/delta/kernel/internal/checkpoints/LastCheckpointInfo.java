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
package io.delta.kernel.internal.checkpoints;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.kernel.internal.util.JsonUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Parsed, typed view of a {@code _last_checkpoint} file.
 *
 * <p>{@link CheckpointMetaData} projects only the classic columnar fields ({@code version}, {@code
 * size}, {@code parts}, {@code tags}) because the columnar JSON reader cannot express the on-disk
 * pointer's optional and recursive fields (notably {@code checkpointSchema}, a schema-of-schema).
 * This type is the counterpart for callers that need the full pointer -- e.g. the SoftStore /
 * metadata-cache path that mirrors these fields into a wire format. It is produced from the raw
 * blob (see {@link LastCheckpointSerialized}) via a plain JSON tree walk, so no field has to be
 * projectable into a fixed columnar schema.
 *
 * <p>The field set mirrors DBR's {@code LastCheckpointInfo} case class one-for-one, except for the
 * Edge-only {@code checkpointFiles} / {@code consecutiveCompactedFiles}, which are not JSON
 * serialized into {@code _last_checkpoint} and so are never present in the blob.
 *
 * <p>{@code checkpointSchema} is deliberately kept as its raw JSON string ({@code StructType.json})
 * rather than decoded into a {@code StructType}: it round-trips losslessly and semantically, and it
 * avoids eagerly decoding a recursive schema-of-schema that most callers relay verbatim. Callers
 * that want the structural type can parse it with {@code DataTypeJsonSerDe.deserializeStructType}.
 */
public final class LastCheckpointInfo {

  // ----- _last_checkpoint field names (must match DBR LastCheckpointInfo's JSON) -----
  private static final String VERSION = "version";
  private static final String SIZE = "size";
  private static final String PARTS = "parts";
  private static final String SIZE_IN_BYTES = "sizeInBytes";
  private static final String NUM_OF_ADD_FILES = "numOfAddFiles";
  private static final String CHECKPOINT_SCHEMA = "checkpointSchema";
  private static final String CHECKSUM = "checksum";
  private static final String V2_CHECKPOINT = "v2Checkpoint";

  /**
   * Parses a {@code _last_checkpoint} JSON blob into a {@link LastCheckpointInfo}.
   *
   * @param json the raw {@code _last_checkpoint} contents (see {@link
   *     LastCheckpointSerialized#json()})
   * @return the parsed pointer
   * @throws IllegalArgumentException if the blob is not a JSON object or lacks the required {@code
   *     version} / {@code size} fields
   */
  public static LastCheckpointInfo fromJson(String json) {
    JsonNode root;
    try {
      root = JsonUtils.mapper().readTree(json);
    } catch (Exception e) {
      throw new IllegalArgumentException("_last_checkpoint is not parseable JSON", e);
    }
    return fromJsonNode(root);
  }

  /** Parses an already-decoded {@code _last_checkpoint} tree. See {@link #fromJson(String)}. */
  public static LastCheckpointInfo fromJsonNode(JsonNode root) {
    if (root == null || !root.isObject()) {
      throw new IllegalArgumentException("_last_checkpoint must be a JSON object");
    }
    return new LastCheckpointInfo(
        requiredLong(root, VERSION),
        requiredLong(root, SIZE),
        optionalInt(root, PARTS),
        optionalLong(root, SIZE_IN_BYTES),
        optionalLong(root, NUM_OF_ADD_FILES),
        // Keep the schema-of-schema as its raw JSON string; see class doc.
        rawJson(root, CHECKPOINT_SCHEMA),
        optionalString(root, CHECKSUM),
        LastCheckpointV2.fromJsonNode(root.get(V2_CHECKPOINT)));
  }

  private final long version;
  private final long size;
  private final Optional<Integer> parts;
  private final Optional<Long> sizeInBytes;
  private final Optional<Long> numOfAddFiles;
  private final Optional<String> checkpointSchemaJson;
  private final Optional<String> checksum;
  private final Optional<LastCheckpointV2> v2Checkpoint;

  public LastCheckpointInfo(
      long version,
      long size,
      Optional<Integer> parts,
      Optional<Long> sizeInBytes,
      Optional<Long> numOfAddFiles,
      Optional<String> checkpointSchemaJson,
      Optional<String> checksum,
      Optional<LastCheckpointV2> v2Checkpoint) {
    this.version = version;
    this.size = size;
    this.parts = parts;
    this.sizeInBytes = sizeInBytes;
    this.numOfAddFiles = numOfAddFiles;
    this.checkpointSchemaJson = checkpointSchemaJson;
    this.checksum = checksum;
    this.v2Checkpoint = v2Checkpoint;
  }

  public long getVersion() {
    return version;
  }

  public long getSize() {
    return size;
  }

  public Optional<Integer> getParts() {
    return parts;
  }

  public Optional<Long> getSizeInBytes() {
    return sizeInBytes;
  }

  public Optional<Long> getNumOfAddFiles() {
    return numOfAddFiles;
  }

  /**
   * The checkpoint schema as its raw {@code StructType.json} string, or empty when the pointer
   * omits it. Not decoded into a {@code StructType}; see the class doc for why.
   */
  public Optional<String> getCheckpointSchemaJson() {
    return checkpointSchemaJson;
  }

  public Optional<String> getChecksum() {
    return checksum;
  }

  public Optional<LastCheckpointV2> getV2Checkpoint() {
    return v2Checkpoint;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LastCheckpointInfo)) {
      return false;
    }
    LastCheckpointInfo that = (LastCheckpointInfo) o;
    return version == that.version
        && size == that.size
        && parts.equals(that.parts)
        && sizeInBytes.equals(that.sizeInBytes)
        && numOfAddFiles.equals(that.numOfAddFiles)
        && checkpointSchemaJson.equals(that.checkpointSchemaJson)
        && checksum.equals(that.checksum)
        && v2Checkpoint.equals(that.v2Checkpoint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        version,
        size,
        parts,
        sizeInBytes,
        numOfAddFiles,
        checkpointSchemaJson,
        checksum,
        v2Checkpoint);
  }

  @Override
  public String toString() {
    return "LastCheckpointInfo{"
        + "version="
        + version
        + ", size="
        + size
        + ", parts="
        + parts
        + ", sizeInBytes="
        + sizeInBytes
        + ", numOfAddFiles="
        + numOfAddFiles
        + ", checkpointSchemaJson="
        + (checkpointSchemaJson.isPresent() ? "<present>" : "<absent>")
        + ", checksum="
        + checksum
        + ", v2Checkpoint="
        + v2Checkpoint
        + '}';
  }

  /**
   * The V2-checkpoint pointer nested under {@code v2Checkpoint}. Mirrors DBR's {@code
   * LastCheckpointV2}.
   *
   * <p>{@code nonFileActions} is exposed as the list of verbatim per-action JSON objects (e.g.
   * {@code {"protocol":{...}}}, {@code {"metaData":{...}}}, {@code {"checkpointMetadata":{...}}}).
   * Decoding those into fully typed action objects is intentionally left to the consumer: the
   * action model (Metadata / Protocol / DomainMetadata / ...) is large, and the SoftStore codec
   * maps them into wire types the kernel has no knowledge of. The single action every consumer
   * needs -- {@code checkpointMetadata} -- is surfaced directly via {@link
   * #getCheckpointMetadataVersion()}. {@code sidecarFiles} is fully parsed since it is small.
   */
  public static final class LastCheckpointV2 {
    private static final String PATH = "path";
    private static final String SIZE_IN_BYTES = "sizeInBytes";
    private static final String MODIFICATION_TIME = "modificationTime";
    private static final String NON_FILE_ACTIONS = "nonFileActions";
    private static final String SIDECAR_FILES = "sidecarFiles";
    private static final String CHECKPOINT_METADATA = "checkpointMetadata";
    private static final String VERSION = "version";

    static Optional<LastCheckpointV2> fromJsonNode(JsonNode node) {
      if (node == null || !node.isObject()) {
        return Optional.empty();
      }
      return Optional.of(
          new LastCheckpointV2(
              requiredString(node, PATH),
              requiredLong(node, SIZE_IN_BYTES),
              requiredLong(node, MODIFICATION_TIME),
              rawJsonArrayElements(node.get(NON_FILE_ACTIONS)),
              SidecarFileInfo.fromArray(node.get(SIDECAR_FILES))));
    }

    private final String path;
    private final long sizeInBytes;
    private final long modificationTime;
    // Absent (None) vs empty list is preserved: absent = info missing, empty = no such actions.
    private final Optional<List<String>> nonFileActionsJson;
    private final Optional<List<SidecarFileInfo>> sidecarFiles;

    public LastCheckpointV2(
        String path,
        long sizeInBytes,
        long modificationTime,
        Optional<List<String>> nonFileActionsJson,
        Optional<List<SidecarFileInfo>> sidecarFiles) {
      this.path = path;
      this.sizeInBytes = sizeInBytes;
      this.modificationTime = modificationTime;
      this.nonFileActionsJson = nonFileActionsJson;
      this.sidecarFiles = sidecarFiles;
    }

    public String getPath() {
      return path;
    }

    public long getSizeInBytes() {
      return sizeInBytes;
    }

    public long getModificationTime() {
      return modificationTime;
    }

    /** The non-file actions as verbatim per-action JSON objects. See {@link LastCheckpointV2}. */
    public Optional<List<String>> getNonFileActionsJson() {
      return nonFileActionsJson;
    }

    public Optional<List<SidecarFileInfo>> getSidecarFiles() {
      return sidecarFiles;
    }

    /**
     * The {@code version} of the {@code checkpointMetadata} action among {@code nonFileActions}, or
     * empty when the pointer carries no such action. This is the field the metadata-cache path uses
     * to decide whether a V2 pointer is cacheable.
     */
    public Optional<Long> getCheckpointMetadataVersion() {
      if (!nonFileActionsJson.isPresent()) {
        return Optional.empty();
      }
      for (String actionJson : nonFileActionsJson.get()) {
        JsonNode action;
        try {
          action = JsonUtils.mapper().readTree(actionJson);
        } catch (Exception e) {
          continue;
        }
        JsonNode cm = action.get(CHECKPOINT_METADATA);
        if (cm != null && cm.isObject()) {
          return optionalLong(cm, VERSION);
        }
      }
      return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof LastCheckpointV2)) {
        return false;
      }
      LastCheckpointV2 that = (LastCheckpointV2) o;
      return sizeInBytes == that.sizeInBytes
          && modificationTime == that.modificationTime
          && path.equals(that.path)
          && nonFileActionsJson.equals(that.nonFileActionsJson)
          && sidecarFiles.equals(that.sidecarFiles);
    }

    @Override
    public int hashCode() {
      return Objects.hash(path, sizeInBytes, modificationTime, nonFileActionsJson, sidecarFiles);
    }

    @Override
    public String toString() {
      return "LastCheckpointV2{path=" + path + ", sizeInBytes=" + sizeInBytes + '}';
    }
  }

  /** A sidecar-file pointer inside a V2 checkpoint. Mirrors DBR's {@code SidecarFile}. */
  public static final class SidecarFileInfo {
    private static final String PATH = "path";
    private static final String SIZE_IN_BYTES = "sizeInBytes";
    private static final String MODIFICATION_TIME = "modificationTime";
    private static final String TAGS = "tags";

    static Optional<List<SidecarFileInfo>> fromArray(JsonNode arrayNode) {
      if (arrayNode == null || !arrayNode.isArray()) {
        return Optional.empty();
      }
      List<SidecarFileInfo> files = new ArrayList<>();
      for (JsonNode f : arrayNode) {
        files.add(
            new SidecarFileInfo(
                requiredString(f, PATH),
                requiredLong(f, SIZE_IN_BYTES),
                requiredLong(f, MODIFICATION_TIME),
                stringMap(f.get(TAGS))));
      }
      return Optional.of(Collections.unmodifiableList(files));
    }

    private final String path;
    private final long sizeInBytes;
    private final long modificationTime;
    private final Map<String, String> tags;

    public SidecarFileInfo(
        String path, long sizeInBytes, long modificationTime, Map<String, String> tags) {
      this.path = path;
      this.sizeInBytes = sizeInBytes;
      this.modificationTime = modificationTime;
      this.tags = tags;
    }

    public String getPath() {
      return path;
    }

    public long getSizeInBytes() {
      return sizeInBytes;
    }

    public long getModificationTime() {
      return modificationTime;
    }

    public Map<String, String> getTags() {
      return tags;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SidecarFileInfo)) {
        return false;
      }
      SidecarFileInfo that = (SidecarFileInfo) o;
      return sizeInBytes == that.sizeInBytes
          && modificationTime == that.modificationTime
          && path.equals(that.path)
          && tags.equals(that.tags);
    }

    @Override
    public int hashCode() {
      return Objects.hash(path, sizeInBytes, modificationTime, tags);
    }

    @Override
    public String toString() {
      return "SidecarFileInfo{path=" + path + ", sizeInBytes=" + sizeInBytes + '}';
    }
  }

  // ----- JSON helpers -----

  private static long requiredLong(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || !v.isNumber()) {
      throw new IllegalArgumentException(
          "_last_checkpoint is missing required numeric field: " + field);
    }
    return v.asLong();
  }

  private static String requiredString(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || !v.isTextual()) {
      throw new IllegalArgumentException(
          "_last_checkpoint is missing required string field: " + field);
    }
    return v.asText();
  }

  private static Optional<Long> optionalLong(JsonNode node, String field) {
    JsonNode v = node.get(field);
    return (v != null && v.isNumber()) ? Optional.of(v.asLong()) : Optional.empty();
  }

  private static Optional<Integer> optionalInt(JsonNode node, String field) {
    JsonNode v = node.get(field);
    return (v != null && v.isNumber()) ? Optional.of(v.asInt()) : Optional.empty();
  }

  private static Optional<String> optionalString(JsonNode node, String field) {
    JsonNode v = node.get(field);
    return (v != null && v.isTextual()) ? Optional.of(v.asText()) : Optional.empty();
  }

  /** Returns the compact JSON text of a sub-object field (e.g. {@code checkpointSchema}). */
  private static Optional<String> rawJson(JsonNode node, String field) {
    JsonNode v = node.get(field);
    return (v != null && !v.isNull()) ? Optional.of(v.toString()) : Optional.empty();
  }

  /** Returns each element of a JSON array as its verbatim JSON text; empty Optional if absent. */
  private static Optional<List<String>> rawJsonArrayElements(JsonNode arrayNode) {
    if (arrayNode == null || !arrayNode.isArray()) {
      return Optional.empty();
    }
    List<String> out = new ArrayList<>();
    for (JsonNode e : arrayNode) {
      out.add(e.toString());
    }
    return Optional.of(Collections.unmodifiableList(out));
  }

  private static Map<String, String> stringMap(JsonNode node) {
    if (node == null || !node.isObject()) {
      return Collections.emptyMap();
    }
    Map<String, String> map = new LinkedHashMap<>();
    ObjectNode obj = (ObjectNode) node;
    obj.fields().forEachRemaining(e -> map.put(e.getKey(), e.getValue().asText()));
    return Collections.unmodifiableMap(map);
  }
}
