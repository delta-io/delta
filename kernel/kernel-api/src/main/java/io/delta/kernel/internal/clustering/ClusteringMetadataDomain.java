package io.delta.kernel.internal.clustering;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.metadatadomain.JsonMetadataDomain;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import java.util.*;

/**
 * Represents the metadata domain for clustering.
 */
public final class ClusteringMetadataDomain extends JsonMetadataDomain {

  public static final String DOMAIN_NAME = "delta.clustering";

  @JsonProperty("clusteringColumns")
  private final List<List<String>> clusteringColumns;

  @Override
  public String getDomainName() {
    return DOMAIN_NAME;
  }

  /**
   * Constructs a ClusteringMetadataDomain with the specified clustering columns.
   *
   * @param clusteringColumns the columns used for clustering
   */
  @JsonCreator
  public ClusteringMetadataDomain(List<String> logicalClusteringColumns, StructType schema) {
    List<List<String>> physicalClusteringColumns = new ArrayList<>();
    for (String logicalName : logicalClusteringColumns) {
      physicalClusteringColumns.add(getPhysicalColumnNames(schema, logicalName));
    }
    this.clusteringColumns = physicalClusteringColumns;
  }

  private List<String> getPhysicalColumnNames(StructType schema, String logicalName) {
    String[] logicalNameParts = logicalName.split("\\.");
    List<String> physicalNameParts = new ArrayList<>();
    DataType currentType = schema;

    for (String namePart : logicalNameParts) {
      if (!(currentType instanceof StructType)) {
        throw new IllegalArgumentException("Column not found in schema: " + logicalName);
      }

      StructType structType = (StructType) currentType;
      Optional<StructField> fieldOpt = structType.fields().stream()
              .filter(field -> field.getName().toLowerCase(Locale.ROOT).equals(namePart.toLowerCase(Locale.ROOT)))
              .findFirst();

      if (!fieldOpt.isPresent()) {
        throw new IllegalArgumentException("Column not found in schema: " + logicalName);
      }

      StructField field = fieldOpt.get();
      currentType = field.getDataType();  // Move deeper into the nested structure

      // Convert logical name to physical name using Delta Kernel's column mapping
      physicalNameParts.add(ColumnMapping.getPhysicalName(field));
    }
    return physicalNameParts;
  }

  public List<List<String>> getClusteringColumns() {
    return clusteringColumns;
  }
}
