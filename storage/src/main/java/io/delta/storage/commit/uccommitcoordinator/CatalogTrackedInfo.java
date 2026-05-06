package org.apache.spark.sql.delta.coordinatedcommits;

import io.delta.storage.commit.uniform.UniformMetadata;

import java.util.Optional;

public class CatalogTrackedInfo {
    public static final CatalogTrackedInfo EMPTY = new CatalogTrackedInfo(Optional.empty());

    private final Optional<UniformMetadata> deltaUniformIceberg;

    public CatalogTrackedInfo(Optional<UniformMetadata> deltaUniformIceberg) {
        this.deltaUniformIceberg = deltaUniformIceberg;
    }

    public Optional<UniformMetadata> deltaUniformIceberg() {
        return deltaUniformIceberg;
    }
}
