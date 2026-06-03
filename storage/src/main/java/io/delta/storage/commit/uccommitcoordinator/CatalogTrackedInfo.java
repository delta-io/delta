package org.apache.spark.sql.delta.coordinatedcommits;

import io.delta.storage.commit.actions.AbstractDomainMetadata;
import io.delta.storage.commit.uniform.UniformMetadata;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class CatalogTrackedInfo {
    public static final CatalogTrackedInfo EMPTY =
        new CatalogTrackedInfo(Optional.empty(), Collections.emptyList());

    private final Optional<UniformMetadata> deltaUniformIceberg;
    private final List<AbstractDomainMetadata> domainMetadataToCommit;

    public CatalogTrackedInfo(Optional<UniformMetadata> deltaUniformIceberg) {
        this(deltaUniformIceberg, Collections.emptyList());
    }

    public CatalogTrackedInfo(
            Optional<UniformMetadata> deltaUniformIceberg,
            List<AbstractDomainMetadata> domainMetadataToCommit) {
        this.deltaUniformIceberg = deltaUniformIceberg;
        this.domainMetadataToCommit = domainMetadataToCommit;
    }

    public Optional<UniformMetadata> deltaUniformIceberg() {
        return deltaUniformIceberg;
    }

    public List<AbstractDomainMetadata> domainMetadataToCommit() {
        return domainMetadataToCommit;
    }
}
