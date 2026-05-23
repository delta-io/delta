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
    private final List<AbstractDomainMetadata> domainMetadatas;

    public CatalogTrackedInfo(Optional<UniformMetadata> deltaUniformIceberg) {
        this(deltaUniformIceberg, Collections.emptyList());
    }

    public CatalogTrackedInfo(
            Optional<UniformMetadata> deltaUniformIceberg,
            List<AbstractDomainMetadata> domainMetadatas) {
        this.deltaUniformIceberg = deltaUniformIceberg;
        this.domainMetadatas = Collections.unmodifiableList(domainMetadatas);
    }

    public Optional<UniformMetadata> deltaUniformIceberg() {
        return deltaUniformIceberg;
    }

    public List<AbstractDomainMetadata> domainMetadatas() {
        return domainMetadatas;
    }
}
