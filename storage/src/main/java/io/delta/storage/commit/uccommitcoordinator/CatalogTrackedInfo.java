package org.apache.spark.sql.delta.coordinatedcommits;

import io.delta.storage.commit.actions.AbstractDomainMetadata;
import io.delta.storage.commit.uniform.UniformMetadata;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class CatalogTrackedInfo {
    public static final CatalogTrackedInfo EMPTY = new CatalogTrackedInfo(Optional.empty());

    private final Optional<UniformMetadata> deltaUniformIceberg;

    private final List<AbstractDomainMetadata> oldDomainMetadata;

    private final List<AbstractDomainMetadata> latestDomainMetadata;

    public CatalogTrackedInfo(Optional<UniformMetadata> deltaUniformIceberg) {
        this(deltaUniformIceberg, Collections.emptyList(), Collections.emptyList());
    }

    public CatalogTrackedInfo(
            Optional<UniformMetadata> deltaUniformIceberg,
            List<AbstractDomainMetadata> oldDomainMetadata,
            List<AbstractDomainMetadata> latestDomainMetadata) {
        this.deltaUniformIceberg = deltaUniformIceberg;
        this.oldDomainMetadata = oldDomainMetadata;
        this.latestDomainMetadata = latestDomainMetadata;
    }

    public Optional<UniformMetadata> deltaUniformIceberg() {
        return deltaUniformIceberg;
    }

    public List<AbstractDomainMetadata> oldDomainMetadata() {
        return oldDomainMetadata;
    }

    public List<AbstractDomainMetadata> latestDomainMetadata() {
        return latestDomainMetadata;
    }
}
