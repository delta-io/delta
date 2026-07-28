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
    private final List<AbstractDomainMetadata> transactionDomainMetadata;

    public CatalogTrackedInfo(Optional<UniformMetadata> deltaUniformIceberg) {
        this(deltaUniformIceberg, Collections.emptyList());
    }

    public CatalogTrackedInfo(
            Optional<UniformMetadata> deltaUniformIceberg,
            List<AbstractDomainMetadata> transactionDomainMetadata) {
        this.deltaUniformIceberg = deltaUniformIceberg;
        this.transactionDomainMetadata = transactionDomainMetadata;
    }

    public Optional<UniformMetadata> deltaUniformIceberg() {
        return deltaUniformIceberg;
    }

    public List<AbstractDomainMetadata> transactionDomainMetadata() {
        return transactionDomainMetadata;
    }
}
