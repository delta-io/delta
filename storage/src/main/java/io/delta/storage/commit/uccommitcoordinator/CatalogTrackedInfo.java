package org.apache.spark.sql.delta.coordinatedcommits;

import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import io.delta.storage.commit.uniform.UniformMetadata;

import java.util.Optional;

/**
 * Carries UC-specific metadata through the commit path.
 * Used by UCCommitCoordinatorClient to pass DRC-specific data
 * (etag, old metadata/protocol for diffing) without changing
 * the commitToUC() signature.
 */
public class CatalogTrackedInfo {
    public static final CatalogTrackedInfo EMPTY =
        new CatalogTrackedInfo(Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty());

    private final Optional<UniformMetadata> deltaUniformIceberg;
    private final Optional<String> etag;
    private final Optional<AbstractMetadata> oldMetadata;
    private final Optional<AbstractProtocol> oldProtocol;

    public CatalogTrackedInfo(Optional<UniformMetadata> deltaUniformIceberg) {
        this(deltaUniformIceberg, Optional.empty(),
            Optional.empty(), Optional.empty());
    }

    public CatalogTrackedInfo(
            Optional<UniformMetadata> deltaUniformIceberg,
            Optional<String> etag,
            Optional<AbstractMetadata> oldMetadata,
            Optional<AbstractProtocol> oldProtocol) {
        this.deltaUniformIceberg = deltaUniformIceberg;
        this.etag = etag;
        this.oldMetadata = oldMetadata;
        this.oldProtocol = oldProtocol;
    }

    public Optional<UniformMetadata> deltaUniformIceberg() {
        return deltaUniformIceberg;
    }

    public Optional<String> etag() {
        return etag;
    }

    public Optional<AbstractMetadata> oldMetadata() {
        return oldMetadata;
    }

    public Optional<AbstractProtocol> oldProtocol() {
        return oldProtocol;
    }
}
