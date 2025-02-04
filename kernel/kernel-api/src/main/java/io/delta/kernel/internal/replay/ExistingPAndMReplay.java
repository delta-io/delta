package io.delta.kernel.internal.replay;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.snapshot.LogSegment;

public class ExistingPAndMReplay extends LogReplay {

  private final Protocol protocol;
  private final Metadata metadata;

  public ExistingPAndMReplay(
      Engine engine, Path dataPath, LogSegment logSegment, Protocol protocol, Metadata metadata) {
    super(engine, dataPath, logSegment);
    this.protocol = protocol;
    this.metadata = metadata;
  }

  @Override
  public Protocol getProtocol() {
    return protocol;
  }

  @Override
  public Metadata getMetadata() {
    return metadata;
  }

  @Override
  public Optional<CRCInfo> getCurrentCrcInfo() {
    return Optional.empty();
  }
}
