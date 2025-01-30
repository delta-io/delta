package io.delta.kernel.engine;

import io.delta.kernel.internal.actions.PostCommitAction;
import io.delta.kernel.internal.checksum.CRCInfo;
import java.util.List;
import java.util.Optional;

public interface PostCommitHandler {

  List<PostCommitAction> getPostCommitAction(
      long version, boolean isReadyForCheckpoint, Optional<CRCInfo> crcInfo);
}
