package io.delta.kernel.defaults.engine;

import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.PostCommitHandler;
import io.delta.kernel.internal.actions.PostCommitAction;
import io.delta.kernel.internal.checksum.CRCInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DefaultPostCommitHandler implements PostCommitHandler {

  @Override
  public List<PostCommitAction> getPostCommitAction(
      long version, boolean isReadyForCheckpoint, Optional<CRCInfo> crcInfo) {
    List<PostCommitAction> actions = new ArrayList<>();
    if (isReadyForCheckpoint) {
      actions.add(
          new PostCommitAction() {
            @Override
            public void threadSafeInvoke(Table table, Engine engine) throws IOException {
              table.checkpoint(engine, version);
            }

            @Override
            public String getType() {
              return "checkpoint";
            }
          });
    }

    if (!crcInfo.isPresent()) {
//      actions.add(
//          new PostCommitAction() {
//            @Override
//            public void threadSafeInvoke(Table table, Engine engine) throws IOException {
//              table.checksum(engine, version);
//            }
//
//            @Override
//            public String getType() {
//              return "checksum large";
//            }
//          });
    } else {
      actions.add(
          new PostCommitAction() {
            @Override
            public void threadSafeInvoke(Table table, Engine engine) throws IOException {
              table.checksumSimple(engine, crcInfo.get());
            }

            @Override
            public String getType() {
              return "checksum simple";
            }
          });
    }

    return actions;
  }
}
