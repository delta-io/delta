package io.delta.storage.commit.uccommitcoordinator;

import java.io.Closeable;

public interface UCTokenProvider extends Closeable {
    String accessToken();
}
