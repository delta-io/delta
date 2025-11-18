package io.delta.storage.commit.uccommitcoordinator;

import java.io.IOException;

public class FixedUCTokenProvider implements UCTokenProvider{
    private final String token;
    public FixedUCTokenProvider(String token){
        this.token = token;
    }

    @Override
    public String accessToken() {
        return token;
    }

    @Override
    public void close() throws IOException {
    }
}
