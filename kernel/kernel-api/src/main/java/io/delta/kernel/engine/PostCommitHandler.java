package io.delta.kernel.engine;

public interface PostCommitHandler {

    void checkPoint();

    void checkSum();

}
