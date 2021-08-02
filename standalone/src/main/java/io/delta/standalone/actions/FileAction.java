package io.delta.standalone.actions;

public interface FileAction extends Action {

    String getPath();

    boolean isDataChange();
}
