package io.delta.standalone.actions;

import java.util.Objects;

public class NotebookInfo {
    private final String notebookId;

    public NotebookInfo(String notebookId) {
        this.notebookId = notebookId;
    }

    public String getNotebookId() {
        return notebookId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NotebookInfo that = (NotebookInfo) o;
        return Objects.equals(notebookId, that.notebookId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(notebookId);
    }
}
