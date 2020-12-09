/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.standalone.actions;

import java.util.Objects;

/** Represents the Databricks Notebook information that committed to the Delta table. */
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
