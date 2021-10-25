// TODO: copyright

package io.delta.standalone.actions;

/**
 * Generic interface for {@link Action}s pertaining to the addition and removal of files.
 */
public interface FileAction extends Action {

    /**
     @return the relative path or the absolute path of the file being added or removed by this
      *      action. If it's a relative path, it's relative to the root of the table. Note: the path
      *      is encoded and should be decoded by {@code new java.net.URI(path)} when using it.
     */
    String getPath();

    /**
     * @return whether any data was changed as a result of this file being added or removed.
     */
    boolean isDataChange();
}
