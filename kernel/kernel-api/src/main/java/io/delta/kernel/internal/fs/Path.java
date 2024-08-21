/*
 * Copyright (2023) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.fs;

import java.io.InvalidObjectException;
import java.io.ObjectInputValidation;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

/**
 * Names a file or directory in a FileSystem. Path strings use slash as the directory separator.
 *
 * <p>Taken from https://github.com/apache/hadoop/blob/branch-3.3
 * .4/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/Path.java
 */
// TODO remove this class
public class Path implements Comparable<Path>, Serializable, ObjectInputValidation {

  /** The directory separator, a slash. */
  public static final String SEPARATOR = "/";

  /** The directory separator, a slash, as a character. */
  public static final char SEPARATOR_CHAR = '/';

  /** The current directory, ".". */
  public static final String CUR_DIR = ".";

  /** Whether the current host is a Windows machine. */
  public static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");

  /** Pre-compiled regular expressions to detect path formats. */
  private static final Pattern HAS_DRIVE_LETTER_SPECIFIER = Pattern.compile("^/?[a-zA-Z]:");

  /** Pre-compiled regular expressions to detect duplicated slashes. */
  private static final Pattern SLASHES = Pattern.compile("/+");

  private static final long serialVersionUID = 0xad00f;

  private URI uri; // a hierarchical uri

  /**
   * Create a new Path based on the child path resolved against the parent path.
   *
   * @param parent the parent path
   * @param child the child path
   */
  public Path(String parent, String child) {
    this(new Path(parent), new Path(child));
  }

  /**
   * Create a new Path based on the child path resolved against the parent path.
   *
   * @param parent the parent path
   * @param child the child path
   */
  public Path(Path parent, String child) {
    this(parent, new Path(child));
  }

  /**
   * Create a new Path based on the child path resolved against the parent path.
   *
   * @param parent the parent path
   * @param child the child path
   */
  public Path(String parent, Path child) {
    this(new Path(parent), child);
  }

  /**
   * Create a new Path based on the child path resolved against the parent path.
   *
   * @param parent the parent path
   * @param child the child path
   */
  public Path(Path parent, Path child) {
    // Add a slash to parent's path so resolution is compatible with URI's
    URI parentUri = parent.uri;
    String parentPath = parentUri.getPath();
    if (!(parentPath.equals("/") || parentPath.isEmpty())) {
      try {
        parentUri =
            new URI(
                parentUri.getScheme(),
                parentUri.getAuthority(),
                parentUri.getPath() + "/",
                null,
                parentUri.getFragment());
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
    }
    URI resolved = parentUri.resolve(child.uri);
    initialize(
        resolved.getScheme(), resolved.getAuthority(), resolved.getPath(), resolved.getFragment());
  }

  private void checkPathArg(String path) throws IllegalArgumentException {
    // disallow construction of a Path from an empty string
    if (path == null) {
      throw new IllegalArgumentException("Can not create a Path from a null string");
    }
    if (path.length() == 0) {
      throw new IllegalArgumentException("Can not create a Path from an empty string");
    }
  }

  /**
   * Construct a path from a String. Path strings are URIs, but with unescaped elements and some
   * additional normalization.
   *
   * @param pathString the path string
   */
  public Path(String pathString) throws IllegalArgumentException {
    checkPathArg(pathString);

    // We can't use 'new URI(String)' directly, since it assumes things are
    // escaped, which we don't require of Paths.

    // add a slash in front of paths with Windows drive letters
    if (hasWindowsDrive(pathString) && pathString.charAt(0) != '/') {
      pathString = "/" + pathString;
    }

    // parse uri components
    String scheme = null;
    String authority = null;

    int start = 0;

    // parse uri scheme, if any
    int colon = pathString.indexOf(':');
    int slash = pathString.indexOf('/');
    if ((colon != -1) && ((slash == -1) || (colon < slash))) { // has a scheme
      scheme = pathString.substring(0, colon);
      start = colon + 1;
    }

    // parse uri authority, if any
    if (pathString.startsWith("//", start) && (pathString.length() - start > 2)) { // has authority
      int nextSlash = pathString.indexOf('/', start + 2);
      int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
      authority = pathString.substring(start + 2, authEnd);
      start = authEnd;
    }

    // uri path is the rest of the string -- query & fragment not supported
    String path = pathString.substring(start, pathString.length());

    initialize(scheme, authority, path, null);
  }

  /**
   * Construct a path from a URI
   *
   * @param aUri the source URI
   */
  public Path(URI aUri) {
    uri = aUri.normalize();
  }

  /**
   * Construct a Path from components.
   *
   * @param scheme the scheme
   * @param authority the authority
   * @param path the path
   */
  public Path(String scheme, String authority, String path) {
    checkPathArg(path);

    // add a slash in front of paths with Windows drive letters
    if (hasWindowsDrive(path) && path.charAt(0) != '/') {
      path = "/" + path;
    }

    // add "./" in front of Linux relative paths so that a path containing
    // a colon e.q. "a:b" will not be interpreted as scheme "a".
    if (!WINDOWS && path.charAt(0) != '/') {
      path = "./" + path;
    }

    initialize(scheme, authority, path, null);
  }

  private void initialize(String scheme, String authority, String path, String fragment) {
    try {
      this.uri =
          new URI(scheme, authority, normalizePath(scheme, path), null, fragment).normalize();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Normalize a path string to use non-duplicated forward slashes as the path separator and remove
   * any trailing path separators.
   *
   * @param scheme the URI scheme. Used to deduce whether we should replace backslashes or not
   * @param path the scheme-specific part
   * @return the normalized path string
   */
  private static String normalizePath(String scheme, String path) {
    // Remove duplicated slashes.
    path = SLASHES.matcher(path).replaceAll("/");

    // Remove backslashes if this looks like a Windows path. Avoid
    // the substitution if it looks like a non-local URI.
    if (WINDOWS
        && (hasWindowsDrive(path)
            || (scheme == null)
            || (scheme.isEmpty())
            || (scheme.equals("file")))) {
      path = path.replace("\\", "/");
    }

    // trim trailing slash from non-root path (ignoring windows drive)
    int minLength = startPositionWithoutWindowsDrive(path) + 1;
    if (path.length() > minLength && path.endsWith(SEPARATOR)) {
      path = path.substring(0, path.length() - 1);
    }

    return path;
  }

  private static boolean hasWindowsDrive(String path) {
    return (WINDOWS && HAS_DRIVE_LETTER_SPECIFIER.matcher(path).find());
  }

  private static int startPositionWithoutWindowsDrive(String path) {
    if (hasWindowsDrive(path)) {
      return path.charAt(0) == SEPARATOR_CHAR ? 3 : 2;
    } else {
      return 0;
    }
  }

  /**
   * Convert this Path to a URI.
   *
   * @return this Path as a URI
   */
  public URI toUri() {
    return uri;
  }

  /**
   * Returns true if the path component (i.e. directory) of this URI is absolute.
   *
   * @return whether this URI's path is absolute
   */
  public boolean isUriPathAbsolute() {
    int start = startPositionWithoutWindowsDrive(uri.getPath());
    return uri.getPath().startsWith(SEPARATOR, start);
  }

  /**
   * Returns true if the path component (i.e. directory) of this URI is absolute. This method is a
   * wrapper for {@link #isUriPathAbsolute()}.
   *
   * @return whether this URI's path is absolute
   */
  public boolean isAbsolute() {
    return isUriPathAbsolute();
  }

  /**
   * Returns true if and only if this path represents the root of a file system.
   *
   * @return true if and only if this path represents the root of a file system
   */
  public boolean isRoot() {
    return getParent() == null;
  }

  /**
   * Returns the final component of this path.
   *
   * @return the final component of this path
   */
  public String getName() {
    String path = uri.getPath();
    int slash = path.lastIndexOf(SEPARATOR);
    return path.substring(slash + 1);
  }

  /**
   * Returns the parent of a path or null if at root.
   *
   * @return the parent of a path or null if at root
   */
  public Path getParent() {
    String path = uri.getPath();
    int lastSlash = path.lastIndexOf('/');
    int start = startPositionWithoutWindowsDrive(path);
    if ((path.length() == start)
        || // empty path
        (lastSlash == start && path.length() == start + 1)) { // at root
      return null;
    }
    String parent;
    if (lastSlash == -1) {
      parent = CUR_DIR;
    } else {
      parent = path.substring(0, lastSlash == start ? start + 1 : lastSlash);
    }
    return new Path(uri.getScheme(), uri.getAuthority(), parent);
  }

  @Override
  public String toString() {
    // we can't use uri.toString(), which escapes everything, because we want
    // illegal characters unescaped in the string, for glob processing, etc.
    StringBuilder buffer = new StringBuilder();
    if (uri.getScheme() != null) {
      buffer.append(uri.getScheme()).append(":");
    }
    if (uri.getAuthority() != null) {
      buffer.append("//").append(uri.getAuthority());
    }
    if (uri.getPath() != null) {
      String path = uri.getPath();
      if (path.indexOf('/') == 0
          && hasWindowsDrive(path)
          && // has windows drive
          uri.getScheme() == null
          && // but no scheme
          uri.getAuthority() == null) { // or authority
        path = path.substring(1); // remove slash before drive
      }
      buffer.append(path);
    }
    if (uri.getFragment() != null) {
      buffer.append("#").append(uri.getFragment());
    }
    return buffer.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Path)) {
      return false;
    }
    Path that = (Path) o;
    return this.uri.equals(that.uri);
  }

  @Override
  public int hashCode() {
    return uri.hashCode();
  }

  @Override
  public int compareTo(Path o) {
    return this.uri.compareTo(o.uri);
  }

  /**
   * Validate the contents of a deserialized Path, so as to defend against malicious object streams.
   *
   * @throws InvalidObjectException if there's no URI
   */
  @Override
  public void validateObject() throws InvalidObjectException {
    if (uri == null) {
      throw new InvalidObjectException("No URI in deserialized Path");
    }
  }
}
