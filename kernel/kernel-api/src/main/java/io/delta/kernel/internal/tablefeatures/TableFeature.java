/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.tablefeatures;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.internal.actions.Metadata;
import java.util.Collections;
import java.util.Set;

/**
 * Base class for table features.
 *
 * <p>A feature can be <b>explicitly supported</b> by a table's protocol when the protocol contains
 * a feature's `name`. Writers (for writer-only features) or readers and writers (for reader-writer
 * features) must recognize supported features and must handle them appropriately.
 *
 * <p>A table feature that released before Delta Table Features (reader version 3 and writer version
 * 7) is considered as a <strong>legacy feature</strong>. Legacy features are <strong> implicitly
 * supported</strong> when (a) the protocol does not support table features, i.e., has reader
 * version less than 3 or writer version less than 7 and (b) the feature's minimum reader/writer
 * version is less than or equal to the current protocol's reader/writer version.
 *
 * <p>Separately, a feature can be automatically supported by a table's metadata when certain
 * feature-specific table properties are set. For example, `changeDataFeed` is automatically
 * supported when there's a table property `delta.enableChangeDataFeed=true`. See {@link
 * FeatureAutoEnabledByMetadata} for details on how to define such features. This is independent of
 * the table's enabled features. When a feature is supported (explicitly or implicitly) by the table
 * protocol but its metadata requirements are not satisfied, then clients still have to understand
 * the feature (at least to the extent that they can read and preserve the existing data in the
 * table that uses the feature).
 */
public abstract class TableFeature {

  /////////////////////////////////////////////////////////////////////////////////
  /// Instance variables.                                                       ///
  /////////////////////////////////////////////////////////////////////////////////
  private final String featureName;
  private final int minReaderVersion;
  private final int minWriterVersion;

  /////////////////////////////////////////////////////////////////////////////////
  /// Public methods.                                                           ///
  /////////////////////////////////////////////////////////////////////////////////
  /**
   * Constructor. Does validations to make sure:
   *
   * <ul>
   *   <li>Feature name is not null or empty and has valid characters
   *   <li>minReaderVersion is always 0 for writer features
   * </ul>
   *
   * @param featureName a globally-unique string indicator to represent the feature. All characters
   *     must be letters (a-z, A-Z), digits (0-9), '-', or '_'. Words must be in camelCase.
   * @param minReaderVersion the minimum reader version this feature requires. For a feature that
   *     can only be explicitly supported, this is either `0` (i.e writerOnly feature) or `3` (the
   *     reader protocol version that supports table features), depending on the feature is
   *     writer-only or reader-writer. For a legacy feature that can be implicitly supported, this
   *     is the first protocol version which the feature is introduced.
   * @param minWriterVersion the minimum writer version this feature requires. For a feature that
   *     can only be explicitly supported, this is the writer protocol `7` that supports table
   *     features. For a legacy feature that can be implicitly supported, this is the first protocol
   *     version which the feature is introduced.
   */
  public TableFeature(String featureName, int minReaderVersion, int minWriterVersion) {
    this.featureName = requireNonNull(featureName, "name is null");
    checkArgument(!featureName.isEmpty(), "name is empty");
    checkArgument(
        featureName.chars().allMatch(c -> Character.isLetterOrDigit(c) || c == '-' || c == '_'),
        "name contains invalid characters: " + featureName);
    checkArgument(minReaderVersion >= 0, "minReaderVersion is negative");
    checkArgument(minWriterVersion >= 1, "minWriterVersion is less than 1");
    this.minReaderVersion = minReaderVersion;
    this.minWriterVersion = minWriterVersion;

    validate();
  }

  /** @return the name of the table feature. */
  public String featureName() {
    return featureName;
  }

  /**
   * @return true if this feature is applicable to both reader and writer, false if it is
   *     writer-only.
   */
  public boolean isReaderWriterFeature() {
    return this instanceof ReaderWriterFeatureType;
  }

  /** @return the minimum reader version this feature requires */
  public int minReaderVersion() {
    return minReaderVersion;
  }

  /** @return the minimum writer version that this feature requires. */
  public int minWriterVersion() {
    return minWriterVersion;
  }

  /** @return if this feature is a legacy feature? */
  public boolean isLegacyFeature() {
    return this instanceof LegacyFeatureType;
  }

  /**
   * Set of table features that this table feature depends on. I.e. the set of features that need to
   * be enabled if this table feature is enabled.
   *
   * @return the set of table features that this table feature depends on.
   */
  public Set<TableFeature> requiredFeatures() {
    return Collections.emptySet();
  }

  /**
   * Does Kernel has support to read a table containing this feature? Default implementation returns
   * true. Features should override this method if they have special requirements or not supported
   * by the Kernel yet.
   *
   * @return true if Kernel has support to read a table containing this feature.
   */
  public boolean hasKernelReadSupport() {
    checkArgument(isReaderWriterFeature(), "Should be called only for reader-writer features");
    return true;
  }

  /**
   * Does Kernel has support to write a table containing this feature? Default implementation
   * returns true. Features should override this method if they have special requirements or not
   * supported by the Kernel yet.
   *
   * @param metadata the metadata of the table. Sometimes checking the metadata is necessary to know
   *     the Kernel can write the table or not.
   * @return true if Kernel has support to write a table containing this feature.
   */
  public boolean hasKernelWriteSupport(Metadata metadata) {
    return true;
  }

  /////////////////////////////////////////////////////////////////////////////////
  /// Define the {@link TableFeature}s traits that define behavior/attributes.  ///
  /////////////////////////////////////////////////////////////////////////////////
  /**
   * An interface to indicate a feature is legacy, i.e., released before Table Features. All legacy
   * features are auto enabled by metadata.
   */
  public interface LegacyFeatureType extends FeatureAutoEnabledByMetadata {}

  /** An interface to indicate a feature applies to readers and writers. */
  public interface ReaderWriterFeatureType {}

  /////////////////////////////////////////////////////////////////////////////////
  /// Base classes for each of the feature category.                            ///
  /////////////////////////////////////////////////////////////////////////////////
  /** A base class for all table legacy writer-only features. */
  public abstract static class LegacyWriterFeature extends TableFeature
      implements LegacyFeatureType {
    public LegacyWriterFeature(String featureName, int minWriterVersion) {
      super(featureName, /* minReaderVersion = */ 0, minWriterVersion);
    }

    @Override
    public boolean hasKernelReadSupport() {
      return true;
    }
  }

  /** A base class for all table legacy reader-writer features. */
  public abstract static class LegacyReaderWriterFeature extends TableFeature
      implements LegacyFeatureType, ReaderWriterFeatureType {
    public LegacyReaderWriterFeature(
        String featureName, int minReaderVersion, int minWriterVersion) {
      super(featureName, minReaderVersion, minWriterVersion);
    }
  }

  /** A base class for all non-legacy table writer features. */
  public abstract static class WriterFeature extends TableFeature {
    public WriterFeature(String featureName, int minWriterVersion) {
      super(featureName, /* minReaderVersion = */ 0, minWriterVersion);
    }

    @Override
    public boolean hasKernelReadSupport() {
      return true;
    }
  }

  /** A base class for all non-legacy table reader-writer features. */
  public abstract static class ReaderWriterFeature extends TableFeature
      implements ReaderWriterFeatureType {
    public ReaderWriterFeature(String featureName, int minReaderVersion, int minWriterVersion) {
      super(featureName, minReaderVersion, minWriterVersion);
    }
  }

  /**
   * Validate the table feature. This method should throw an exception if the table feature
   * properties are invalid. Should be called after the object deriving the {@link TableFeature} is
   * constructed.
   */
  private void validate() {
    if (!isReaderWriterFeature()) {
      checkArgument(minReaderVersion() == 0, "Writer-only feature must have minReaderVersion=0");
    }
  }

  // Important note: uses the default implementation of `equals` and `hashCode` methods.
  // We expect that the feature instances are singletons, so we don't need to compare the fields.
}
