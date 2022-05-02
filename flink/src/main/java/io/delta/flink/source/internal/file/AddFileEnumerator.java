package io.delta.flink.source.internal.file;

import java.io.Serializable;
import java.util.List;
import java.util.function.Predicate;

import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.core.fs.Path;

import io.delta.standalone.actions.AddFile;

/**
 * The {@code AddFileEnumerator}'s task is to convert all discovered {@link AddFile} to set of
 * {@link DeltaSourceSplit}.
 */
public interface AddFileEnumerator<SplitT extends DeltaSourceSplit> {

    /**
     * Creates {@link DeltaSourceSplit} for the given {@link AddFile}. The {@code splitFilter}
     * decides which AddFiles should be excluded from conversion.
     *
     * @param context     {@link AddFileEnumeratorContext} input object for Split conversion.
     * @param splitFilter {@link SplitFilter} instance that will be used to filter out {@link
     *                    AddFile} from split conversion.
     * @return List of Splits.
     */
    List<SplitT> enumerateSplits(AddFileEnumeratorContext context, SplitFilter<Path> splitFilter);

    // ------------------------------------------------------------------------

    /**
     * Factory for the {@code AddFileEnumerator}
     */
    @FunctionalInterface
    interface Provider<SplitT extends DeltaSourceSplit> extends Serializable {

        AddFileEnumerator<SplitT> create();
    }

    /**
     * A functional interface that can be used by {@code AddFileEnumerator} to exclude {@link
     * AddFile} from conversion to {@link DeltaSourceSplit}.
     *
     * @param <T> - Parametrized {@code SplitFilter} instance.
     */
    @FunctionalInterface
    interface SplitFilter<T> extends Predicate<T>, Serializable {

    }
}
