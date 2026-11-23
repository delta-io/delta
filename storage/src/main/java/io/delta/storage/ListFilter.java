/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.storage;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * :: DeveloperApi ::
 *
 * Decides which entries a {@link LogStore#listFrom(org.apache.hadoop.fs.Path, boolean, ListFilter,
 * org.apache.hadoop.conf.Configuration)} call leaves out. Both methods receive the entry's own
 * name (the last path component), at every depth of the listing.
 *
 * <p>Skipping a directory skips the directory entry and everything under it. Implementations
 * that list object stores use this to jump past the whole subtree with a single request instead
 * of reading it.</p>
 *
 * @since 4.5.0
 */
public interface ListFilter extends Serializable {

    /** Returns true to leave the file named {@code name} out of the listing. */
    boolean skipFile(String name);

    /** Returns true to leave the directory named {@code name}, and its whole subtree, out. */
    boolean skipDirectory(String name);

    /** Keeps every entry. */
    ListFilter NONE = new ListFilter() {
        @Override
        public boolean skipFile(String name) { return false; }

        @Override
        public boolean skipDirectory(String name) { return false; }

        @Override
        public String toString() { return "ListFilter.NONE"; }
    };

    /**
     * Builds a filter from name prefixes.
     *
     * @param ignoreFilePrefixes files whose name starts with one of these are skipped
     * @param ignoreDirPrefixes directories whose name starts with one of these are skipped
     * @param keepDirPrefixes directories whose name starts with one of these are kept even when
     *                        they also match {@code ignoreDirPrefixes} (for example
     *                        {@code _change_data} or a partition directory {@code _col=})
     */
    static ListFilter prefixes(
            Set<String> ignoreFilePrefixes,
            Set<String> ignoreDirPrefixes,
            Set<String> keepDirPrefixes) {
        return new PrefixListFilter(ignoreFilePrefixes, ignoreDirPrefixes, keepDirPrefixes);
    }

    /** Prefix-based {@link ListFilter}. */
    final class PrefixListFilter implements ListFilter {
        private final Set<String> ignoreFilePrefixes;
        private final Set<String> ignoreDirPrefixes;
        private final Set<String> keepDirPrefixes;

        PrefixListFilter(
                Set<String> ignoreFilePrefixes,
                Set<String> ignoreDirPrefixes,
                Set<String> keepDirPrefixes) {
            this.ignoreFilePrefixes = copy(ignoreFilePrefixes);
            this.ignoreDirPrefixes = copy(ignoreDirPrefixes);
            this.keepDirPrefixes = copy(keepDirPrefixes);
        }

        private static Set<String> copy(Set<String> prefixes) {
            return prefixes == null
                ? Collections.emptySet()
                : Collections.unmodifiableSet(new LinkedHashSet<>(prefixes));
        }

        private static boolean startsWithAny(String name, Set<String> prefixes) {
            for (String prefix : prefixes) {
                if (name.startsWith(prefix)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean skipFile(String name) {
            return startsWithAny(name, ignoreFilePrefixes);
        }

        @Override
        public boolean skipDirectory(String name) {
            return startsWithAny(name, ignoreDirPrefixes) && !startsWithAny(name, keepDirPrefixes);
        }

        @Override
        public String toString() {
            return "ListFilter.prefixes(ignoreFiles=" + ignoreFilePrefixes
                + ", ignoreDirs=" + ignoreDirPrefixes + ", keepDirs=" + keepDirPrefixes + ")";
        }
    }
}
