/*
 * Copyright 2021-present the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.core.support;

import java.util.Arrays;

import org.springframework.data.core.TypedPropertyPath;

/**
 * A common interface for all of Insert, Replace, Upsert and Remove mutations that take options.
 *
 * @author Tigran Babloyan
 * @author Emilien Bevierre
 * @param <T> - the entity class
 */
public interface WithMutateInPaths<T> {
	Object withRemovePaths(final String... removePaths);

	Object withInsertPaths(final String... insertPaths);

	Object withReplacePaths(final String... replacePaths);

	Object withUpsertPaths(final String... upsertPaths);

	/**
	 * Type-safe variant of {@link #withRemovePaths(String...)} using property paths.
	 *
	 * @since 6.1
	 */
	default Object withRemovePaths(TypedPropertyPath<?, ?>... removePaths) {
		return withRemovePaths(Arrays.stream(removePaths).map(TypedPropertyPath::toDotPath).toArray(String[]::new));
	}

	/**
	 * Type-safe variant of {@link #withInsertPaths(String...)} using property paths.
	 *
	 * @since 6.1
	 */
	default Object withInsertPaths(TypedPropertyPath<?, ?>... insertPaths) {
		return withInsertPaths(Arrays.stream(insertPaths).map(TypedPropertyPath::toDotPath).toArray(String[]::new));
	}

	/**
	 * Type-safe variant of {@link #withReplacePaths(String...)} using property paths.
	 *
	 * @since 6.1
	 */
	default Object withReplacePaths(TypedPropertyPath<?, ?>... replacePaths) {
		return withReplacePaths(Arrays.stream(replacePaths).map(TypedPropertyPath::toDotPath).toArray(String[]::new));
	}

	/**
	 * Type-safe variant of {@link #withUpsertPaths(String...)} using property paths.
	 *
	 * @since 6.1
	 */
	default Object withUpsertPaths(TypedPropertyPath<?, ?>... upsertPaths) {
		return withUpsertPaths(Arrays.stream(upsertPaths).map(TypedPropertyPath::toDotPath).toArray(String[]::new));
	}
}
