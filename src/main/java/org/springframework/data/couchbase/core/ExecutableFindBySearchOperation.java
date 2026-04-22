/*
 * Copyright 2025-present the original author or authors
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
package org.springframework.data.couchbase.core;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.core.TypedPropertyPath;
import org.springframework.data.couchbase.core.support.InCollection;
import org.springframework.data.couchbase.core.support.InScope;
import org.springframework.data.couchbase.core.support.WithSearchConsistency;
import org.springframework.data.couchbase.core.support.WithSearchOptions;
import org.springframework.data.couchbase.core.support.WithSearchQuery;
import org.jspecify.annotations.Nullable;

import com.couchbase.client.java.search.HighlightStyle;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchRequest;
import com.couchbase.client.java.search.SearchScanConsistency;
import com.couchbase.client.java.search.facet.SearchFacet;
import com.couchbase.client.java.search.result.SearchRow;
import com.couchbase.client.java.search.sort.SearchSort;

/**
 * Full-Text Search (FTS) Operations (Blocking)
 *
 * @author Emilien Bevierre
 * @since 6.2
 */
public interface ExecutableFindBySearchOperation {

	/**
	 * Queries the Full-Text Search (FTS) service.
	 *
	 * @param domainType the entity type to use for the results.
	 */
	<T> ExecutableFindBySearch<T> findBySearch(Class<T> domainType);

	interface TerminatingFindBySearch<T> {

		/**
		 * Get exactly zero or one result.
		 *
		 * @return {@link Optional#empty()} if no match found.
		 * @throws IncorrectResultSizeDataAccessException if more than one match found.
		 */
		default Optional<T> one() {
			return Optional.ofNullable(oneValue());
		}

		/**
		 * Get exactly zero or one result.
		 *
		 * @return {@literal null} if no match found.
		 * @throws IncorrectResultSizeDataAccessException if more than one match found.
		 */
		@Nullable
		T oneValue();

		/**
		 * Get the first or no result.
		 *
		 * @return {@link Optional#empty()} if no match found.
		 */
		default Optional<T> first() {
			return Optional.ofNullable(firstValue());
		}

		/**
		 * Get the first or no result.
		 *
		 * @return {@literal null} if no match found.
		 */
		@Nullable
		T firstValue();

		/**
		 * Get all matching elements, hydrated as entities via KV GET.
		 *
		 * @return never {@literal null}.
		 */
		List<T> all();

		/**
		 * Stream all matching elements.
		 *
		 * @return a {@link Stream} of results. Never {@literal null}.
		 */
		Stream<T> stream();

		/**
		 * Get the number of matching elements.
		 *
		 * @return total number of matching elements.
		 */
		long count();

		/**
		 * Check for the presence of matching elements.
		 *
		 * @return {@literal true} if at least one matching element exists.
		 */
		boolean exists();

		/**
		 * Get raw FTS search rows (without entity hydration).
		 *
		 * @return never {@literal null}.
		 */
		List<SearchRow> rows();

		/**
		 * Get a combined result including hydrated entities, raw rows, metadata, and facet results.
		 *
		 * @return a {@link SearchResult} containing the full response.
		 */
		SearchResult<T> result();
	}

	interface FindBySearchWithQuery<T> extends TerminatingFindBySearch<T>, WithSearchQuery<T> {
		@Override
		TerminatingFindBySearch<T> matching(SearchRequest searchRequest);
	}

	interface FindBySearchWithOptions<T> extends FindBySearchWithQuery<T>, WithSearchOptions<T> {
		@Override
		FindBySearchWithQuery<T> withOptions(SearchOptions options);
	}

	interface FindBySearchInCollection<T> extends FindBySearchWithOptions<T>, InCollection<T> {
		@Override
		FindBySearchWithOptions<T> inCollection(String collection);
	}

	interface FindBySearchInScope<T> extends FindBySearchInCollection<T>, InScope<T> {
		@Override
		FindBySearchInCollection<T> inScope(String scope);
	}

	interface FindBySearchWithConsistency<T> extends FindBySearchInScope<T>, WithSearchConsistency<T> {
		@Override
		FindBySearchInScope<T> withConsistency(SearchScanConsistency scanConsistency);
	}

	interface FindBySearchWithLimit<T> extends FindBySearchWithConsistency<T> {
		FindBySearchWithConsistency<T> withLimit(int limit);
	}

	interface FindBySearchWithSkip<T> extends FindBySearchWithLimit<T> {
		FindBySearchWithLimit<T> withSkip(int skip);
	}

	interface FindBySearchWithSort<T> extends FindBySearchWithSkip<T> {
		FindBySearchWithSkip<T> withSort(SearchSort... sort);

		<P> FindBySearchWithSkip<T> withSort(TypedPropertyPath<P, ?> property,
				TypedPropertyPath<P, ?>... additionalProperties);
	}

	interface FindBySearchWithHighlight<T> extends FindBySearchWithSort<T> {
		FindBySearchWithSort<T> withHighlight(HighlightStyle style, String... fields);

		default FindBySearchWithSort<T> withHighlight(String... fields) {
			return withHighlight(HighlightStyle.SERVER_DEFAULT, fields);
		}

		<P> FindBySearchWithSort<T> withHighlight(HighlightStyle style, TypedPropertyPath<P, ?> field,
				TypedPropertyPath<P, ?>... additionalFields);

		default <P> FindBySearchWithSort<T> withHighlight(TypedPropertyPath<P, ?> field,
				TypedPropertyPath<P, ?>... additionalFields) {
			return withHighlight(HighlightStyle.SERVER_DEFAULT, field, additionalFields);
		}
	}

	interface FindBySearchWithFacets<T> extends FindBySearchWithHighlight<T> {
		FindBySearchWithHighlight<T> withFacets(Map<String, SearchFacet> facets);
	}

	interface FindBySearchWithFields<T> extends FindBySearchWithFacets<T> {
		FindBySearchWithFacets<T> withFields(String... fields);

		<P> FindBySearchWithFacets<T> withFields(TypedPropertyPath<P, ?> field,
				TypedPropertyPath<P, ?>... additionalFields);
	}

	interface FindBySearchWithProjection<T> extends FindBySearchWithFields<T> {
		<R> FindBySearchWithFields<R> as(Class<R> returnType);
	}

	interface FindBySearchWithIndex<T> extends FindBySearchWithProjection<T> {
		FindBySearchWithProjection<T> withIndex(String indexName);
	}

	interface ExecutableFindBySearch<T> extends FindBySearchWithIndex<T> {}
}
