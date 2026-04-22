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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.couchbase.core.support.InCollection;
import org.springframework.data.couchbase.core.support.InScope;
import org.springframework.data.couchbase.core.support.WithSearchConsistency;
import org.springframework.data.couchbase.core.support.WithSearchOptions;
import org.springframework.data.couchbase.core.support.WithSearchQuery;

import com.couchbase.client.java.search.HighlightStyle;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchRequest;
import com.couchbase.client.java.search.SearchScanConsistency;
import com.couchbase.client.java.search.facet.SearchFacet;
import com.couchbase.client.java.search.result.SearchRow;
import com.couchbase.client.java.search.sort.SearchSort;

/**
 * Full-Text Search (FTS) Operations (Reactive)
 *
 * @author Emilien Bevierre
 * @since 6.2
 */
public interface ReactiveFindBySearchOperation {

	/**
	 * Queries the Full-Text Search (FTS) service.
	 *
	 * @param domainType the entity type to use for the results.
	 */
	<T> ReactiveFindBySearch<T> findBySearch(Class<T> domainType);

	/**
	 * Terminating operations invoking the actual execution.
	 */
	interface TerminatingFindBySearch<T> {

		/**
		 * Get exactly zero or one result.
		 *
		 * @return a mono with the match if found (an empty one otherwise).
		 * @throws IncorrectResultSizeDataAccessException if more than one match found.
		 */
		Mono<T> one();

		/**
		 * Get the first or no result.
		 *
		 * @return the first or an empty mono if none found.
		 */
		Mono<T> first();

		/**
		 * Get all matching elements, hydrated as entities via KV GET.
		 *
		 * @return never {@literal null}.
		 */
		Flux<T> all();

		/**
		 * Get the number of matching elements.
		 *
		 * @return total number of matching elements.
		 */
		Mono<Long> count();

		/**
		 * Check for the presence of matching elements.
		 *
		 * @return {@literal true} if at least one matching element exists.
		 */
		Mono<Boolean> exists();

		/**
		 * Get raw FTS search rows (without entity hydration). Useful for accessing scores, fragments, locations, etc.
		 *
		 * @return a {@link Flux} of {@link SearchRow} results.
		 */
		Flux<SearchRow> rows();

		/**
		 * Get a combined result including hydrated entities, raw rows, metadata, and facet results.
		 *
		 * @return a {@link Mono} of {@link SearchResult} containing the full response.
		 */
		Mono<SearchResult<T>> result();
	}

	interface FindBySearchWithQuery<T> extends TerminatingFindBySearch<T>, WithSearchQuery<T> {

		/**
		 * Set the search request to be used.
		 *
		 * @param searchRequest must not be {@literal null}.
		 * @throws IllegalArgumentException if searchRequest is {@literal null}.
		 */
		@Override
		TerminatingFindBySearch<T> matching(SearchRequest searchRequest);
	}

	/**
	 * Fluent method to specify options.
	 *
	 * @param <T> the entity type to use.
	 */
	interface FindBySearchWithOptions<T> extends FindBySearchWithQuery<T>, WithSearchOptions<T> {
		@Override
		FindBySearchWithQuery<T> withOptions(SearchOptions options);
	}

	/**
	 * Fluent method to specify the collection.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindBySearchInCollection<T> extends FindBySearchWithOptions<T>, InCollection<T> {
		@Override
		FindBySearchWithOptions<T> inCollection(String collection);
	}

	/**
	 * Fluent method to specify the scope.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindBySearchInScope<T> extends FindBySearchInCollection<T>, InScope<T> {
		@Override
		FindBySearchInCollection<T> inScope(String scope);
	}

	/**
	 * Fluent method to specify scan consistency.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindBySearchWithConsistency<T> extends FindBySearchInScope<T>, WithSearchConsistency<T> {
		@Override
		FindBySearchInScope<T> withConsistency(SearchScanConsistency scanConsistency);
	}

	/**
	 * Fluent method to specify a limit on results.
	 */
	interface FindBySearchWithLimit<T> extends FindBySearchWithConsistency<T> {
		/**
		 * Limit the number of results returned.
		 *
		 * @param limit the maximum number of results.
		 */
		FindBySearchWithConsistency<T> withLimit(int limit);
	}

	/**
	 * Fluent method to specify a skip/offset.
	 */
	interface FindBySearchWithSkip<T> extends FindBySearchWithLimit<T> {
		/**
		 * Skip the given number of results (for pagination).
		 *
		 * @param skip the number of results to skip.
		 */
		FindBySearchWithLimit<T> withSkip(int skip);
	}

	/**
	 * Fluent method to specify sorting.
	 */
	interface FindBySearchWithSort<T> extends FindBySearchWithSkip<T> {
		/**
		 * Specify the sort order for results.
		 *
		 * @param sort the sort specifications.
		 */
		FindBySearchWithSkip<T> withSort(SearchSort... sort);
	}

	/**
	 * Fluent method to specify highlighting.
	 */
	interface FindBySearchWithHighlight<T> extends FindBySearchWithSort<T> {
		/**
		 * Enable highlighting on results with the given style and optional field restrictions.
		 *
		 * @param style  the highlight style.
		 * @param fields optional fields to highlight. If empty, all matched fields are highlighted.
		 */
		FindBySearchWithSort<T> withHighlight(HighlightStyle style, String... fields);

		/**
		 * Enable highlighting with the server's default style.
		 *
		 * @param fields optional fields to highlight.
		 */
		default FindBySearchWithSort<T> withHighlight(String... fields) {
			return withHighlight(HighlightStyle.SERVER_DEFAULT, fields);
		}
	}

	/**
	 * Fluent method to specify facets.
	 */
	interface FindBySearchWithFacets<T> extends FindBySearchWithHighlight<T> {
		/**
		 * Specify facets to include in the search results.
		 *
		 * @param facets a map of facet name to facet definition.
		 */
		FindBySearchWithHighlight<T> withFacets(Map<String, SearchFacet> facets);
	}

	/**
	 * Fluent method to specify which stored fields to return.
	 */
	interface FindBySearchWithFields<T> extends FindBySearchWithFacets<T> {
		/**
		 * Specify which stored fields to include in the search results.
		 *
		 * @param fields the field names.
		 */
		FindBySearchWithFacets<T> withFields(String... fields);
	}

	/**
	 * Result type override (Optional).
	 */
	interface FindBySearchWithProjection<T> extends FindBySearchWithFields<T> {

		/**
		 * Define the target type fields should be mapped to.
		 *
		 * @param returnType must not be {@literal null}.
		 * @return new instance of {@link FindBySearchWithFields}.
		 * @throws IllegalArgumentException if returnType is {@literal null}.
		 */
		<R> FindBySearchWithFields<R> as(Class<R> returnType);
	}

	/**
	 * Fluent method to specify the FTS index name.
	 */
	interface FindBySearchWithIndex<T> extends FindBySearchWithProjection<T> {
		/**
		 * Specify the FTS index name to query.
		 *
		 * @param indexName the name of the FTS index.
		 * @return new instance for further fluent configuration.
		 */
		FindBySearchWithProjection<T> withIndex(String indexName);
	}

	interface ReactiveFindBySearch<T> extends FindBySearchWithIndex<T> {}
}
