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

import java.util.Arrays;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.core.TypedPropertyPath;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.util.Assert;

import com.couchbase.client.java.search.HighlightStyle;
import com.couchbase.client.java.search.SearchMetaData;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchRequest;
import com.couchbase.client.java.search.SearchScanConsistency;
import com.couchbase.client.java.search.facet.SearchFacet;
import com.couchbase.client.java.search.result.ReactiveSearchResult;
import com.couchbase.client.java.search.result.SearchFacetResult;
import com.couchbase.client.java.search.result.SearchRow;
import com.couchbase.client.java.search.sort.SearchSort;

/**
 * {@link ReactiveFindBySearchOperation} implementations for Couchbase.
 *
 * @author Emilien Bevierre
 * @since 6.2
 */
public class ReactiveFindBySearchOperationSupport implements ReactiveFindBySearchOperation {

	private final ReactiveCouchbaseTemplate template;
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveFindBySearchOperationSupport.class);

	/**
	 * Limit applied when neither {@code withLimit(...)} nor {@code withOptions(...)} is used. Without an explicit size
	 * the FTS service returns only its default of 10 hits, which would silently truncate {@code all()} results.
	 */
	static final int DEFAULT_LIMIT = 10_000;

	public ReactiveFindBySearchOperationSupport(final ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveFindBySearch<T> findBySearch(final Class<T> domainType) {
		return new ReactiveFindBySearchSupport<>(template, domainType, domainType, null, null, null,
				OptionsBuilder.getScopeFrom(domainType), OptionsBuilder.getCollectionFrom(domainType), null,
				null, null, null, null, null, null, template.support());
	}

	static class ReactiveFindBySearchSupport<T> implements ReactiveFindBySearch<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<?> domainType;
		private final Class<T> returnType;
		private final String indexName;
		private final SearchRequest searchRequest;
		private final SearchScanConsistency scanConsistency;
		private final String scope;
		private final String collection;
		private final SearchOptions options;
		private final SearchSort[] sort;
		private final HighlightStyle highlightStyle;
		private final String[] highlightFields;
		private final Map<String, SearchFacet> facets;
		private final String[] fields;
		private final Integer[] limitSkip; // [0]=limit, [1]=skip; null means unset
		private final ReactiveTemplateSupport support;

		ReactiveFindBySearchSupport(final ReactiveCouchbaseTemplate template, final Class<?> domainType,
				final Class<T> returnType, final String indexName, final SearchRequest searchRequest,
				final SearchScanConsistency scanConsistency, final String scope, final String collection,
				final SearchOptions options, final SearchSort[] sort, final HighlightStyle highlightStyle,
				final String[] highlightFields, final Map<String, SearchFacet> facets, final String[] fields,
				final Integer[] limitSkip, final ReactiveTemplateSupport support) {
			this.template = template;
			this.domainType = domainType;
			this.returnType = returnType;
			this.indexName = indexName;
			this.searchRequest = searchRequest;
			this.scanConsistency = scanConsistency;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.sort = sort;
			this.highlightStyle = highlightStyle;
			this.highlightFields = highlightFields;
			this.facets = facets;
			this.fields = fields;
			this.limitSkip = limitSkip;
			this.support = support;
		}

		@Override
		public TerminatingFindBySearch<T> matching(SearchRequest searchRequest) {
			Assert.notNull(searchRequest, "SearchRequest must not be null");
			return new ReactiveFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, highlightStyle, highlightFields, facets, fields,
					limitSkip, support);
		}

		@Override
		public FindBySearchWithProjection<T> withIndex(String indexName) {
			Assert.notNull(indexName, "Index name must not be null!");
			return new ReactiveFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, highlightStyle, highlightFields, facets, fields,
					limitSkip, support);
		}

		@Override
		public <R> FindBySearchWithFields<R> as(final Class<R> returnType) {
			Assert.notNull(returnType, "returnType must not be null");
			return new ReactiveFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, highlightStyle, highlightFields, facets,
					fields, limitSkip, support);
		}

		@Override
		public FindBySearchInScope<T> withConsistency(SearchScanConsistency scanConsistency) {
			return new ReactiveFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, highlightStyle, highlightFields, facets, fields,
					limitSkip, support);
		}

		@Override
		public FindBySearchInCollection<T> inScope(final String scope) {
			return new ReactiveFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope != null ? scope : this.scope, collection, options, sort, highlightStyle,
					highlightFields, facets, fields, limitSkip, support);
		}

		@Override
		public FindBySearchWithOptions<T> inCollection(final String collection) {
			return new ReactiveFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection != null ? collection : this.collection, options, sort,
					highlightStyle, highlightFields, facets, fields, limitSkip, support);
		}

		@Override
		public FindBySearchWithQuery<T> withOptions(final SearchOptions options) {
			return new ReactiveFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options != null ? options : this.options, sort, highlightStyle,
					highlightFields, facets, fields, limitSkip, support);
		}

		@Override
		public FindBySearchWithConsistency<T> withLimit(int limit) {
			Integer[] ls = limitSkip != null ? Arrays.copyOf(limitSkip, 2) : new Integer[2];
			ls[0] = limit;
			return new ReactiveFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, highlightStyle, highlightFields, facets,
					fields, ls, support);
		}

		@Override
		public FindBySearchWithLimit<T> withSkip(int skip) {
			Integer[] ls = limitSkip != null ? Arrays.copyOf(limitSkip, 2) : new Integer[2];
			ls[1] = skip;
			return new ReactiveFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, highlightStyle, highlightFields, facets,
					fields, ls, support);
		}

		@Override
		public FindBySearchWithSkip<T> withSort(SearchSort... sort) {
			return new ReactiveFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, highlightStyle, highlightFields, facets,
					fields, limitSkip, support);
		}

		@Override
		public <P> FindBySearchWithSkip<T> withSort(TypedPropertyPath<P, ?> property,
				TypedPropertyPath<P, ?>... additionalProperties) {
			return withSort(PropertyPathSupport.toSearchSorts(template.getConverter(), property, additionalProperties));
		}

		@Override
		public FindBySearchWithSort<T> withHighlight(HighlightStyle style, String... fields) {
			return new ReactiveFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, style, fields, facets, this.fields, limitSkip,
					support);
		}

		@Override
		public <P> FindBySearchWithSort<T> withHighlight(HighlightStyle style, TypedPropertyPath<P, ?> field,
				TypedPropertyPath<P, ?>... additionalFields) {
			return withHighlight(style,
					PropertyPathSupport.getMappedFieldPaths(template.getConverter(), field, additionalFields));
		}

		@Override
		public FindBySearchWithHighlight<T> withFacets(Map<String, SearchFacet> facets) {
			return new ReactiveFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, highlightStyle, highlightFields, facets,
					fields, limitSkip, support);
		}

		@Override
		public FindBySearchWithFacets<T> withFields(String... fields) {
			return new ReactiveFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, highlightStyle, highlightFields, facets,
					fields, limitSkip, support);
		}

		@Override
		public <P> FindBySearchWithFacets<T> withFields(TypedPropertyPath<P, ?> field,
				TypedPropertyPath<P, ?>... additionalFields) {
			return withFields(PropertyPathSupport.getMappedFieldPaths(template.getConverter(), field, additionalFields));
		}

		@Override
		public Mono<T> one() {
			return all().singleOrEmpty();
		}

		@Override
		public Mono<T> first() {
			return all().next();
		}

		@Override
		public Flux<T> all() {
			return Flux.defer(() -> {
				Assert.notNull(indexName, "Index name must be specified via withIndex()");
				Assert.notNull(searchRequest, "SearchRequest must be specified via matching()");

				if (LOG.isDebugEnabled()) {
					LOG.debug("findBySearch index: {}", indexName);
				}

				return TransactionalSupport.verifyNotInTransaction("findBySearch")
						.thenMany(executeSearch()
								.flatMapMany(ReactiveSearchResult::rows))
						.flatMapSequential(this::hydrateRow)
						.onErrorMap(throwable -> {
							if (throwable instanceof RuntimeException) {
								return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
							} else {
								return throwable;
							}
						});
			});
		}

		@Override
		public Mono<Long> count() {
			return Mono.defer(() -> {
				Assert.notNull(indexName, "Index name must be specified via withIndex()");
				Assert.notNull(searchRequest, "SearchRequest must be specified via matching()");

				// rows must be drained before the SDK emits the metadata trailer, even with limit 0
				return TransactionalSupport.verifyNotInTransaction("findBySearch")
						.then(executeSearch(true))
						.flatMap(result -> result.rows().then(result.metaData()))
						.map(metaData -> metaData.metrics().totalRows())
						.onErrorMap(throwable -> {
							if (throwable instanceof RuntimeException) {
								return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
							} else {
								return throwable;
							}
						});
			});
		}

		@Override
		public Mono<Boolean> exists() {
			return count().map(count -> count > 0);
		}

		@Override
		public Flux<SearchRow> rows() {
			return Flux.defer(() -> {
				Assert.notNull(indexName, "Index name must be specified via withIndex()");
				Assert.notNull(searchRequest, "SearchRequest must be specified via matching()");

				return TransactionalSupport.verifyNotInTransaction("findBySearch")
						.thenMany(executeSearch()
								.flatMapMany(ReactiveSearchResult::rows))
						.onErrorMap(throwable -> {
							if (throwable instanceof RuntimeException) {
								return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
							} else {
								return throwable;
							}
						});
			});
		}

		@Override
		public Mono<SearchResult<T>> result() {
			return Mono.defer(() -> {
				Assert.notNull(indexName, "Index name must be specified via withIndex()");
				Assert.notNull(searchRequest, "SearchRequest must be specified via matching()");

				return TransactionalSupport.verifyNotInTransaction("findBySearch")
						.then(executeSearch())
						.flatMap(this::collectFullResult)
						.onErrorMap(throwable -> {
							if (throwable instanceof RuntimeException) {
								return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
							} else {
								return throwable;
							}
						});
			});
		}

		private Mono<SearchResult<T>> collectFullResult(ReactiveSearchResult reactiveResult) {
			Mono<java.util.List<SearchRow>> rowsMono = reactiveResult.rows().collectList();
			Mono<SearchMetaData> metaMono = reactiveResult.metaData();
			Mono<java.util.Map<String, SearchFacetResult>> facetsMono = reactiveResult.facets();

			return Mono.zip(rowsMono, metaMono, facetsMono)
					.flatMap(tuple -> {
						java.util.List<SearchRow> searchRows = tuple.getT1();
						SearchMetaData metaData = tuple.getT2();
						java.util.Map<String, SearchFacetResult> facetResults = tuple.getT3();

						return Flux.fromIterable(searchRows)
								.flatMapSequential(this::hydrateRow)
								.collectList()
								.map(entities -> new SearchResult<>(entities, searchRows, metaData, facetResults));
					});
		}

		/**
		 * Hydrates a SearchRow into an entity via KV GET, skipping documents that have been
		 * deleted between the FTS index update and the KV fetch (stale index entries).
		 * <p>
		 * Misses are logged at WARN so operators can detect index staleness; persistent or high-volume
		 * misses typically indicate an out-of-sync FTS index.
		 */
		private Mono<T> hydrateRow(SearchRow row) {
			// findById already maps DocumentNotFoundException to an empty Mono, so misses surface as emptiness
			return template.findById(returnType)
					.inScope(scope)
					.inCollection(collection)
					.one(row.id())
					.switchIfEmpty(Mono.defer(() -> {
						LOG.warn("Skipping stale FTS result for document id '{}': document not found in KV "
								+ "(index '{}' may be out of sync)", row.id(), indexName);
						return Mono.empty();
					}));
		}
		private Mono<ReactiveSearchResult> executeSearch() {
			return executeSearch(false);
		}

		private Mono<ReactiveSearchResult> executeSearch(boolean metadataOnly) {
			SearchOptions opts = buildSearchOptions(metadataOnly);
			if (scope != null) {
				return template.getCouchbaseClientFactory()
						.withScope(scope).getScope().reactive()
						.search(indexName, searchRequest, opts);
			} else {
				return template.getCouchbaseClientFactory()
						.getCluster().reactive()
						.search(indexName, searchRequest, opts);
			}
		}

		private SearchOptions buildSearchOptions(boolean metadataOnly) {
			if (options != null) {
				if (hasFluentOptions()) {
					throw new IllegalArgumentException("withOptions() cannot be combined with withConsistency(), withSort(), "
							+ "withHighlight(), withFacets(), withFields(), withLimit() or withSkip(); "
							+ "set those directly on the SearchOptions instead");
				}
				if (collection != null) {
					options.collections(collection);
				}
				return options;
			}
			SearchOptions opts = SearchOptions.searchOptions();
			if (scanConsistency != null) {
				opts.scanConsistency(scanConsistency);
			}
			if (collection != null) {
				opts.collections(collection);
			}
			if (sort != null && sort.length > 0) {
				opts.sort((Object[]) sort);
			}
			if (highlightStyle != null) {
				if (highlightFields != null && highlightFields.length > 0) {
					opts.highlight(highlightStyle, highlightFields);
				} else {
					opts.highlight(highlightStyle);
				}
			}
			if (facets != null && !facets.isEmpty()) {
				opts.facets(facets);
			}
			if (fields != null && fields.length > 0) {
				opts.fields(fields);
			}
			if (metadataOnly) {
				opts.limit(0);
			} else {
				if (limitSkip != null && limitSkip[0] != null) {
					opts.limit(limitSkip[0]);
				} else {
					opts.limit(DEFAULT_LIMIT);
				}
				if (limitSkip != null && limitSkip[1] != null) {
					opts.skip(limitSkip[1]);
				}
			}
			return opts;
		}

		private boolean hasFluentOptions() {
			return scanConsistency != null || (sort != null && sort.length > 0) || highlightStyle != null
					|| (facets != null && !facets.isEmpty()) || (fields != null && fields.length > 0) || limitSkip != null;
		}
	}
}
