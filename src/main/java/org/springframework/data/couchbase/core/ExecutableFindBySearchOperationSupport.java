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
import java.util.stream.Stream;

import org.springframework.data.core.TypedPropertyPath;
import org.springframework.data.couchbase.core.ReactiveFindBySearchOperationSupport.ReactiveFindBySearchSupport;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.util.Assert;

import com.couchbase.client.java.search.HighlightStyle;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchRequest;
import com.couchbase.client.java.search.SearchScanConsistency;
import com.couchbase.client.java.search.facet.SearchFacet;
import com.couchbase.client.java.search.result.SearchRow;
import com.couchbase.client.java.search.sort.SearchSort;

/**
 * {@link ExecutableFindBySearchOperation} implementations for Couchbase.
 *
 * @author Emilien Bevierre
 * @since 6.2
 */
public class ExecutableFindBySearchOperationSupport implements ExecutableFindBySearchOperation {

	private final CouchbaseTemplate template;

	public ExecutableFindBySearchOperationSupport(final CouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ExecutableFindBySearch<T> findBySearch(final Class<T> domainType) {
		return new ExecutableFindBySearchSupport<>(template, domainType, domainType, null, null, null,
				OptionsBuilder.getScopeFrom(domainType), OptionsBuilder.getCollectionFrom(domainType), null,
				null, null, null, null, null, null);
	}

	static class ExecutableFindBySearchSupport<T> implements ExecutableFindBySearch<T> {

		private final CouchbaseTemplate template;
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
		private final Integer[] limitSkip;
		private final ReactiveFindBySearchSupport<T> reactiveSupport;

		ExecutableFindBySearchSupport(final CouchbaseTemplate template, final Class<?> domainType,
				final Class<T> returnType, final String indexName, final SearchRequest searchRequest,
				final SearchScanConsistency scanConsistency, final String scope, final String collection,
				final SearchOptions options, final SearchSort[] sort, final HighlightStyle highlightStyle,
				final String[] highlightFields, final Map<String, SearchFacet> facets, final String[] fields,
				final Integer[] limitSkip) {
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
			this.reactiveSupport = new ReactiveFindBySearchSupport<>(template.reactive(), domainType, returnType,
					indexName, searchRequest, scanConsistency, scope, collection, options, sort, highlightStyle,
					highlightFields, facets, fields, limitSkip,
					new NonReactiveSupportWrapper(template.support()));
		}

		@Override
		public T oneValue() {
			return reactiveSupport.one().block();
		}

		@Override
		public T firstValue() {
			return reactiveSupport.first().block();
		}

		@Override
		public List<T> all() {
			return reactiveSupport.all().collectList().block();
		}

		@Override
		public Stream<T> stream() {
			return reactiveSupport.all().toStream();
		}

		@Override
		public long count() {
			return reactiveSupport.count().block();
		}

		@Override
		public boolean exists() {
			return count() > 0;
		}

		@Override
		public List<SearchRow> rows() {
			return reactiveSupport.rows().collectList().block();
		}

		@Override
		public SearchResult<T> result() {
			return reactiveSupport.result().block();
		}

		@Override
		public TerminatingFindBySearch<T> matching(SearchRequest searchRequest) {
			Assert.notNull(searchRequest, "SearchRequest must not be null!");
			return new ExecutableFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, highlightStyle, highlightFields, facets,
					fields, limitSkip);
		}

		@Override
		public FindBySearchWithProjection<T> withIndex(String indexName) {
			Assert.notNull(indexName, "Index name must not be null!");
			return new ExecutableFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, highlightStyle, highlightFields, facets,
					fields, limitSkip);
		}

		@Override
		public <R> FindBySearchWithFields<R> as(final Class<R> returnType) {
			Assert.notNull(returnType, "returnType must not be null!");
			return new ExecutableFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, highlightStyle, highlightFields, facets,
					fields, limitSkip);
		}

		@Override
		public FindBySearchInScope<T> withConsistency(SearchScanConsistency scanConsistency) {
			return new ExecutableFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, highlightStyle, highlightFields, facets,
					fields, limitSkip);
		}

		@Override
		public FindBySearchInCollection<T> inScope(final String scope) {
			return new ExecutableFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope != null ? scope : this.scope, collection, options, sort, highlightStyle,
					highlightFields, facets, fields, limitSkip);
		}

		@Override
		public FindBySearchWithOptions<T> inCollection(final String collection) {
			return new ExecutableFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection != null ? collection : this.collection, options, sort,
					highlightStyle, highlightFields, facets, fields, limitSkip);
		}

		@Override
		public FindBySearchWithQuery<T> withOptions(final SearchOptions options) {
			return new ExecutableFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options != null ? options : this.options, sort, highlightStyle,
					highlightFields, facets, fields, limitSkip);
		}

		@Override
		public FindBySearchWithConsistency<T> withLimit(int limit) {
			Integer[] ls = limitSkip != null ? java.util.Arrays.copyOf(limitSkip, 2) : new Integer[2];
			ls[0] = limit;
			return new ExecutableFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, highlightStyle, highlightFields, facets,
					fields, ls);
		}

		@Override
		public FindBySearchWithLimit<T> withSkip(int skip) {
			Integer[] ls = limitSkip != null ? java.util.Arrays.copyOf(limitSkip, 2) : new Integer[2];
			ls[1] = skip;
			return new ExecutableFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, highlightStyle, highlightFields, facets,
					fields, ls);
		}

		@Override
		public FindBySearchWithSkip<T> withSort(SearchSort... sort) {
			return new ExecutableFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, highlightStyle, highlightFields, facets,
					fields, limitSkip);
		}

		@Override
		public <P> FindBySearchWithSkip<T> withSort(TypedPropertyPath<P, ?> property,
				TypedPropertyPath<P, ?>... additionalProperties) {
			return withSort(SearchPropertyPathSupport.toSearchSorts(template.getConverter(), property, additionalProperties));
		}

		@Override
		public FindBySearchWithSort<T> withHighlight(HighlightStyle style, String... fields) {
			return new ExecutableFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, style, fields, facets, this.fields, limitSkip);
		}

		@Override
		public <P> FindBySearchWithSort<T> withHighlight(HighlightStyle style, TypedPropertyPath<P, ?> field,
				TypedPropertyPath<P, ?>... additionalFields) {
			return withHighlight(style,
					SearchPropertyPathSupport.getMappedFieldPaths(template.getConverter(), field, additionalFields));
		}

		@Override
		public FindBySearchWithHighlight<T> withFacets(Map<String, SearchFacet> facets) {
			return new ExecutableFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, highlightStyle, highlightFields, facets,
					fields, limitSkip);
		}

		@Override
		public FindBySearchWithFacets<T> withFields(String... fields) {
			return new ExecutableFindBySearchSupport<>(template, domainType, returnType, indexName, searchRequest,
					scanConsistency, scope, collection, options, sort, highlightStyle, highlightFields, facets,
					fields, limitSkip);
		}

		@Override
		public <P> FindBySearchWithFacets<T> withFields(TypedPropertyPath<P, ?> field,
				TypedPropertyPath<P, ?>... additionalFields) {
			return withFields(SearchPropertyPathSupport.getMappedFieldPaths(template.getConverter(), field, additionalFields));
		}
	}
}
