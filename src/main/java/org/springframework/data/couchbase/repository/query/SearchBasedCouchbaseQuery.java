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
package org.springframework.data.couchbase.repository.query;

import java.util.List;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.ExecutableFindBySearchOperation;
import org.springframework.data.couchbase.repository.Search;
import org.springframework.data.couchbase.repository.SearchIndex;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.util.Assert;

import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.SearchRequest;
import com.couchbase.client.java.search.SearchScanConsistency;

/**
 * {@link RepositoryQuery} implementation for FTS {@link Search}-annotated methods.
 * <p>
 * Supports positional parameter substitution ({@code ?0}, {@code ?1}, etc.) in the query string.
 * Supports {@link Pageable} parameters for limit/skip pagination.
 * Supports {@link org.springframework.data.couchbase.repository.ScanConsistency} for FTS scan consistency.
 *
 * @author Emilien Bevierre
 * @since 6.2
 */
public class SearchBasedCouchbaseQuery implements RepositoryQuery {

	private final CouchbaseQueryMethod method;
	private final CouchbaseOperations operations;
	private final String searchQueryTemplate;
	private final String indexName;

	public SearchBasedCouchbaseQuery(CouchbaseQueryMethod method, CouchbaseOperations operations) {
		Assert.notNull(method, "CouchbaseQueryMethod must not be null!");
		Assert.notNull(operations, "CouchbaseOperations must not be null!");

		this.method = method;
		this.operations = operations;

		Search searchAnnotation = method.getAnnotation(Search.class);
		Assert.notNull(searchAnnotation, "Method must be annotated with @Search!");
		this.searchQueryTemplate = searchAnnotation.value();

		this.indexName = resolveIndexName(method);
		Assert.notNull(indexName, "FTS index name must be specified via @SearchIndex on the method or entity class!");
	}

	@Override
	public Object execute(Object[] parameters) {
		ParametersParameterAccessor accessor = new ParametersParameterAccessor(method.getParameters(), parameters);
		ResultProcessor processor = method.getResultProcessor().withDynamicProjection(accessor);
		SearchRepositoryQuerySupport.validateSort(accessor);

		String resolvedQuery = resolveParameters(searchQueryTemplate, accessor);
		SearchRequest request = SearchRequest.create(SearchQuery.queryString(resolvedQuery));
		Object result = executeDependingOnType(accessor, request);

		return processor.processResult(result);
	}

	@Override
	public CouchbaseQueryMethod getQueryMethod() {
		return method;
	}

	static String resolveParameters(String template, ParametersParameterAccessor accessor) {
		return SearchRepositoryQuerySupport.bindQueryString(template, accessor);
	}

	private Object executeDependingOnType(ParametersParameterAccessor accessor, SearchRequest request) {
		ExecutableFindBySearchOperation.FindBySearchWithQuery<?> queryOp = createQueryOperation(accessor, true);

		if (method.isCountQuery()) {
			return createQueryOperation(accessor, false).matching(request).count();
		} else if (method.isExistsQuery()) {
			return createQueryOperation(accessor, false).matching(request).exists();
		} else if (method.isPageQuery()) {
			return executePage(accessor, request);
		} else if (method.isSliceQuery()) {
			return executeSlice(accessor, request);
		} else if (method.isCollectionQuery()) {
			return queryOp.matching(request).all();
		} else if (method.isStreamQuery()) {
			return queryOp.matching(request).stream();
		} else {
			return queryOp.matching(request).oneValue();
		}
	}

	private PageImpl<?> executePage(ParametersParameterAccessor accessor, SearchRequest request) {
		Pageable pageable = accessor.getPageable();
		List<?> content = createQueryOperation(accessor, true).matching(request).all();
		long total = createQueryOperation(accessor, false).matching(request).count();
		return new PageImpl<>(content, pageable, total);
	}

	private SliceImpl<?> executeSlice(ParametersParameterAccessor accessor, SearchRequest request) {
		Pageable pageable = accessor.getPageable();
		if (!pageable.isPaged()) {
			return new SliceImpl<>(createQueryOperation(accessor, false).matching(request).all(), pageable, false);
		}

		int pageSize = pageable.getPageSize();
		List<?> content = createQueryOperation(accessor, false, pageSize + 1, (int) pageable.getOffset()).matching(request)
				.all();
		boolean hasNext = pageable.isPaged() && content.size() > pageSize;
		if (hasNext) {
			content = content.subList(0, pageSize);
		}
		return new SliceImpl<>(content, pageable, hasNext);
	}

	private ExecutableFindBySearchOperation.FindBySearchWithQuery<?> createQueryOperation(
			ParametersParameterAccessor accessor, boolean applyPagination) {
		return createQueryOperation(accessor, applyPagination, null, null);
	}

	private ExecutableFindBySearchOperation.FindBySearchWithQuery<?> createQueryOperation(
			ParametersParameterAccessor accessor, boolean applyPagination, Integer limitOverride, Integer skipOverride) {
		Class<?> domainType = method.getEntityInformation().getJavaType();
		ExecutableFindBySearchOperation.FindBySearchWithProjection<?> withIndex = operations.findBySearch(domainType)
				.withIndex(indexName);

		ExecutableFindBySearchOperation.FindBySearchWithConsistency<?> withPagination = applyPagination
				? applyPagination(withIndex, accessor)
				: applyLimitAndSkip(withIndex, limitOverride, skipOverride);

		SearchScanConsistency searchConsistency = method.getSearchScanConsistency();
		ExecutableFindBySearchOperation.FindBySearchInScope<?> withConsistency = searchConsistency != null
				? withPagination.withConsistency(searchConsistency)
				: withPagination;

		return withConsistency.inScope(method.getScope()).inCollection(method.getCollection());
	}

	private ExecutableFindBySearchOperation.FindBySearchWithConsistency<?> applyPagination(
			ExecutableFindBySearchOperation.FindBySearchWithProjection<?> searchOp,
			ParametersParameterAccessor accessor) {
		Pageable pageable = accessor.getPageable();
		if (pageable.isPaged()) {
			return applyLimitAndSkip(searchOp, (int) pageable.getPageSize(), (int) pageable.getOffset());
		}
		return applyLimitAndSkip(searchOp, null, null);
	}

	private ExecutableFindBySearchOperation.FindBySearchWithConsistency<?> applyLimitAndSkip(
			ExecutableFindBySearchOperation.FindBySearchWithProjection<?> searchOp, Integer limit, Integer skip) {
		ExecutableFindBySearchOperation.FindBySearchWithLimit<?> withSkipApplied = skip != null
				? searchOp.withSkip(skip)
				: searchOp;
		return limit != null ? withSkipApplied.withLimit(limit) : withSkipApplied;
	}

	private static String resolveIndexName(CouchbaseQueryMethod method) {
		SearchIndex methodAnnotation = method.getAnnotation(SearchIndex.class);
		if (methodAnnotation != null) {
			return methodAnnotation.value();
		}
		SearchIndex entityAnnotation = method.getEntityInformation().getJavaType().getAnnotation(SearchIndex.class);
		if (entityAnnotation != null) {
			return entityAnnotation.value();
		}
		return null;
	}
}
