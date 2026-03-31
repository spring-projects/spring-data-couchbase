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

import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveFindBySearchOperation;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.util.ReactiveWrapperConverters;
import org.springframework.util.Assert;

import org.springframework.data.couchbase.repository.Search;
import org.springframework.data.couchbase.repository.SearchIndex;

import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.SearchRequest;
import com.couchbase.client.java.search.SearchScanConsistency;

/**
 * Reactive {@link RepositoryQuery} implementation for FTS {@link Search}-annotated methods.
 * <p>
 * Supports positional parameter substitution ({@code ?0}, {@code ?1}, etc.) in the query string.
 * Supports {@link Pageable} parameters for limit/skip pagination.
 * Supports {@link org.springframework.data.couchbase.repository.ScanConsistency} for FTS scan consistency.
 *
 * @author Emilien Bevierre
 * @since 6.2
 */
public class ReactiveSearchBasedCouchbaseQuery implements RepositoryQuery {

	private final ReactiveCouchbaseQueryMethod method;
	private final ReactiveCouchbaseOperations operations;
	private final String searchQueryTemplate;
	private final String indexName;

	public ReactiveSearchBasedCouchbaseQuery(ReactiveCouchbaseQueryMethod method,
			ReactiveCouchbaseOperations operations) {
		Assert.notNull(method, "ReactiveCouchbaseQueryMethod must not be null!");
		Assert.notNull(operations, "ReactiveCouchbaseOperations must not be null!");

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
		ReactiveCouchbaseParameterAccessor accessor = new ReactiveCouchbaseParameterAccessor(method, parameters);

		return accessor.resolveParameters().flatMapMany(resolvedAccessor -> {
			ResultProcessor processor = method.getResultProcessor().withDynamicProjection(resolvedAccessor);
			SearchRepositoryQuerySupport.validateSort(resolvedAccessor);

			String resolvedQuery = SearchBasedCouchbaseQuery.resolveParameters(searchQueryTemplate, resolvedAccessor);
			SearchRequest request = SearchRequest.create(SearchQuery.queryString(resolvedQuery));
			Object result = executeDependingOnType(resolvedAccessor, request);

			return ReactiveWrapperConverters.toWrapper(processor.processResult(result), reactor.core.publisher.Flux.class);
		});
	}

	@Override
	public ReactiveCouchbaseQueryMethod getQueryMethod() {
		return method;
	}

	private Object executeDependingOnType(ParametersParameterAccessor accessor, SearchRequest request) {
		ReactiveFindBySearchOperation.FindBySearchWithQuery<?> queryOp = createQueryOperation(accessor, true);

		if (method.isCountQuery()) {
			return createQueryOperation(accessor, false).matching(request).count();
		} else if (method.isExistsQuery()) {
			return createQueryOperation(accessor, false).matching(request).exists();
		} else if (method.isCollectionQuery()) {
			return queryOp.matching(request).all();
		} else {
			return queryOp.matching(request).one();
		}
	}

	private ReactiveFindBySearchOperation.FindBySearchWithQuery<?> createQueryOperation(
			ParametersParameterAccessor accessor, boolean applyPagination) {
		Class<?> domainType = method.getEntityInformation().getJavaType();
		ReactiveFindBySearchOperation.FindBySearchWithProjection<?> withIndex = operations.findBySearch(domainType)
				.withIndex(indexName);

		ReactiveFindBySearchOperation.FindBySearchWithConsistency<?> withPagination = applyPagination
				? applyPagination(withIndex, accessor)
				: withIndex;

		SearchScanConsistency searchConsistency = method.getSearchScanConsistency();
		ReactiveFindBySearchOperation.FindBySearchInScope<?> withConsistency = searchConsistency != null
				? withPagination.withConsistency(searchConsistency)
				: withPagination;

		return withConsistency.inScope(method.getScope()).inCollection(method.getCollection());
	}

	private ReactiveFindBySearchOperation.FindBySearchWithConsistency<?> applyPagination(
			ReactiveFindBySearchOperation.FindBySearchWithProjection<?> searchOp,
			ParametersParameterAccessor accessor) {
		Pageable pageable = accessor.getPageable();
		if (pageable.isPaged()) {
			return searchOp
					.withSkip((int) pageable.getOffset())
					.withLimit((int) pageable.getPageSize());
		}
		return searchOp;
	}
	private static String resolveIndexName(ReactiveCouchbaseQueryMethod method) {
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
