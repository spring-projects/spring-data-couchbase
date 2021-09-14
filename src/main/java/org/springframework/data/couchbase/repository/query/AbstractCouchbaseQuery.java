/*
 * Copyright 2020-2021 the original author or authors
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

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.ExecutableFindByQueryOperation.ExecutableFindByQuery;
import org.springframework.data.couchbase.core.ExecutableFindByQueryOperation.TerminatingFindByQuery;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.repository.query.CouchbaseQueryExecution.DeleteExecution;
import org.springframework.data.couchbase.repository.query.CouchbaseQueryExecution.PagedExecution;
import org.springframework.data.repository.core.EntityMetadata;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link RepositoryQuery} implementation for Couchbase.
 *
 * @author Michael Reiche
 * @since 4.1
 */
public abstract class AbstractCouchbaseQuery extends AbstractCouchbaseQueryBase<CouchbaseOperations>
		implements RepositoryQuery {

	private final ExecutableFindByQuery<?> findOperationWithProjection;

	/**
	 * Creates a new {@link AbstractCouchbaseQuery} from the given {@link ReactiveCouchbaseQueryMethod} and
	 * {@link org.springframework.data.couchbase.core.CouchbaseOperations}.
	 *
	 * @param method must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 */
	public AbstractCouchbaseQuery(CouchbaseQueryMethod method, CouchbaseOperations operations,
			SpelExpressionParser expressionParser, QueryMethodEvaluationContextProvider evaluationContextProvider) {
		super(method, operations, expressionParser, evaluationContextProvider);
		Assert.notNull(method, "CouchbaseQueryMethod must not be null!");
		Assert.notNull(operations, "ReactiveCouchbaseOperations must not be null!");
		Assert.notNull(expressionParser, "SpelExpressionParser must not be null!");
		Assert.notNull(evaluationContextProvider, "QueryMethodEvaluationContextProvider must not be null!");
		EntityMetadata<?> metadata = method.getEntityInformation();
		Class<?> type = metadata.getJavaType();
		ExecutableFindByQuery<?> findOp = operations.findByQuery(type);
		findOp = (ExecutableFindByQuery<?>) (findOp.inScope(method.getScope()).inCollection(method.getCollection()));
		this.findOperationWithProjection = findOp;
	}

	/**
	 * Execute the {@link RepositoryQuery} of the given method with the parameters provided by the
	 * {@link ParametersParameterAccessor accessor}
	 *
	 * @param method the {@link ReactiveCouchbaseQueryMethod} invoked. Never {@literal null}.
	 * @param processor {@link ResultProcessor} for post procession. Never {@literal null}.
	 * @param accessor for providing invocation arguments. Never {@literal null}.
	 * @param typeToRead the desired component target type. Can be {@literal null}.
	 */
	@Override
	protected Object doExecute(CouchbaseQueryMethod method, ResultProcessor processor,
			ParametersParameterAccessor accessor, @Nullable Class<?> typeToRead) {

		Query query = createQuery(accessor);
		// query = applyAnnotatedCollationIfPresent(query, accessor); // not yet implemented
		query = applyQueryMetaAttributesIfPresent(query, typeToRead);

		ExecutableFindByQuery<?> find = findOperationWithProjection;

		CouchbaseQueryExecution execution = getExecution(accessor,
				new ResultProcessingConverter<>(processor, getOperations(), getInstantiators()), find);
		return execution.execute(query, processor.getReturnedType().getDomainType(), typeToRead, null);
	}

	/**
	 * Returns the execution instance to use.
	 *
	 * @param accessor must not be {@literal null}.
	 * @param resultProcessing must not be {@literal null}.
	 * @return
	 */
	private CouchbaseQueryExecution getExecution(ParameterAccessor accessor, Converter<Object, Object> resultProcessing,
			ExecutableFindByQuery<?> operation) {
		return new CouchbaseQueryExecution.ResultProcessingExecution(getExecutionToWrap(accessor, operation),
				resultProcessing);
	}

	/**
	 * Returns the execution to wrap
	 *
	 * @param accessor must not be {@literal null}.
	 * @param operation must not be {@literal null}.
	 * @return
	 */
	private CouchbaseQueryExecution getExecutionToWrap(ParameterAccessor accessor, ExecutableFindByQuery<?> operation) {

		if (isDeleteQuery()) {
			return new DeleteExecution(getOperations(), getQueryMethod());
		} else if (isTailable(getQueryMethod())) {
			return (q, t, r, c) -> operation.as(r).matching(q.with(accessor.getPageable())).all(); // s/b tail() instead of
																																															// all()
		} else if (getQueryMethod().isCollectionQuery()) {
			return (q, t, r, c) -> operation.as(r).matching(q.with(accessor.getPageable())).all();
		} else if (getQueryMethod().isStreamQuery()) {
			return (q, t, r, c) -> operation.as(r).matching(q.with(accessor.getPageable())).stream();
		} else if (isCountQuery()) {
			return (q, t, r, c) -> operation.as(r).matching(q).count();
		} else if (isExistsQuery()) {
			return (q, t, r, c) -> operation.as(r).matching(q).exists();
		} else if (getQueryMethod().isPageQuery()) {
			return new PagedExecution(operation, accessor.getPageable());
		} else {
			return (q, t, r, c) -> {
				TerminatingFindByQuery<?> find = operation.as(r).matching(q);
				if (isCountQuery()) {
					return find.count();
				}
				return isLimiting() ? find.first() : find.one();
			};
		}
	}

}
