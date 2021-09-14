/*
 * Copyright 2020-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository.query;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveFindByQueryOperation;
import org.springframework.data.couchbase.core.ReactiveFindByQueryOperation.ReactiveFindByQuery;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.repository.query.ReactiveCouchbaseQueryExecution.DeleteExecution;
import org.springframework.data.couchbase.repository.query.ReactiveCouchbaseQueryExecution.ResultProcessingExecution;
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
 * Base class for reactive {@link RepositoryQuery} implementations for Couchbase.
 *
 * @author Michael Reiche
 * @since 4.1
 */
public abstract class AbstractReactiveCouchbaseQuery extends AbstractCouchbaseQueryBase<ReactiveCouchbaseOperations>
		implements RepositoryQuery {

	private final ReactiveFindByQuery<?> findOperationWithProjection;

	/**
	 * Creates a new {@link AbstractReactiveCouchbaseQuery} from the given {@link ReactiveCouchbaseQueryMethod} and
	 * {@link org.springframework.data.couchbase.core.CouchbaseOperations}.
	 *
	 * @param method must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 */
	public AbstractReactiveCouchbaseQuery(ReactiveCouchbaseQueryMethod method, ReactiveCouchbaseOperations operations,
			SpelExpressionParser expressionParser, QueryMethodEvaluationContextProvider evaluationContextProvider) {
		super(method, operations, expressionParser, evaluationContextProvider);
		Assert.notNull(method, "CouchbaseQueryMethod must not be null!");
		Assert.notNull(operations, "ReactiveCouchbaseOperations must not be null!");
		Assert.notNull(expressionParser, "SpelExpressionParser must not be null!");
		Assert.notNull(evaluationContextProvider, "QueryMethodEvaluationContextProvider must not be null!");

		EntityMetadata<?> metadata = method.getEntityInformation();
		Class<?> type = metadata.getJavaType();
		ReactiveFindByQuery<?> findOp = operations.findByQuery(type);
		findOp = (ReactiveFindByQuery<?>) (findOp.inScope(method.getScope()).inCollection(method.getCollection()));
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
	protected Object doExecute(CouchbaseQueryMethod method, ResultProcessor processor,
			ParametersParameterAccessor accessor, @Nullable Class<?> typeToRead) {

		Query query = createQuery(accessor);
		// query = applyAnnotatedCollationIfPresent(query, accessor); // not yet implemented
		query = applyQueryMetaAttributesIfPresent(query, typeToRead);

		ReactiveFindByQuery<?> find = findOperationWithProjection;

		ReactiveCouchbaseQueryExecution execution = getExecution(accessor,
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
	private ReactiveCouchbaseQueryExecution getExecution(ParameterAccessor accessor,
			Converter<Object, Object> resultProcessing, ReactiveFindByQuery<?> operation) {
		return new ResultProcessingExecution(getExecutionToWrap(accessor, operation), resultProcessing);
	}

	/**
	 * Returns the execution to wrap
	 *
	 * @param accessor must not be {@literal null}.
	 * @param operation must not be {@literal null}.
	 * @return
	 */
	private ReactiveCouchbaseQueryExecution getExecutionToWrap(ParameterAccessor accessor,
			ReactiveFindByQuery<?> operation) {

		if (isDeleteQuery()) {
			return new DeleteExecution(getOperations(), getQueryMethod());
		} else if (isTailable(getQueryMethod())) {
			return (q, t, r, c) -> operation.as(r).matching(q.with(accessor.getPageable())).all(); // s/b tail() instead of
																																															// all()
		} else if (getQueryMethod().isCollectionQuery()) {
			return (q, t, r, c) -> operation.as(r).matching(q.with(accessor.getPageable())).all();
			// } else if (getQueryMethod().isStreamQuery()) {
			// return (q, t, c) -> operation.matching(q.with(accessor.getPageable())).all().toStream();
		} else if (isCountQuery()) {
			return (q, t, r, c) -> operation.as(r).matching(q).count();
		} else if (isExistsQuery()) {
			return (q, t, r, c) -> operation.as(r).matching(q).exists();
		} else {
			return (q, t, r, c) -> {
				ReactiveFindByQueryOperation.TerminatingFindByQuery<?> find = operation.as(r).matching(q);
				return isLimiting() ? find.first() : find.one();
			};
		}
	}

}
