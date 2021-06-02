/*
 * Copyright 2020 the original author or authors
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.reactivestreams.Publisher;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.ExecutableFindByQueryOperation.ExecutableFindByQuery;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.repository.core.EntityMetadata;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link RepositoryQuery} implementation for Couchbase. CouchbaseOperationsType is either CouchbaseOperations or
 * ReactiveCouchbaseOperations
 * 
 * @author Michael Reiche
 * @since 4.1
 */
public abstract class AbstractCouchbaseQueryBase<CouchbaseOperationsType> implements RepositoryQuery {

	private final CouchbaseQueryMethod method;
	private final CouchbaseOperationsType operations;
	private final EntityInstantiators instantiators;
	private final ExecutableFindByQuery<?> findOperationWithProjection;
	private final SpelExpressionParser expressionParser;
	private final QueryMethodEvaluationContextProvider evaluationContextProvider;

	/**
	 * Creates a new {@link AbstractCouchbaseQuery} from the given {@link ReactiveCouchbaseQueryMethod} and
	 * {@link org.springframework.data.couchbase.core.CouchbaseOperations}.
	 *
	 * @param method must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 */
	public AbstractCouchbaseQueryBase(CouchbaseQueryMethod method, CouchbaseOperationsType operations,
			SpelExpressionParser expressionParser, QueryMethodEvaluationContextProvider evaluationContextProvider) {

		Assert.notNull(method, "CouchbaseQueryMethod must not be null!");
		Assert.notNull(operations, "ReactiveCouchbaseOperations must not be null!");
		Assert.notNull(expressionParser, "SpelExpressionParser must not be null!");
		Assert.notNull(evaluationContextProvider, "QueryMethodEvaluationContextProvider must not be null!");

		this.method = method;
		this.operations = operations;
		this.instantiators = new EntityInstantiators();
		this.expressionParser = expressionParser;
		this.evaluationContextProvider = evaluationContextProvider;

		EntityMetadata<?> metadata = method.getEntityInformation();
		Class<?> type = metadata.getJavaType();
		this.findOperationWithProjection = operations instanceof CouchbaseOperations
				? ((CouchbaseOperations) operations).findByQuery(type)
				: null; // not yet implemented for Reactive
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#getQueryMethod()
	 */
	public CouchbaseQueryMethod getQueryMethod() {
		return method;
	};

	/*
	 * (non-Javadoc)
	 */
	public CouchbaseOperationsType getOperations() {
		return operations;
	}

	/*
	 * (non-Javadoc)
	 */
	EntityInstantiators getInstantiators() {
		return instantiators;
	}

	/**
	 * Execute the query with the provided parameters
	 * 
	 * @see org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.Object[])
	 */
	public Object execute(Object[] parameters) {
		return method.hasReactiveWrapperParameter() ? executeDeferred(parameters)
				: execute(new ReactiveCouchbaseParameterAccessor(getQueryMethod(), parameters));
	}

	private Object executeDeferred(Object[] parameters) {
		ReactiveCouchbaseParameterAccessor parameterAccessor = new ReactiveCouchbaseParameterAccessor(method, parameters);
		if (getQueryMethod().isCollectionQuery()) {
			return Flux.defer(() -> (Publisher<Object>) execute(parameterAccessor));
		}
		return Mono.defer(() -> (Mono<Object>) execute(parameterAccessor));
	}

	private Object execute(ParametersParameterAccessor parameterAccessor) {
		TypeInformation<?> returnType = ClassTypeInformation
				.from(method.getResultProcessor().getReturnedType().getReturnedType());
		ResultProcessor processor = method.getResultProcessor().withDynamicProjection(parameterAccessor);
		Class<?> typeToRead = processor.getReturnedType().getTypeToRead();

		if (typeToRead == null && returnType.getComponentType() != null) {
			typeToRead = returnType.getComponentType().getType();
		}
		return doExecute(getQueryMethod(), processor, parameterAccessor, typeToRead);
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
	abstract protected Object doExecute(CouchbaseQueryMethod method, ResultProcessor processor,
			ParametersParameterAccessor accessor, @Nullable Class<?> typeToRead);

	/**
	 * Add a scan consistency from {@link org.springframework.data.couchbase.repository.ScanConsistency} to the given
	 * {@link Query} if present.
	 *
	 * @param query the {@link Query} to potentially apply the sort to.
	 * @return the query with potential scan consistency applied.
	 * @since 4.1
	 */
	Query applyAnnotatedConsistencyIfPresent(Query query) {
		if (!method.hasScanConsistencyAnnotation()) {
			return query;
		}
		return query.scanConsistency(method.getScanConsistencyAnnotation().query());
	}

	/**
	 * Creates a {@link Query} instance using the given {@link ParametersParameterAccessor}. Will delegate to
	 * {@link #createQuery(ParametersParameterAccessor)} by default but allows customization of the count query to be
	 * triggered.
	 *
	 * @param accessor must not be {@literal null}.
	 * @return
	 */
	protected abstract Query createCountQuery(ParametersParameterAccessor accessor);

	/**
	 * Creates a {@link Query} instance using the given {@link ParameterAccessor}
	 *
	 * @param accessor must not be {@literal null}.
	 * @return
	 */
	protected abstract Query createQuery(ParametersParameterAccessor accessor);

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.couchbase.repository.query.AbstractReactiveCouchbaseQuery#isCountQuery()
	 */
	protected boolean isCountQuery() {
		return getQueryMethod().isCountQuery();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.couchbase.repository.query.AbstractReactiveCouchbaseQuery#isExistsQuery()
	 */
	protected boolean isExistsQuery() {
		return getQueryMethod().isExistsQuery();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.couchbase.repository.query.AbstractReactiveCouchbaseQuery#isDeleteQuery()
	 */
	protected boolean isDeleteQuery() {
		return getQueryMethod().isDeleteQuery();
	}

	/**
	 * Return whether the query is tailable
	 *
	 * @return
	 */
	boolean isTailable(CouchbaseQueryMethod method) {
		return false; // method.getTailableAnnotation() != null; // Not yet implemented
	}

	/**
	 * Return whether the query has an explicit limit set.
	 *
	 * @return
	 */
	protected abstract boolean isLimiting();

	/**
	 * Return whether there are ambiguous projection flags
	 *
	 * @return
	 */
	static boolean hasAmbiguousProjectionFlags(boolean isCountQuery, boolean isExistsQuery, boolean isDeleteQuery) {
		return multipleOf(isCountQuery, isExistsQuery, isDeleteQuery);
	}

	/**
	 * Count the number of {@literal true} values.
	 *
	 * @param values
	 * @return are there more than one of these true?
	 */
	static boolean multipleOf(boolean... values) {
		int count = 0;
		for (boolean value : values) {
			if (value) {
				if (count != 0) {
					return true;
				}
				count++;
			}
		}
		return false;
	}
}
