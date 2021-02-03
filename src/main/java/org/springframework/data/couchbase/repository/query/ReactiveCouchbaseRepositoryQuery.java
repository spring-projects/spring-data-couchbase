/*
 * Copyright 2012-2021 the original author or authors
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
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @deprecated
 */
@Deprecated
public class ReactiveCouchbaseRepositoryQuery extends AbstractReactiveCouchbaseQuery {

	private final ReactiveCouchbaseOperations operations;
	private final ReactiveCouchbaseQueryMethod queryMethod;
	private final NamedQueries namedQueries;
	private final QueryMethodEvaluationContextProvider evaluationContextProvider;

	public ReactiveCouchbaseRepositoryQuery(final ReactiveCouchbaseOperations operations,
			final ReactiveCouchbaseQueryMethod queryMethod, final NamedQueries namedQueries) {
		super(queryMethod, operations, new SpelExpressionParser(), QueryMethodEvaluationContextProvider.DEFAULT);
		this.operations = operations;
		this.queryMethod = queryMethod;
		this.namedQueries = namedQueries;
		this.evaluationContextProvider = QueryMethodEvaluationContextProvider.DEFAULT;
		//throw new RuntimeException("Deprecated");
	}

	@Override
	public Object execute(final Object[] parameters) {
		return new ReactiveN1qlRepositoryQueryExecutor(operations, queryMethod, namedQueries, evaluationContextProvider)
				.execute(parameters);
	}

	@Override
	protected Query createCountQuery(ParametersParameterAccessor accessor) {
		return null;
	}

	@Override
	protected Query createQuery(ParametersParameterAccessor accessor) {
		return null;
	}

	@Override
	protected boolean isLimiting() {
		// TODO
		return false;
	}

	@Override
	public ReactiveCouchbaseQueryMethod getQueryMethod() {
		return queryMethod;
	}

}
