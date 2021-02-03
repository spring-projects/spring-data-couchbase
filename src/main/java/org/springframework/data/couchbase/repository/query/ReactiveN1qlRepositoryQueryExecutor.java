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
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @deprecated
 */
@Deprecated
public class ReactiveN1qlRepositoryQueryExecutor {

	private final ReactiveCouchbaseOperations operations;
	private final ReactiveCouchbaseQueryMethod queryMethod;
	private final NamedQueries namedQueries;
	private final QueryMethodEvaluationContextProvider evaluationContextProvider;

	public ReactiveN1qlRepositoryQueryExecutor(final ReactiveCouchbaseOperations operations,
			final ReactiveCouchbaseQueryMethod queryMethod, final NamedQueries namedQueries,
			QueryMethodEvaluationContextProvider evaluationContextProvider) {
		this.operations = operations;
		this.queryMethod = queryMethod;
		this.namedQueries = namedQueries;
		this.evaluationContextProvider = evaluationContextProvider;
		//throw new RuntimeException("Deprecated");
	}

	/**
	 * see also {@link N1qlRepositoryQueryExecutor#execute(Object[] parameters) execute }
	 *
	 * @param parameters
	 * @return
	 */
	public Object execute(final Object[] parameters) {
		// counterpart to N1qlRespositoryQueryExecutor,

		if (queryMethod.hasN1qlAnnotation()) {
			return new ReactiveStringBasedCouchbaseQuery(queryMethod, operations, new SpelExpressionParser(),
					evaluationContextProvider, namedQueries).execute(parameters);
		} else {
			return new ReactivePartTreeCouchbaseQuery(queryMethod, operations, new SpelExpressionParser(),
					evaluationContextProvider).execute(parameters);
		}

	}

}
