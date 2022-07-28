/*
 * Copyright 2012-2020 the original author or authors
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

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @deprecated
 */
@Deprecated
public class CouchbaseRepositoryQuery implements RepositoryQuery {

	private final CouchbaseOperations operations;
	private final CouchbaseQueryMethod queryMethod;
	private final NamedQueries namedQueries;
	private final QueryMethodEvaluationContextProvider evaluationContextProvider;

	public CouchbaseRepositoryQuery(final CouchbaseOperations operations, final CouchbaseQueryMethod queryMethod,
			final NamedQueries namedQueries) {
		this.operations = operations;
		this.queryMethod = queryMethod;
		this.namedQueries = namedQueries;
		this.evaluationContextProvider = QueryMethodEvaluationContextProvider.DEFAULT;
	}

	@Override
	public Object execute(final Object[] parameters) {
		return new N1qlRepositoryQueryExecutor(operations, queryMethod, namedQueries, evaluationContextProvider)
				.execute(parameters);
	}

	@Override
	public QueryMethod getQueryMethod() {
		return queryMethod;
	}

}
