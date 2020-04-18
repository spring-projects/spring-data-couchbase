package org.springframework.data.couchbase.repository.query;

import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;

public class ReactiveCouchbaseRepositoryQuery implements RepositoryQuery {

	private final ReactiveCouchbaseOperations operations;
	private final QueryMethod queryMethod;

	public ReactiveCouchbaseRepositoryQuery(final ReactiveCouchbaseOperations operations, final QueryMethod queryMethod) {
		this.operations = operations;
		this.queryMethod = queryMethod;
	}

	@Override
	public Object execute(final Object[] parameters) {
		return new ReactiveN1qlRepositoryQueryExecutor(operations, queryMethod).execute(parameters);
	}

	@Override
	public QueryMethod getQueryMethod() {
		return queryMethod;
	}

}
