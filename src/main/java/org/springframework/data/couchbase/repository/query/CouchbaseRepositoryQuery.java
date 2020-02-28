package org.springframework.data.couchbase.repository.query;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;

public class CouchbaseRepositoryQuery implements RepositoryQuery {

	private final CouchbaseOperations operations;
	private final QueryMethod queryMethod;

	public CouchbaseRepositoryQuery(final CouchbaseOperations operations, final QueryMethod queryMethod) {
		this.operations = operations;
		this.queryMethod = queryMethod;
	}

	@Override
	public Object execute(final Object[] parameters) {
		return new N1qlRepositoryQueryExecutor(operations, queryMethod).execute(parameters);
	}

	@Override
	public QueryMethod getQueryMethod() {
		return queryMethod;
	}

}
