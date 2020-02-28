package org.springframework.data.couchbase.repository.query;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;

import java.util.Iterator;
import java.util.List;

public class N1qlRepositoryQueryExecutor {

	private final CouchbaseOperations operations;
	private final QueryMethod queryMethod;

	public N1qlRepositoryQueryExecutor(final CouchbaseOperations operations, final QueryMethod queryMethod) {
		this.operations = operations;
		this.queryMethod = queryMethod;
	}

	public Object execute(final Object[] parameters) {
		final Class<?> domainClass = queryMethod.getResultProcessor().getReturnedType().getDomainType();
		final Query query = queryFromPartTree(domainClass, null);

		List<?> all = operations.findByQuery(domainClass).matching(query).all();
		return all;
	}

	private Query queryFromPartTree(final Class<?> domainClass, Iterator<Object> parameters) {
		final PartTree tree = new PartTree(queryMethod.getName(), domainClass);

		Query query = new Query();

		for (Part part : tree.getParts()) {
			Part.Type type = part.getType();

			switch (type) {

			}
		}

		return query;
	}

}
