package org.springframework.data.couchbase.repository.query;

import reactor.core.publisher.Flux;

import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.PartTree;

public class ReactiveN1qlRepositoryQueryExecutor {

	private final ReactiveCouchbaseOperations operations;
	private final QueryMethod queryMethod;

	public ReactiveN1qlRepositoryQueryExecutor(final ReactiveCouchbaseOperations operations,
			final QueryMethod queryMethod) {
		this.operations = operations;
		this.queryMethod = queryMethod;
	}

	public Object execute(final Object[] parameters) {
		final Class<?> domainClass = queryMethod.getResultProcessor().getReturnedType().getDomainType();
		final ParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);

		final PartTree tree = new PartTree(queryMethod.getName(), domainClass);
		Query query = new N1qlQueryCreator(tree, accessor, operations.getConverter().getMappingContext()).createQuery();

		Flux<?> all = operations.findByQuery(domainClass).matching(query).all();
		return all;
	}

}
