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
