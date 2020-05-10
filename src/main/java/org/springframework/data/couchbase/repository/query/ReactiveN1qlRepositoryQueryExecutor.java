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

import org.springframework.data.couchbase.core.ExecutableFindByQueryOperation;
import org.springframework.data.couchbase.core.ReactiveFindByQueryOperation;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.query.*;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import reactor.core.publisher.Flux;

import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.repository.query.parser.PartTree;

import java.util.List;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
public class ReactiveN1qlRepositoryQueryExecutor {

	private final ReactiveCouchbaseOperations operations;
	private final CouchbaseQueryMethod queryMethod;
	private final NamedQueries namedQueries;

	public ReactiveN1qlRepositoryQueryExecutor(final ReactiveCouchbaseOperations operations,
			final CouchbaseQueryMethod queryMethod, final NamedQueries namedQueries) {
		this.operations = operations;
		this.queryMethod = queryMethod;
		this.namedQueries = namedQueries;
	}

	/**
	 * see also {@link N1qlRepositoryQueryExecutor#execute(Object[] parameters) execute }
	 *
	 * @param parameters
	 * @return
	 */
	public Object execute(final Object[] parameters) {
		final Class<?> domainClass = queryMethod.getResultProcessor().getReturnedType().getDomainType();
		final ParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
		final String namedQueryName = queryMethod.getNamedQueryName();

		// this is identical to ExecutableN1qlRespositoryQueryExecutor,
		// except for the type of 'q', and the call to one() vs oneValue()

		Query query;
		ReactiveFindByQueryOperation.ReactiveFindByQuery q;
		if (queryMethod.hasN1qlAnnotation()) {
			query = new StringN1qlQueryCreator(accessor, queryMethod, operations.getConverter(),
					operations.getBucketName(), QueryMethodEvaluationContextProvider.DEFAULT,
					namedQueries).createQuery();
		} else {
			final PartTree tree = new PartTree(queryMethod.getName(), domainClass);
			query = new N1qlQueryCreator(tree, accessor, queryMethod, operations.getConverter()).createQuery();
		}
		q = (ReactiveFindByQueryOperation.ReactiveFindByQuery) operations.findByQuery(domainClass).matching(query);
		if (queryMethod.isCollectionQuery()) {
			return q.all();
		} else {
			return q.one();
		}
	}

}
