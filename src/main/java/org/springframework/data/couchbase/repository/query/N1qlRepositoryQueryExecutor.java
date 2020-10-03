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

import com.couchbase.client.java.query.QueryScanConsistency;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.ExecutableFindByQueryOperation;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
public class N1qlRepositoryQueryExecutor {

	private final CouchbaseOperations operations;
	private final CouchbaseQueryMethod queryMethod;
	private final NamedQueries namedQueries;

	public N1qlRepositoryQueryExecutor(final CouchbaseOperations operations, final CouchbaseQueryMethod queryMethod,
			final NamedQueries namedQueries) {
		this.operations = operations;
		this.queryMethod = queryMethod;
		this.namedQueries = namedQueries;
	}

	/**
	 * see also {@link ReactiveN1qlRepositoryQueryExecutor#execute(Object[] parameters) execute }
	 *
	 * @param parameters
	 * @return
	 */
	public Object execute(final Object[] parameters) {
		final Class<?> domainClass = queryMethod.getResultProcessor().getReturnedType().getDomainType();
		final ParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);

		// this is identical to ReactiveN1qlRespositoryQueryExecutor,
		// except for the type of 'q', and the call to oneValue() vs one()
		Query query;
		ExecutableFindByQueryOperation.ExecutableFindByQuery q;
		if (queryMethod.hasN1qlAnnotation()) {
			query = new StringN1qlQueryCreator(accessor, queryMethod, operations.getConverter(), operations.getBucketName(),
					QueryMethodEvaluationContextProvider.DEFAULT, namedQueries).createQuery();
		} else {
			final PartTree tree = new PartTree(queryMethod.getName(), domainClass);
			query = new N1qlQueryCreator(tree, accessor, queryMethod, operations.getConverter()).createQuery();
		}
		q = (ExecutableFindByQueryOperation.ExecutableFindByQuery) operations.findByQuery(domainClass)
				.consistentWith(buildQueryScanConsistency()).matching(query);
		if (queryMethod.isCountQuery()) {
			return q.count();
		} else if (queryMethod.isCollectionQuery()) {
			return q.all();
		} else {
			return q.oneValue();
		}

	}

	private QueryScanConsistency buildQueryScanConsistency() {
		QueryScanConsistency scanConsistency = QueryScanConsistency.NOT_BOUNDED;
		if (queryMethod.hasConsistencyAnnotation()) {
			scanConsistency = queryMethod.getConsistencyAnnotation().value();
		} else if (queryMethod.hasScanConsistencyAnnotation()) {
			scanConsistency = queryMethod.getScanConsistencyAnnotation().query();
		}
		return scanConsistency;
	}

}
