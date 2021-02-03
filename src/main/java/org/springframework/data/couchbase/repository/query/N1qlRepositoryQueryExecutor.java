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

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.ExecutableFindByQueryOperation.ExecutableFindByQuery;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @deprecated
 */
@Deprecated
public class N1qlRepositoryQueryExecutor {

	private final CouchbaseOperations operations;
	private final CouchbaseQueryMethod queryMethod;
	private final NamedQueries namedQueries;
	private final QueryMethodEvaluationContextProvider evaluationContextProvider;

	public N1qlRepositoryQueryExecutor(final CouchbaseOperations operations, final CouchbaseQueryMethod queryMethod,
			final NamedQueries namedQueries, final QueryMethodEvaluationContextProvider evaluationContextProvider) {
		this.operations = operations;
		this.queryMethod = queryMethod;
		this.namedQueries = namedQueries;
		this.evaluationContextProvider = evaluationContextProvider;
		//throw new RuntimeException("Deprecated");
	}

	private static final SpelExpressionParser SPEL_PARSER = new SpelExpressionParser();

	/**
	 * see also {@link ReactiveN1qlRepositoryQueryExecutor#execute(Object[] parameters) execute }
	 *
	 * @param parameters
	 * @return
	 */
	public Object execute(final Object[] parameters) {
		final Class<?> domainClass = queryMethod.getResultProcessor().getReturnedType().getDomainType();
		final ParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);

		// counterpart to ReactiveN1qlRespositoryQueryExecutor,
		Query query;
		ExecutableFindByQuery q;
		if (queryMethod.hasN1qlAnnotation()) {
			query = new StringN1qlQueryCreator(accessor, queryMethod, operations.getConverter(), operations.getBucketName(),
					SPEL_PARSER, evaluationContextProvider, namedQueries).createQuery();
		} else {
			final PartTree tree = new PartTree(queryMethod.getName(), domainClass);
			query = new N1qlQueryCreator(tree, accessor, queryMethod, operations.getConverter(), operations.getBucketName()).createQuery();
		}

		ExecutableFindByQuery<?> operation = (ExecutableFindByQuery<?>) operations
				.findByQuery(domainClass).withConsistency(buildQueryScanConsistency());
		if (queryMethod.isCountQuery()) {
			return operation.matching(query).count();
		} else if (queryMethod.isCollectionQuery()) {
			return operation.matching(query).all();
		} else if (queryMethod.isPageQuery()) {
			Pageable p = accessor.getPageable();
			return new CouchbaseQueryExecution.PagedExecution(operation, p).execute(query, null, null);
		} else {
			return operation.matching(query).oneValue();
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
