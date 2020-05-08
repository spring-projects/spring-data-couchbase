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

import java.util.List;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.mapping.context.MappingContext;
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

	public Object execute(final Object[] parameters) {
		final Class<?> domainClass = queryMethod.getResultProcessor().getReturnedType().getDomainType();
		final ParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
		final MappingContext mappingCtx = operations.getConverter().getMappingContext();
		final String namedQueryName = queryMethod.getNamedQueryName();

		Query query;
		if (queryMethod.hasN1qlAnnotation()) {
			String queryString = null;
			if (queryMethod.hasInlineN1qlQuery()) {
				queryString = queryMethod.getInlineN1qlQuery();
			} else if (namedQueries.hasQuery(namedQueryName)) {
				queryString = namedQueries.getQuery(namedQueryName);
			} else {
				throw new RuntimeException("query has n1ql annotation but no inline Query or named Query not found");
			}
			query = new StringN1qlQueryCreator(queryString, accessor, mappingCtx, queryMethod,
					operations.getConverter(), operations.getBucketName(), QueryMethodEvaluationContextProvider.DEFAULT).createQuery();
		} else {
			final PartTree tree = new PartTree(queryMethod.getName(), domainClass);
			query = new N1qlQueryCreator(tree, accessor, mappingCtx, queryMethod, operations.getConverter()).createQuery();
		}
		List<?> all = operations.findByQuery(domainClass).matching(query).all();
		return all;

	}


}
