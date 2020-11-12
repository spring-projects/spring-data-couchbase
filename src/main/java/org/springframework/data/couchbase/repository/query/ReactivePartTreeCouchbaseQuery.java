/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * Reactive PartTree {@link RepositoryQuery} implementation for Couchbase. Replaces ReactivePartN1qlBasedQuery
 *
 * @author Michael Reiche
 * @since 4.1
 */
public class ReactivePartTreeCouchbaseQuery extends AbstractReactiveCouchbaseQuery {

	private final PartTree tree;
	private final CouchbaseConverter converter;
	private static final Logger LOG = LoggerFactory.getLogger(ReactivePartTreeCouchbaseQuery.class);

	/**
	 * Creates a new {@link ReactivePartTreeCouchbaseQuery} from the given {@link QueryMethod} and
	 * {@link CouchbaseTemplate}.
	 *
	 * @param method must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 */
	public ReactivePartTreeCouchbaseQuery(ReactiveCouchbaseQueryMethod method, ReactiveCouchbaseOperations operations,
			SpelExpressionParser expressionParser, QueryMethodEvaluationContextProvider evaluationContextProvider) {

		super(method, operations, expressionParser, evaluationContextProvider);
		this.tree = new PartTree(method.getName(), method.getResultProcessor().getReturnedType().getDomainType());
		this.converter = operations.getConverter();
	}

	/**
	 * Return the {@link PartTree} backing the query.
	 *
	 * @return the tree
	 */
	public PartTree getTree() {
		return tree;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.couchbease.repository.query.AbstractCouchbaseQuery#createQuery(org.springframework.data.couchbase.repository.query.ConvertingParameterAccessor, boolean)
	 */
	@Override
	protected Query createQuery(ParametersParameterAccessor accessor) {

		N1qlQueryCreator creator = new N1qlQueryCreator(tree, accessor, getQueryMethod(), converter);
		Query query = creator.createQuery();

		if (tree.isLimiting()) {
			query.limit(tree.getMaxResults());
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Created query {} for * fields.", query.export());
		}
		return query;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.couchbase.repository.query.AbstractReactiveCouchbaseQuery#createCountQuery(org.springframework.data.couchbase.repository.query.ConvertingParameterAccessor)
	 */
	@Override
	protected Query createCountQuery(ParametersParameterAccessor accessor) {
		Query query = new N1qlQueryCreator(tree, accessor, getQueryMethod(), converter).createQuery();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Created query {} for * fields.", query.export());
		}
		return query;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.couchbase.repository.query.AbstractReactiveCouchbaseQuery#isLimiting()
	 */
	@Override
	protected boolean isLimiting() {
		return tree.isLimiting();
	}
}
