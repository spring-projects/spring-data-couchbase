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

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * {@link RepositoryQuery} implementation for Couchbase.
 *
 * @author Michael Reiche
 * @since 4.1
 */
public class PartTreeCouchbaseQuery extends AbstractCouchbaseQuery {

	private final PartTree tree;
	// private final boolean isGeoNearQuery;
	private final MappingContext<?, CouchbasePersistentProperty> context;
	private final ResultProcessor processor;
	private final CouchbaseConverter converter;

	/**
	 * Creates a new {@link PartTreeCouchbaseQuery} from the given {@link QueryMethod} and {@link CouchbaseTemplate}.
	 *
	 * @param method must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 */
	public PartTreeCouchbaseQuery(CouchbaseQueryMethod method, CouchbaseOperations operations,
			SpelExpressionParser expressionParser, QueryMethodEvaluationContextProvider evaluationContextProvider) {

		super(method, operations, expressionParser, evaluationContextProvider);

		this.processor = method.getResultProcessor();
		for (PartTree.OrPart parts : this.tree = new PartTree(method.getName(),
				processor.getReturnedType().getDomainType())) {}

		this.context = operations.getConverter().getMappingContext();
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
	 * @see org.springframework.data.couchbase.repository.query.AbstractCouchbaseQuery#createQuery(org.springframework.data.couchbase.repository.query.ConvertingParameterAccessor, boolean)
	 */
	@Override
	protected Query createQuery(ParametersParameterAccessor accessor) {

		N1qlQueryCreator creator = new N1qlQueryCreator(tree, accessor, getQueryMethod(), converter);
		Query query = creator.createQuery();

		if (tree.isLimiting()) {
			query.limit(tree.getMaxResults());
		}
		return query;

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.couchbase.repository.query.AbstractCouchbaseQuery#createCountQuery(org.springframework.data.couchbase.repository.query.ConvertingParameterAccessor)
	 */
	@Override
	protected Query createCountQuery(ParametersParameterAccessor accessor) {
		return new N1qlQueryCreator(tree, accessor, getQueryMethod(), converter).createQuery();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.couchbase.repository.query.AbstractCouchbaseQuery#isCountQuery()
	 */
	@Override
	protected boolean isCountQuery() {
		return tree.isCountProjection();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.couchbase.repository.query.AbstractCouchbaseQuery#isExistsQuery()
	 */
	@Override
	protected boolean isExistsQuery() {
		return tree.isExistsProjection();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.couchbase.repository.query.AbstractCouchbaseQuery#isDeleteQuery()
	 */
	@Override
	protected boolean isDeleteQuery() {
		return tree.isDelete();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.couchbase.repository.query.AbstractCouchbaseQuery#isLimiting()
	 */
	@Override
	protected boolean isLimiting() {
		return tree.isLimiting();
	}
}
