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
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;

/**
 * Query to use a plain JSON String to create the {@link Query} to actually execute.
 *
 * @author Michael Reiche
 * @since 4.1
 */
public class ReactiveStringBasedCouchbaseQuery extends AbstractReactiveCouchbaseQuery {

	private static final String COUNT_EXISTS_AND_DELETE = "Manually defined query for %s cannot be a count and exists or delete query at the same time!";
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveStringBasedCouchbaseQuery.class);

	private final SpelExpressionParser expressionParser;
	private final QueryMethodEvaluationContextProvider evaluationContextProvider;
	private final NamedQueries namedQueries;

	/**
	 * Creates a new {@link ReactiveStringBasedCouchbaseQuery} for the given {@link String}, {@link CouchbaseQueryMethod},
	 * {@link ReactiveCouchbaseOperations}, {@link SpelExpressionParser} and {@link QueryMethodEvaluationContextProvider}.
	 *
	 * @param method must not be {@literal null}.
	 * @param couchbaseOperations must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 * @param namedQueries must not be {@literal null}.
	 */
	public ReactiveStringBasedCouchbaseQuery(ReactiveCouchbaseQueryMethod method,
			ReactiveCouchbaseOperations couchbaseOperations, SpelExpressionParser expressionParser,
			QueryMethodEvaluationContextProvider evaluationContextProvider, NamedQueries namedQueries) {

		super(method, couchbaseOperations, expressionParser, evaluationContextProvider);

		Assert.notNull(expressionParser, "SpelExpressionParser must not be null!");

		this.expressionParser = expressionParser;
		this.evaluationContextProvider = evaluationContextProvider;

		if (hasAmbiguousProjectionFlags(isCountQuery(), isExistsQuery(), isDeleteQuery())) {
			throw new IllegalArgumentException(String.format(COUNT_EXISTS_AND_DELETE, method));
		}

		this.namedQueries = namedQueries;

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.couchbase.repository.query.AbstractReactiveCouchbaseQuery#createQuery(org.springframework.data.couchbase.repository.query.ConvertingParameterAccessor)
	 */
	@Override
	protected Query createQuery(ParametersParameterAccessor accessor) {

		StringN1qlQueryCreator creator = new StringN1qlQueryCreator(accessor, getQueryMethod(),
				getOperations().getConverter(), getOperations().getBucketName(), expressionParser, evaluationContextProvider,
				namedQueries);
		Query query = creator.createQuery();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Created query " + query.export());
		}

		return query;
	}

	@Override
	protected Query createCountQuery(ParametersParameterAccessor accessor) {
		return applyQueryMetaAttributesWhenPresent(createQuery(accessor));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.couchbase.repository.query.AbstractReactiveCouchbaseQuery#isLimiting()
	 */
	@Override
	protected boolean isLimiting() {
		return false; // not yet implemented
	}

}
