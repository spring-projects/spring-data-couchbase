/*
 * Copyright 2020-present the original author or authors.
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
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * Query to use a plain JSON String to create the {@link Query} to actually execute.
 *
 * @author Michael Reiche
 * @since 4.1
 */
public class StringBasedCouchbaseQuery extends AbstractCouchbaseQuery {

	private static final String COUNT_EXISTS_AND_DELETE = "Manually defined query for %s cannot be a count and exists or delete query at the same time!";
	private static final Logger LOG = LoggerFactory.getLogger(StringBasedCouchbaseQuery.class);

	private final ValueExpressionDelegate valueExpressionDelegate;
	private final NamedQueries namedQueries;

	/**
	 * Creates a new {@link StringBasedCouchbaseQuery} for the given {@link String}, {@link CouchbaseQueryMethod},
	 * {@link CouchbaseOperations}, {@link SpelExpressionParser} and {@link ValueExpressionDelegate}.
	 *
	 * @param method must not be {@literal null}.
	 * @param couchbaseOperations must not be {@literal null}.
	 * @param valueExpressionDelegate must not be {@literal null}.
	 * @param namedQueries must not be {@literal null}.
	 */
	public StringBasedCouchbaseQuery(CouchbaseQueryMethod method, CouchbaseOperations couchbaseOperations,
			ValueExpressionDelegate valueExpressionDelegate,
			NamedQueries namedQueries) {

		super(method, couchbaseOperations, valueExpressionDelegate);

		this.valueExpressionDelegate = valueExpressionDelegate;

		if (hasAmbiguousProjectionFlags(isCountQuery(), isExistsQuery(), isDeleteQuery())) {
			throw new IllegalArgumentException(String.format(COUNT_EXISTS_AND_DELETE, method));
		}
		this.namedQueries = namedQueries;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.couchbase.repository.query.AbstractCouchbaseQuery#createQuery(org.springframework.data.couchbase.repository.query.ConvertingParameterAccessor)
	 */
	@Override
	protected Query createQuery(ParametersParameterAccessor accessor) {

		StringN1qlQueryCreator creator = new StringN1qlQueryCreator(accessor, getQueryMethod(),
				getOperations().getConverter(), valueExpressionDelegate, namedQueries);
		Query query = creator.createQuery();

		if (LOG.isTraceEnabled()) {
			LOG.trace("Created query " + query.export());
		}

		return query;
	}

	@Override
	protected Query createCountQuery(ParametersParameterAccessor accessor) {
		return applyQueryMetaAttributesIfPresent(createQuery(accessor), null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.couchbase.repository.query.AbstractCouchbaseQuery#isLimiting()
	 */
	@Override
	protected boolean isLimiting() {
		return false; // not yet implemented
	}

}
