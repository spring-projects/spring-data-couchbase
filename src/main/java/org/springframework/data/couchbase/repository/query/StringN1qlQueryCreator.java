/*
 * Copyright 2021-2025 the original author or authors
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

import static org.springframework.data.couchbase.core.query.N1QLExpression.*;
import static org.springframework.data.couchbase.core.query.QueryCriteria.*;

import java.util.Iterator;
import java.util.Optional;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.core.query.StringQuery;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * @author Michael Reiche
 * @author Mauro Monti
 */
public class StringN1qlQueryCreator extends AbstractQueryCreator<Query, QueryCriteria> {

	// everything we need in the StringQuery such that we can doParse() later when we have the scope and collection
	private final ParameterAccessor accessor;
	private final MappingContext<?, CouchbasePersistentProperty> context;
	private final ValueExpressionDelegate valueExpressionDelegate;
	private final CouchbaseQueryMethod queryMethod;
	private final CouchbaseConverter couchbaseConverter;
	private final String queryString;

	public StringN1qlQueryCreator(final ParameterAccessor accessor, CouchbaseQueryMethod queryMethod,
			CouchbaseConverter couchbaseConverter,
			ValueExpressionDelegate valueExpressionDelegate, NamedQueries namedQueries) {

		// AbstractQueryCreator needs a PartTree, so we give it a dummy one.
		// The resulting dummy criteria will not be included in the Query
		// by {@link #complete((QueryCriteria criteria, Sort sort)) complete}
		super(new PartTree("dummy", (new Object() {
			String dummy;
		}).getClass()), accessor);
		this.accessor = accessor;
		this.context = couchbaseConverter.getMappingContext();
		this.queryMethod = queryMethod;
		this.couchbaseConverter = couchbaseConverter;
		this.valueExpressionDelegate = valueExpressionDelegate;
		final String namedQueryName = queryMethod.getNamedQueryName();
		String qString;
		if (queryMethod.hasInlineN1qlQuery()) {
			qString = queryMethod.getInlineN1qlQuery();
		} else if (namedQueries.hasQuery(namedQueryName)) {
			qString = namedQueries.getQuery(namedQueryName);
		} else {
			throw new IllegalArgumentException("query has no inline Query or named Query not found");
		}
		// Save the query string to be parsed later after we have the scope and collection to be used in the query
		this.queryString = qString;
	}

	protected QueryMethod getQueryMethod() {
		return queryMethod;
	}

	protected String getTypeField() {
		return couchbaseConverter.getTypeKey();
	}

	protected Class getType() {
		return getQueryMethod().getEntityInformation().getJavaType();
	}

	@Override
	protected QueryCriteria create(final Part part, final Iterator<Object> iterator) {
		PersistentPropertyPath<CouchbasePersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		CouchbasePersistentProperty property = path.getLeafProperty();
		return from(part, property, where(x(path.toDotPath())), iterator);
	}

	@Override
	public Query createQuery() {
		Query q = this.createQuery((Optional.of(this.accessor).map(ParameterAccessor::getSort).orElse(Sort.unsorted())));
		Pageable pageable = accessor.getPageable();
		if (pageable.isPaged()) {
			q.skip(pageable.getOffset());
			q.limit(pageable.getPageSize());
		}
		return q;
	}

	@Override
	protected QueryCriteria and(final Part part, final QueryCriteria base, final Iterator<Object> iterator) {
		if (base == null) {
			return create(part, iterator);
		}

		PersistentPropertyPath<CouchbasePersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		CouchbasePersistentProperty property = path.getLeafProperty();

		return from(part, property, base.and(x(path.toDotPath())), iterator);
	}

	@Override
	protected QueryCriteria or(QueryCriteria base, QueryCriteria criteria) {
		return base.or(criteria);
	}

	@Override
	protected Query complete(QueryCriteria criteria, Sort sort) {
		// everything we need in the StringQuery such that we can doParse() later when we have the scope and collection
		Query q = new StringQuery(queryMethod, queryString, valueExpressionDelegate, accessor)
				.with(sort);
		return q;
	}

	private QueryCriteria from(final Part part, final CouchbasePersistentProperty property, final QueryCriteria criteria,
			final Iterator<Object> parameters) {

		final Part.Type type = part.getType();
		switch (type) {
			case SIMPLE_PROPERTY:
				return criteria; // this will be the dummy from PartTree
			default:
				throw new IllegalArgumentException("Unsupported keyword!");
		}
	}

}
