/*
 * Copyright 2020 the original author or authors
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

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.core.query.StringQuery;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.util.Iterator;

import static org.springframework.data.couchbase.core.query.QueryCriteria.where;

/**
 * @author Michael Reiche
 */
public class StringN1qlQueryCreator extends AbstractQueryCreator<Query, QueryCriteria> {

	private final ParameterAccessor accessor;
	private final MappingContext<?, CouchbasePersistentProperty> context;
	private final SpelExpressionParser parser;
	private final QueryMethodEvaluationContextProvider evaluationContextProvider;
	private final StringBasedN1qlQueryParser queryParser;
	private final QueryMethod queryMethod;
	private final CouchbaseConverter couchbaseConverter;
	private final N1QLExpression parsedExpression;

	public StringN1qlQueryCreator(final ParameterAccessor accessor, CouchbaseQueryMethod queryMethod,
			CouchbaseConverter couchbaseConverter, String bucketName, SpelExpressionParser spelExpressionParser,
			QueryMethodEvaluationContextProvider evaluationContextProvider, NamedQueries namedQueries) {

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
		this.evaluationContextProvider = evaluationContextProvider;
		final String namedQueryName = queryMethod.getNamedQueryName();
		String queryString;
		if (queryMethod.hasInlineN1qlQuery()) {
			queryString = queryMethod.getInlineN1qlQuery();
		} else if (namedQueries.hasQuery(namedQueryName)) {
			queryString = namedQueries.getQuery(namedQueryName);
		} else {
			throw new IllegalArgumentException("query has no inline Query or named Query not found");
		}
		this.queryParser = new StringBasedN1qlQueryParser(queryString, queryMethod, bucketName, couchbaseConverter,
				getTypeField(), getTypeValue(), accessor, spelExpressionParser, evaluationContextProvider);
		this.parser = spelExpressionParser;
		this.parsedExpression = this.queryParser.parsedExpression;
	}

	protected QueryMethod getQueryMethod() {
		return queryMethod;
	}

	protected String getTypeField() {
		return couchbaseConverter.getTypeKey();
	}

	protected String getTypeValue() {
		return getQueryMethod().getEntityInformation().getJavaType().getName();
	}

	@Override
	protected QueryCriteria create(final Part part, final Iterator<Object> iterator) {
		PersistentPropertyPath<CouchbasePersistentProperty> path = context.getPersistentPropertyPath(
				part.getProperty());
		CouchbasePersistentProperty property = path.getLeafProperty();
		return from(part, property, where(path.toDotPath()), iterator);
	}

	@Override
	protected QueryCriteria and(final Part part, final QueryCriteria base, final Iterator<Object> iterator) {
		if (base == null) {
			return create(part, iterator);
		}

		PersistentPropertyPath<CouchbasePersistentProperty> path = context.getPersistentPropertyPath(
				part.getProperty());
		CouchbasePersistentProperty property = path.getLeafProperty();

		return from(part, property, base.and(path.toDotPath()), iterator);
	}

	@Override
	protected QueryCriteria or(QueryCriteria base, QueryCriteria criteria) {
		return base.or(criteria);
	}

	@Override
	protected Query complete(QueryCriteria criteria, Sort sort) {
		Query q = new StringQuery(parsedExpression.toString()).with(sort);
		JsonValue params = queryParser.getPlaceholderValues(accessor);
		if (params instanceof JsonArray) {
			q.setPositionalParameters((JsonArray) params);
		} else {
			q.setNamedParameters((JsonObject) params);
		}
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
