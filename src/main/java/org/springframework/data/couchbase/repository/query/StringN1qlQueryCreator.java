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

import static org.springframework.data.couchbase.core.query.N1QLExpression.x;
import static org.springframework.data.couchbase.core.query.QueryCriteria.*;

import java.util.ArrayList;
import java.util.Iterator;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.core.query.StringQuery;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.query.*;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;

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
	private static final SpelExpressionParser SPEL_PARSER = new SpelExpressionParser();

	public StringN1qlQueryCreator(final ParameterAccessor accessor, CouchbaseQueryMethod queryMethod,
			CouchbaseConverter couchbaseConverter, String bucketName,
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
				getTypeField(), getTypeValue());
		this.parser = SPEL_PARSER;
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
		PersistentPropertyPath<CouchbasePersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		CouchbasePersistentProperty property = path.getLeafProperty();
		return from(part, property, where(path.toDotPath()), iterator);
	}

	@Override
	protected QueryCriteria and(final Part part, final QueryCriteria base, final Iterator<Object> iterator) {
		if (base == null) {
			return create(part, iterator);
		}

		PersistentPropertyPath<CouchbasePersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		CouchbasePersistentProperty property = path.getLeafProperty();

		return from(part, property, base.and(path.toDotPath()), iterator);
	}

	@Override
	protected QueryCriteria or(QueryCriteria base, QueryCriteria criteria) {
		return base.or(criteria);
	}

	@Override
	protected Query complete(QueryCriteria criteria, Sort sort) {
		N1QLExpression parsedExpression = getExpression(accessor, getParameters(accessor), null /* returnedType */);
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
				return criteria; // .eq(parameters.next()); // this will be the dummy from PartTree
			default:
				throw new IllegalArgumentException("Unsupported keyword!");
		}
	}

	// copied from StringN1qlBasedQuery
	private N1QLExpression getExpression(ParameterAccessor accessor, Object[] runtimeParameters,
			ReturnedType returnedType) {
		EvaluationContext evaluationContext = evaluationContextProvider
				.getEvaluationContext(getQueryMethod().getParameters(), runtimeParameters);
		N1QLExpression parsedStatement = x(this.queryParser.doParse(parser, evaluationContext, false));

		Sort sort = accessor.getSort();
		if (sort.isSorted()) {
			N1QLExpression[] cbSorts = N1qlUtils.createSort(sort);
			parsedStatement = parsedStatement.orderBy(cbSorts);
		}
		if (queryMethod.isPageQuery()) {
			Pageable pageable = accessor.getPageable();
			Assert.notNull(pageable, "Pageable must not be null!");
			parsedStatement = parsedStatement.limit(pageable.getPageSize()).offset(Math.toIntExact(pageable.getOffset()));
		} else if (queryMethod.isSliceQuery()) {
			Pageable pageable = accessor.getPageable();
			Assert.notNull(pageable, "Pageable must not be null!");
			parsedStatement = parsedStatement.limit(pageable.getPageSize() + 1).offset(Math.toIntExact(pageable.getOffset()));
		}
		return parsedStatement;
	}

	// getExpression() could do this itself, but pass as an arg to be consistent with StringN1qlBasedQuery
	private static Object[] getParameters(ParameterAccessor accessor) {
		ArrayList<Object> params = new ArrayList<>();
		for (Object o : accessor) {
			params.add(o);
		}
		return params.toArray();
	}
}
