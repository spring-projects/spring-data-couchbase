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

import static org.springframework.data.couchbase.core.query.N1QLExpression.x;
import static org.springframework.data.couchbase.core.query.QueryCriteria.*;

import java.util.ArrayList;
import java.util.Iterator;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
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

	public StringN1qlQueryCreator(
			String statement,
			final ParameterAccessor accessor,
			final MappingContext<?, CouchbasePersistentProperty> context,
			QueryMethod queryMethod,
			CouchbaseConverter couchbaseConverter,
			String bucketName,
			QueryMethodEvaluationContextProvider evaluationContextProvider) {

		// AbstractQueryCreator needs a PartTree, so we give it a dummy one.
		// The resulting criteria will be discarded
		super(new PartTree("dummy", (new Object() {String dummy;	}).getClass()), accessor);
		this.accessor = accessor;
		this.context = context;
		this.queryMethod = queryMethod;
		this.couchbaseConverter = couchbaseConverter;
		this.evaluationContextProvider = evaluationContextProvider;
		this.queryParser = new StringBasedN1qlQueryParser(statement, queryMethod, bucketName,
				couchbaseConverter, getTypeField(), getTypeValue());
		this.parser = SPEL_PARSER;

	}

	protected QueryMethod getQueryMethod() { return queryMethod;}

	protected String getTypeField() {
		return couchbaseConverter.getTypeKey();
	}

	protected Class<?> getTypeValue() {
		return getQueryMethod().getEntityInformation().getJavaType();
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
		// The criteria will be the dummy from PartTree and discarded
		// in ReactiveFindByQueryOperationSupport.assembleEntityQuery()
		Query q = (criteria == null ? new Query() : new Query().addCriteria(criteria)).with(sort);
		q.clearCriteria();
		ResultProcessor processor = this.queryMethod.getResultProcessor().withDynamicProjection(accessor);
		ReturnedType returnedType = processor.getReturnedType();
		N1QLExpression parsedExpression = getExpression(accessor, getParameters(), returnedType);
		q.setInlineN1qlQuery(parsedExpression.toString());
		q.setParameters( queryParser.getPlaceholderValues(accessor) );
		return q;
	}

	private QueryCriteria from(final Part part, final CouchbasePersistentProperty property, final QueryCriteria criteria,
			final Iterator<Object> parameters) {

		final Part.Type type = part.getType();
		switch (type) {
		case SIMPLE_PROPERTY:
			return criteria; //.eq(parameters.next()); // this will be the dummy from PartTree
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
	private Object[] getParameters() {
		ArrayList<Object> params = new ArrayList<>();
		for (Iterator<Object> it = accessor.iterator(); it.hasNext(); ) {
			Object o = it.next();
			params.add(o);
		}
		return params.toArray();
	}
}


