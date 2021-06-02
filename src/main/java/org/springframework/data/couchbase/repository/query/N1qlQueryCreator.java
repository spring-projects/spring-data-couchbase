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

import static org.springframework.data.couchbase.core.query.N1QLExpression.i;
import static org.springframework.data.couchbase.core.query.N1QLExpression.meta;
import static org.springframework.data.couchbase.core.query.N1QLExpression.path;
import static org.springframework.data.couchbase.core.query.N1QLExpression.x;
import static org.springframework.data.couchbase.core.query.QueryCriteria.where;

import java.util.Iterator;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Mauro Monti
 */
public class N1qlQueryCreator extends AbstractQueryCreator<Query, QueryCriteria> {

	private static final String META_ID_PROPERTY = "id";
	private static final String META_CAS_PROPERTY = "cas";

	private final ParameterAccessor accessor;
	private final MappingContext<?, CouchbasePersistentProperty> context;
	private final QueryMethod queryMethod;
	private final CouchbaseConverter converter;
	private final String bucketName;

	public N1qlQueryCreator(final PartTree tree, final ParameterAccessor accessor, final QueryMethod queryMethod,
			final CouchbaseConverter converter, final String bucketName) {
		super(tree, accessor);
		this.accessor = accessor;
		this.context = converter.getMappingContext();
		this.queryMethod = queryMethod;
		this.converter = converter;
		this.bucketName = bucketName;
	}

	@Override
	protected QueryCriteria create(final Part part, final Iterator<Object> iterator) {
		PersistentPropertyPath<CouchbasePersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		CouchbasePersistentProperty property = path.getLeafProperty();
		return from(part, property, where(addMetaIfRequired(path, property)), iterator);
	}

	static Converter<? super CouchbasePersistentProperty, String> cvtr = new MyConverter();

	static class MyConverter implements Converter<CouchbasePersistentProperty, String> {
		@Override
		public String convert(CouchbasePersistentProperty source) {
			if (source.isIdProperty()) {
				return "META().id";
			} else if (source.isVersionProperty()) {
				return "META().cas";
			} else if (source.isExpirationProperty()) {
				return "META().expiration";
			} else {
				return new StringBuilder(source.getFieldName().length() + 2).append('`').append(source.getFieldName())
						.append('`').toString();
			}
		}
	}

	@Override
	protected QueryCriteria and(final Part part, final QueryCriteria base, final Iterator<Object> iterator) {
		if (base == null) {
			return create(part, iterator);
		}

		PersistentPropertyPath<CouchbasePersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		CouchbasePersistentProperty property = path.getLeafProperty();

		return from(part, property, base.and(addMetaIfRequired(path, property)), iterator);
	}

	@Override
	protected QueryCriteria or(QueryCriteria base, QueryCriteria criteria) {
		return base.or(criteria);
	}

	@Override
	protected Query complete(QueryCriteria criteria, Sort sort) {
		return (criteria == null ? new Query() : new Query().addCriteria(criteria)).with(sort);
	}

	private QueryCriteria from(final Part part, final CouchbasePersistentProperty property, final QueryCriteria criteria,
			final Iterator<Object> parameters) {

		final Part.Type type = part.getType();
		/*
		    NEAR(new String[]{"IsNear", "Near"}),
		 */
		switch (type) {
			case GREATER_THAN:
			case AFTER:
				return criteria.gt(parameters.next());
			case GREATER_THAN_EQUAL:
				return criteria.gte(parameters.next());
			case LESS_THAN:
			case BEFORE:
				return criteria.lt(parameters.next());
			case LESS_THAN_EQUAL:
				return criteria.lte(parameters.next());
			case SIMPLE_PROPERTY:
				return criteria.eq(parameters.next());
			case NEGATING_SIMPLE_PROPERTY:
				return criteria.ne(parameters.next());
			case CONTAINING:
				return criteria.containing(parameters.next());
			case NOT_CONTAINING:
				return criteria.notContaining(parameters.next());
			case STARTING_WITH:
				return criteria.startingWith(parameters.next());
			case ENDING_WITH:
				return criteria.endingWith(parameters.next());
			case LIKE:
				return criteria.like(parameters.next());
			case NOT_LIKE:
				return criteria.notLike(parameters.next());
			case WITHIN:
				return criteria.within(parameters.next());
			case IS_NULL:
				return criteria.isNull(/*parameters.next()*/);
			case IS_NOT_NULL:
				return criteria.isNotNull(/*parameters.next()*/);
			case IS_EMPTY:
				return criteria.isNotValued(/*parameters.next()*/);
			case IS_NOT_EMPTY:
				return criteria.isValued(/*parameters.next()*/);
			case EXISTS:
				return criteria.isNotMissing(/*parameters.next()*/);
			case REGEX:
				return criteria.regex(parameters.next());
			case BETWEEN:
				return criteria.between(parameters.next(), parameters.next());
			case IN:
				return criteria.in(parameters.next());
			case NOT_IN:
				return criteria.notIn(parameters.next());
			case TRUE:
				return criteria.TRUE();
			case FALSE:
				return criteria.FALSE();
			default:
				throw new IllegalArgumentException("Unsupported keyword!");
		}
	}

	private N1QLExpression addMetaIfRequired(
			final PersistentPropertyPath<CouchbasePersistentProperty> persistentPropertyPath,
			final CouchbasePersistentProperty property) {
		if (property.isIdProperty()) {
			return path(meta(i(bucketName)), i(META_ID_PROPERTY));
		}
		if (property.isVersionProperty()) {
			return path(meta(i(bucketName)), i(META_CAS_PROPERTY));
		}
		return x(persistentPropertyPath.toDotPath(cvtr));
	}

}
