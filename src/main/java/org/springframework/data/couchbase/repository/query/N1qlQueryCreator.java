/*
 * Copyright 2012-2025 the original author or authors
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
import java.util.Optional;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentEntity;
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

	public static final String META_ID_PROPERTY = "id";
	public static final String META_CAS_PROPERTY = "cas";
	public static final String META_EXPIRATION_PROPERTY = "expiration";

	private final PartTree tree;
	private final ParameterAccessor accessor;
	private final MappingContext<?, CouchbasePersistentProperty> context;
	private final QueryMethod queryMethod;
	private final CouchbaseConverter converter;
	private final String bucketName;
	private final PersistentEntity entity;

	public N1qlQueryCreator(final PartTree tree, final ParameterAccessor accessor, final QueryMethod queryMethod,
			final CouchbaseConverter converter, final String bucketName) {
		super(tree, accessor);
		this.tree = tree;
		this.accessor = accessor;
		this.context = converter.getMappingContext();
		this.queryMethod = queryMethod;
		this.converter = converter;
		this.bucketName = bucketName;
		this.entity = converter.getMappingContext().getPersistentEntity(queryMethod.getEntityInformation().getJavaType());
	}

	@Override
	protected QueryCriteria create(final Part part, final Iterator<Object> iterator) {
		PersistentPropertyPath<CouchbasePersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		CouchbasePersistentProperty property = path.getLeafProperty();
		return from(part, property, where(addMetaIfRequired(null, path, property, entity)), iterator);
	}

	@Override
	public Query createQuery() {
		Query q = this.createQuery((Optional.of(this.accessor).map(ParameterAccessor::getSort).orElse(Sort.unsorted())));
		Pageable pageable = accessor.getPageable();
		if (pageable.isPaged()) {
			q.skip(pageable.getOffset());
			q.limit(pageable.getPageSize());
		}
		q.distinct(tree.isDistinct());
		return q;
	}

	static Converter<? super CouchbasePersistentProperty, String> cvtr = new MyConverter();

	static class MyConverter implements Converter<CouchbasePersistentProperty, String> {
		@Override
		public String convert(CouchbasePersistentProperty source) {
			return new StringBuilder(source.getFieldName().length() + 2).append("`").append(source.getFieldName()).append("`")
					.toString();
		}
	}

	@Override
	protected QueryCriteria and(final Part part, final QueryCriteria base, final Iterator<Object> iterator) {
		if (base == null) {
			return create(part, iterator);
		}

		PersistentPropertyPath<CouchbasePersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		CouchbasePersistentProperty property = path.getLeafProperty();

		return from(part, property, base.and(addMetaIfRequired(bucketName, path, property, entity)), iterator);
	}

	@Override
	protected QueryCriteria or(QueryCriteria base, QueryCriteria criteria) {
		return base.or(criteria);
	}

	@Override
	protected Query complete(QueryCriteria criteria, Sort sort) {
		Query q = (criteria == null ? new Query() : new Query().addCriteria(criteria)).with(sort);
		return q;
	}

	private QueryCriteria from(final Part part, final CouchbasePersistentProperty property, final QueryCriteria criteria,
			final Iterator<Object> parameters) {
		// deal with ignore case
		PersistentPropertyPath<CouchbasePersistentProperty> path = N1qlUtils.getPathWithAlternativeFieldNames(converter,
				part.getProperty());

		// get the whole doted path with fieldNames instead of potentially wrong propNames
		String fieldNamePath = N1qlUtils.getDottedPathWithAlternativeFieldNames(path);

		boolean ignoreCase = false;
		Class<?> leafType = converter.getWriteClassFor(path.getLeafProperty().getType());
		boolean isString = leafType == String.class;
		if (part.shouldIgnoreCase() == Part.IgnoreCaseType.WHEN_POSSIBLE) {
			ignoreCase = isString;
		} else if (part.shouldIgnoreCase() == Part.IgnoreCaseType.ALWAYS) {
			if (!isString) {
				throw new IllegalArgumentException(
						String.format("Part %s must be of type String but was %s", fieldNamePath, leafType));
			}
			ignoreCase = true;
		}
		final Part.Type type = part.getType();
		/*
		    NEAR(new String[]{"IsNear", "Near"}),
		 */
		switch (type) {
			case GREATER_THAN:
			case AFTER:
				return criteria.gt(ignoreCase, parameters.next());
			case GREATER_THAN_EQUAL:
				return criteria.gte(ignoreCase, parameters.next());
			case LESS_THAN:
			case BEFORE:
				return criteria.lt(ignoreCase, parameters.next());
			case LESS_THAN_EQUAL:
				return criteria.lte(ignoreCase, parameters.next());
			case SIMPLE_PROPERTY:
				return criteria.eq(ignoreCase, parameters.next());
			case NEGATING_SIMPLE_PROPERTY:
				return criteria.ne(ignoreCase, parameters.next());
			case CONTAINING:
				return criteria.containing(ignoreCase, parameters.next());
			case NOT_CONTAINING:
				return criteria.notContaining(ignoreCase, parameters.next());
			case STARTING_WITH:
				return criteria.startingWith(ignoreCase, parameters.next());
			case ENDING_WITH:
				return criteria.endingWith(ignoreCase, parameters.next());
			case LIKE:
				return criteria.like(ignoreCase, parameters.next());
			case NOT_LIKE:
				return criteria.notLike(ignoreCase, parameters.next());
			case WITHIN:
				return criteria.within(ignoreCase, parameters.next());
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
				return criteria.between(ignoreCase, parameters.next(), parameters.next());
			case IN:
				return criteria.in(ignoreCase, new Object[] { parameters.next() });
			case NOT_IN:
				return criteria.notIn(ignoreCase, new Object[] { parameters.next() });
			case TRUE:
				return criteria.TRUE();
			case FALSE:
				return criteria.FALSE();
			default:
				throw new IllegalArgumentException("Unsupported keyword!");
		}
	}

	/**
	 * Translate meta-fields to META(bucketName).id, cas, expiry.<br>
	 * If bucketName is null, META().id etc, <br>
	 * If not a meta-field, just create the corresponding path
	 *
	 * @param bucketName
	 * @param persistentPropertyPath
	 * @param property
	 * @param entity
	 * @return N1QLExpression
	 */
	public static N1QLExpression addMetaIfRequired(String bucketName,
			final PersistentPropertyPath<CouchbasePersistentProperty> persistentPropertyPath,
			final CouchbasePersistentProperty property, final PersistentEntity entity) {
		if (entity != null && property == entity.getIdProperty()) {
			return path(meta(bucketName != null ? i(bucketName) : x("")), i(META_ID_PROPERTY));
		}
		if (property == entity.getVersionProperty()) {
			return path(meta(bucketName != null ? i(bucketName) : x("")), i(META_CAS_PROPERTY));
		}
		if (property.isExpirationProperty()) {
			return path(meta(bucketName != null ? i(bucketName) : x("")), i(META_EXPIRATION_PROPERTY));
		}
		return x(persistentPropertyPath.toDotPath(cvtr));
	}

}
