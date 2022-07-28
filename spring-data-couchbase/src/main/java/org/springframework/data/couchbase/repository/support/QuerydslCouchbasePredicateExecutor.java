/*
 * Copyright 2017-2022 the original author or authors.
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
package org.springframework.data.couchbase.repository.support;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.querydsl.EntityPathResolver;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.querydsl.SimpleEntityPathResolver;
import org.springframework.data.repository.query.FluentQuery;
import org.springframework.data.support.PageableExecutionUtils;
import org.springframework.util.Assert;

import com.querydsl.core.NonUniqueResultException;
import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Predicate;

/**
 * Couchbase-specific {@link QuerydslPredicateExecutor} that allows execution {@link Predicate}s in various forms.
 *
 * @author Michael Reiche
 * @since 5.0
 */
public class QuerydslCouchbasePredicateExecutor<T> extends QuerydslPredicateExecutorSupport<T>
		implements QuerydslPredicateExecutor<T> {

	private final CouchbaseOperations couchbaseOperations;

	/**
	 * Creates a new {@link QuerydslCouchbasePredicateExecutor} for the given {@link CouchbaseEntityInformation} and
	 * {@link CouchbaseOperations}. Uses the {@link SimpleEntityPathResolver} to create an {@link EntityPath} for the
	 * given domain class.
	 *
	 * @param entityInformation must not be {@literal null}.
	 * @param couchbaseOperations must not be {@literal null}.
	 */
	public QuerydslCouchbasePredicateExecutor(CouchbaseEntityInformation<T, ?> entityInformation,
			CouchbaseOperations couchbaseOperations) {
		this(entityInformation, couchbaseOperations, SimpleEntityPathResolver.INSTANCE);
	}

	/**
	 * Creates a new {@link QuerydslCouchbasePredicateExecutor} for the given {@link CouchbaseEntityInformation},
	 * {@linkCouchbaseOperations} and {@link EntityPathResolver}.
	 *
	 * @param entityInformation must not be {@literal null}.
	 * @param couchbaseOperations must not be {@literal null}.
	 * @param resolver must not be {@literal null}.
	 */
	public QuerydslCouchbasePredicateExecutor(CouchbaseEntityInformation<T, ?> entityInformation,
			CouchbaseOperations couchbaseOperations, EntityPathResolver resolver) {
		super(couchbaseOperations.getConverter(), pathBuilderFor(resolver.createPath(entityInformation.getJavaType())),
				entityInformation);
		this.couchbaseOperations = couchbaseOperations;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.QuerydslPredicateExecutor#findById(com.querydsl.core.types.Predicate)
	 */
	@Override
	public Optional<T> findOne(Predicate predicate) {
		Assert.notNull(predicate, "Predicate must not be null!");
		try {
			return Optional.ofNullable(createQueryFor(predicate).fetchOne());
		} catch (NonUniqueResultException ex) {
			throw new IncorrectResultSizeDataAccessException(ex.getMessage(), 1, ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.QuerydslPredicateExecutor#findAll(com.querydsl.core.types.Predicate)
	 */
	@Override
	public List<T> findAll(Predicate predicate) {
		Assert.notNull(predicate, "Predicate must not be null!");
		return createQueryFor(predicate).fetch();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.QuerydslPredicateExecutor#findAll(com.querydsl.core.types.Predicate, com.querydsl.core.types.OrderSpecifier<?>[])
	 */
	@Override
	public List<T> findAll(Predicate predicate, OrderSpecifier<?>... orders) {
		Assert.notNull(predicate, "Predicate must not be null!");
		Assert.notNull(orders, "Order specifiers must not be null!");
		return createQueryFor(predicate).orderBy(orders).fetch();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.QuerydslPredicateExecutor#findAll(com.querydsl.core.types.Predicate, org.springframework.data.domain.Sort)
	 */
	@Override
	public List<T> findAll(Predicate predicate, Sort sort) {
		Assert.notNull(predicate, "Predicate must not be null!");
		Assert.notNull(sort, "Sort must not be null!");
		return applySorting(createQueryFor(predicate), sort).fetch();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.QuerydslPredicateExecutor#findAll(com.querydsl.core.types.OrderSpecifier[])
	 */
	@Override
	public Iterable<T> findAll(OrderSpecifier<?>... orders) {
		Assert.notNull(orders, "Order specifiers must not be null!");
		return createQuery().orderBy(orders).fetch();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.QuerydslPredicateExecutor#findAll(com.querydsl.core.types.Predicate, org.springframework.data.domain.Pageable)
	 */
	@Override
	public Page<T> findAll(Predicate predicate, Pageable pageable) {
		Assert.notNull(predicate, "Predicate must not be null!");
		Assert.notNull(pageable, "Pageable must not be null!");
		SpringDataCouchbaseQuery<T> query = createQueryFor(predicate);
		return PageableExecutionUtils.getPage(applyPagination(query, pageable).fetch(), pageable, query::fetchCount);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.QuerydslPredicateExecutor#count(com.querydsl.core.types.Predicate)
	 */
	@Override
	public long count(Predicate predicate) {
		Assert.notNull(predicate, "Predicate must not be null!");
		return createQueryFor(predicate).fetchCount();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.QuerydslPredicateExecutor#exists(com.querydsl.core.types.Predicate)
	 */
	@Override
	public boolean exists(Predicate predicate) {
		Assert.notNull(predicate, "Predicate must not be null!");
		return createQueryFor(predicate).fetchCount() > 0;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.QuerydslPredicateExecutor#findBy(com.querydsl.core.types.Predicate, java.util.function.Function)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <S extends T, R> R findBy(Predicate predicate,
			Function<FluentQuery.FetchableFluentQuery<S>, R> queryFunction) {
		Assert.notNull(predicate, "Predicate must not be null!");
		Assert.notNull(queryFunction, "Query function must not be null!");
		return queryFunction.apply(new FluentQuerydsl<>(predicate, (Class<S>) typeInformation().getJavaType()));
	}

	/**
	 * Creates a {@link SpringDataCouchbaseQuery} for the given {@link Predicate}.
	 *
	 * @param predicate
	 * @return
	 */
	private SpringDataCouchbaseQuery<T> createQueryFor(Predicate predicate) {
		return createQuery().where(predicate);
	}

	/**
	 * Creates a {@link SpringDataCouchbaseQuery}.
	 *
	 * @return
	 */
	private SpringDataCouchbaseQuery<T> createQuery() {
		return new SpringDataCouchbaseQuery<>(couchbaseOperations, typeInformation().getJavaType());
	}

	/**
	 * Applies the given {@link Pageable} to the given {@link SpringDataCouchbaseQuery}.
	 *
	 * @param query
	 * @param pageable
	 * @return
	 */
	private SpringDataCouchbaseQuery<T> applyPagination(SpringDataCouchbaseQuery<T> query, Pageable pageable) {
		if (pageable.isUnpaged()) {
			return query;
		}
		query = query.offset(pageable.getOffset()).limit(pageable.getPageSize());
		return applySorting(query, pageable.getSort());
	}

	/**
	 * Applies the given {@link Sort} to the given {@link SpringDataCouchbaseQuery}.
	 *
	 * @param query
	 * @param sort
	 * @return
	 */
	private SpringDataCouchbaseQuery<T> applySorting(SpringDataCouchbaseQuery<T> query, Sort sort) {
		toOrderSpecifiers(sort).forEach(query::orderBy);
		return query;
	}

	/**
	 * {@link org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery} using Querydsl
	 * {@link Predicate}.
	 *
	 * @author Mark Paluch
	 * @since 3.3
	 */
	class FluentQuerydsl<T> extends FetchableFluentQuerySupport<Predicate, T> {

		FluentQuerydsl(Predicate predicate, Class<T> resultType) {
			this(predicate, Sort.unsorted(), resultType, Collections.emptyList());
		}

		FluentQuerydsl(Predicate predicate, Sort sort, Class<T> resultType, List<String> fieldsToInclude) {
			super(predicate, sort, resultType, fieldsToInclude);
		}

		@Override
		protected <R> FluentQuerydsl<R> create(Predicate predicate, Sort sort, Class<R> resultType,
				List<String> fieldsToInclude) {
			return new FluentQuerydsl<>(predicate, sort, resultType, fieldsToInclude);
		}

		@Override
		public FetchableFluentQuery sortBy(Sort sort) {
			return null;
		}

		@Override
		public FetchableFluentQuery project(Collection properties) {
			return null;
		}

		@Override
		public FetchableFluentQuery as(Class resultType) {
			return null;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#oneValue()
		 */
		@Override
		public T oneValue() {
			return createQuery().fetchOne();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#firstValue()
		 */
		@Override
		public T firstValue() {
			return createQuery().fetchFirst();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#all()
		 */
		@Override
		public List<T> all() {
			return createQuery().fetch();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#page(org.springframework.data.domain.Pageable)
		 */
		@Override
		public Page<T> page(Pageable pageable) {

			Assert.notNull(pageable, "Pageable must not be null!");

			return createQuery().fetchPage(pageable);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#stream()
		 */
		@Override
		public Stream<T> stream() {
			return createQuery().stream();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#count()
		 */
		@Override
		public long count() {
			return createQuery().fetchCount();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#exists()
		 */
		@Override
		public boolean exists() {
			return count() > 0;
		}

		private SpringDataCouchbaseQuery<T> createQuery() {
			return new SpringDataCouchbaseQuery<>(couchbaseOperations, typeInformation().getJavaType(), getResultType(),
					"collection", this::customize).where(getPredicate());
		}

		private void customize(BasicQuery query) {

			List<String> fieldsToInclude = getFieldsToInclude();
			if (!fieldsToInclude.isEmpty()) {
				Map<String, String> fields = new HashMap();
				fieldsToInclude.forEach(field -> fields.put(field, field));
				query.setProjectionFields(fields);
			}

			if (getSort().isSorted()) {
				query.with(getSort());
			}
		}

	}
}
