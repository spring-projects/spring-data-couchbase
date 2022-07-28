/*
 * Copyright 2012-2022 the original author or authors
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
package org.springframework.data.couchbase.repository.support;

import java.util.List;
import java.util.stream.Stream;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.FluentQuery;

/**
 * Querydsl fluent api
 *
 * @author Michael Reiche
 */
abstract class FetchableFluentQuerySupport<P, T> implements FluentQuery.FetchableFluentQuery<T> {

	private final P predicate;
	private final Sort sort;
	private final Class<T> resultType;
	private final List<String> fieldsToInclude;

	FetchableFluentQuerySupport(P predicate, Sort sort, Class<T> resultType, List<String> fieldsToInclude) {
		this.predicate = predicate;
		this.sort = sort;
		this.resultType = resultType;
		this.fieldsToInclude = fieldsToInclude;
	}

	protected abstract <R> FetchableFluentQuerySupport<P, R> create(P predicate, Sort sort, Class<R> resultType,
			List<String> fieldsToInclude);

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#oneValue()
	 */
	public abstract T oneValue();

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#firstValue()
	 */
	public abstract T firstValue();

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#all()
	 */
	public abstract List<T> all();

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#page(org.springframework.data.domain.Pageable)
	 */
	public abstract Page<T> page(Pageable pageable);

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#stream()
	 */
	public abstract Stream<T> stream();

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#count()
	 */
	public abstract long count();

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#exists()
	 */
	public abstract boolean exists();

	P getPredicate() {
		return predicate;
	}

	Sort getSort() {
		return sort;
	}

	Class<T> getResultType() {
		return resultType;
	}

	List<String> getFieldsToInclude() {
		return fieldsToInclude;
	}
}
