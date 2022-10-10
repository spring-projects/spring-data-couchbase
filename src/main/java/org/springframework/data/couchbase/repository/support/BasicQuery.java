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

import static org.springframework.util.ObjectUtils.nullSafeEquals;
import static org.springframework.util.ObjectUtils.nullSafeHashCode;

import java.util.Map;

import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteriaDefinition;
import org.springframework.data.domain.Sort;
import org.springframework.util.Assert;

/**
 * BasicQuery for Querydsl
 *
 * @author Michael Reiche
 */
public class BasicQuery extends Query {

	Map<String, String> projectionFields;

	/**
	 * Create a new {@link BasicQuery} given a query {@link CouchbaseDocument} and field specification
	 * {@link CouchbaseDocument}.
	 *
	 * @param query must not be {@literal null}.
	 * @param projectionFields must not be {@literal null}.
	 * @throws IllegalArgumentException when {@code sortObject} or {@code fieldsObject} is {@literal null}.
	 */
	public BasicQuery(Query query, Map<String, String> projectionFields) {
		super(query);
		Assert.notNull(projectionFields, "Field document must not be null");
		this.projectionFields = projectionFields;
	}

	public BasicQuery(QueryCriteriaDefinition criteria, Map<String, String> projectionFields) {
		addCriteria(criteria);
		this.projectionFields = projectionFields;
	}

	/**
	 * Set the sort {@link CouchbaseDocument}.
	 *
	 * @param sort must not be {@literal null}.
	 * @throws IllegalArgumentException when {@code sortObject} is {@literal null}.
	 */
	public void setSort(Sort sort) {
		Assert.notNull(sort, "Sort must not be null");
		with(sort);
	}

	/*
	 * indicates if the query is sorted
	 */
	public boolean isSorted() {
		return sort != null && sort != Sort.unsorted();
	}

	/**
	 * Set the fields (projection) {@link CouchbaseDocument}.
	 *
	 * @param projectionFields must not be {@literal null}.
	 * @throws IllegalArgumentException when {@code fieldsObject} is {@literal null}.
	 */
	public void setProjectionFields(Map<String, String> projectionFields) {
		Assert.notNull(projectionFields, "Field document must not be null");
		this.projectionFields = projectionFields;
	}

	/*
	 * (non-Javadoc)
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof BasicQuery)) {
			return false;
		}
		BasicQuery that = (BasicQuery) o;
		return querySettingsEquals(that) && //
				nullSafeEquals(projectionFields, that.projectionFields) && //
				nullSafeEquals(sort, that.sort);
	}

	private boolean querySettingsEquals(BasicQuery that) {
		return super.equals(that);
	}

	/*
	 * (non-Javadoc)
	 */
	@Override
	public int hashCode() {

		int result = super.hashCode();
		result = 31 * result + nullSafeHashCode(getCriteriaList());
		result = 31 * result + nullSafeHashCode(projectionFields);
		result = 31 * result + nullSafeHashCode(sort);

		return result;
	}
}
