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
package org.springframework.data.couchbase.core;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteriaDefinition;
import org.springframework.data.couchbase.core.support.OneAndAll;
import org.springframework.data.couchbase.core.support.WithCollection;
import org.springframework.data.couchbase.core.support.WithConsistency;
import org.springframework.data.couchbase.core.support.WithDistinct;
import org.springframework.data.couchbase.core.support.WithProjection;
import org.springframework.data.couchbase.core.support.WithQuery;
import org.springframework.lang.Nullable;

import com.couchbase.client.java.query.QueryScanConsistency;

public interface ExecutableFindByQueryOperation {

	/**
	 * Queries the N1QL service.
	 *
	 * @param domainType the entity type to use for the results.
	 */
	<T> ExecutableFindByQuery<T> findByQuery(Class<T> domainType);

	interface TerminatingFindByQuery<T> extends OneAndAll<T> {
		/**
		 * Get exactly zero or one result.
		 *
		 * @return {@link Optional#empty()} if no match found.
		 * @throws IncorrectResultSizeDataAccessException if more than one match found.
		 */
		default Optional<T> one() {
			return Optional.ofNullable(oneValue());
		}

		/**
		 * Get exactly zero or one result.
		 *
		 * @return {@literal null} if no match found.
		 * @throws IncorrectResultSizeDataAccessException if more than one match found.
		 */
		@Nullable
		T oneValue();

		/**
		 * Get the first or no result.
		 *
		 * @return {@link Optional#empty()} if no match found.
		 */
		default Optional<T> first() {
			return Optional.ofNullable(firstValue());
		}

		/**
		 * Get the first or no result.
		 *
		 * @return {@literal null} if no match found.
		 */
		@Nullable
		T firstValue();

		/**
		 * Get all matching elements.
		 *
		 * @return never {@literal null}.
		 */
		List<T> all();

		/**
		 * Stream all matching elements.
		 *
		 * @return a {@link Stream} of results. Never {@literal null}.
		 */
		Stream<T> stream();

		/**
		 * Get the number of matching elements.
		 *
		 * @return total number of matching elements.
		 */
		long count();

		/**
		 * Check for the presence of matching elements.
		 *
		 * @return {@literal true} if at least one matching element exists.
		 */
		boolean exists();

	}

	/**
	 * Terminating operations invoking the actual query execution.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface FindByQueryWithQuery<T> extends TerminatingFindByQuery<T>, WithQuery<T> {

		/**
		 * Set the filter for the query to be used.
		 *
		 * @param query must not be {@literal null}.
		 * @throws IllegalArgumentException if query is {@literal null}.
		 */
		TerminatingFindByQuery<T> matching(Query query);

		/**
		 * Set the filter {@link QueryCriteriaDefinition criteria} to be used.
		 *
		 * @param criteria must not be {@literal null}.
		 * @return new instance of {@link ExecutableFindByQuery}.
		 * @throws IllegalArgumentException if criteria is {@literal null}.
		 */
		default TerminatingFindByQuery<T> matching(QueryCriteriaDefinition criteria) {
			return matching(Query.query(criteria));
		}

	}

	interface FindByQueryInCollection<T> extends FindByQueryWithQuery<T>, WithCollection<T> {

		/**
		 * Allows to override the default scan consistency.
		 *
		 * @param collection the collection to use for this query.
		 */
		FindByQueryWithQuery<T> inCollection(String collection);

	}

	@Deprecated
	interface FindByQueryConsistentWith<T> extends FindByQueryInCollection<T> {

		/**
		 * Allows to override the default scan consistency.
		 *
		 * @param scanConsistency the custom scan consistency to use for this query.
		 */
		@Deprecated
		FindByQueryInCollection<T> consistentWith(QueryScanConsistency scanConsistency);

	}

	interface FindByQueryWithConsistency<T> extends FindByQueryConsistentWith<T>, WithConsistency<T> {

		/**
		 * Allows to override the default scan consistency.
		 *
		 * @param scanConsistency the custom scan consistency to use for this query.
		 */
		FindByQueryConsistentWith<T> withConsistency(QueryScanConsistency scanConsistency);

	}

	/**
	 * Result type override (Optional).
	 */
	interface FindByQueryWithProjection<T> extends FindByQueryWithConsistency<T> {

		/**
		 * Define the target type fields should be mapped to. <br />
		 * Skip this step if you are anyway only interested in the original domain type.
		 *
		 * @param returnType must not be {@literal null}.
		 * @return new instance of {@link FindByQueryWithProjection}.
		 * @throws IllegalArgumentException if returnType is {@literal null}.
		 */
		<R> FindByQueryWithConsistency<R> as(Class<R> returnType);
	}

	/**
	 * Distinct Find support.
	 */
	interface FindByQueryWithDistinct<T> extends FindByQueryWithProjection<T>, WithDistinct<T> {

		/**
		 * Finds the distinct values for a specified {@literal field} across a single collection
		 *
		 * @param distinctFields name of the field. Must not be {@literal null}.
		 * @return new instance of {@link ExecutableFindByQuery}.
		 * @throws IllegalArgumentException if field is {@literal null}.
		 */
		FindByQueryWithProjection<T> distinct(String[] distinctFields);

	}

	/**
	 * {@link ExecutableFindByQuery} provides methods for constructing lookup operations in a fluent way.
	 */

	interface ExecutableFindByQuery<T> extends FindByQueryWithDistinct<T> {}

}
