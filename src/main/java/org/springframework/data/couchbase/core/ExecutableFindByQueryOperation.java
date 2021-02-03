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
package org.springframework.data.couchbase.core;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteriaDefinition;
import org.springframework.data.couchbase.core.support.InCollection;
import org.springframework.data.couchbase.core.support.InScope;
import org.springframework.data.couchbase.core.support.OneAndAll;
import org.springframework.data.couchbase.core.support.WithConsistency;
import org.springframework.data.couchbase.core.support.WithDistinct;
import org.springframework.data.couchbase.core.support.WithQuery;
import org.springframework.data.couchbase.core.support.WithQueryOptions;
import org.springframework.lang.Nullable;

import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * Query Operations
 *
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ExecutableFindByQueryOperation {

	/**
	 * Queries the N1QL service.
	 *
	 * @param domainType the entity type to use for the results.
	 */
	<T> ExecutableFindByQuery<T> findByQuery(Class<T> domainType);

	/**
	 * Terminating operations invoking the actual execution.
	 */
	interface TerminatingFindByQuery<T> extends OneAndAll<T> {

		/**
		 * Get exactly zero or one result.
		 *
		 * @return {@link Optional#empty()} if no match found.
		 * @throws IncorrectResultSizeDataAccessException if more than one match found.
		 */
		@Override
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
		@Override
		T oneValue();

		/**
		 * Get the first or no result.
		 *
		 * @return {@link Optional#empty()} if no match found.
		 */
		@Override
		default Optional<T> first() {
			return Optional.ofNullable(firstValue());
		}

		/**
		 * Get the first or no result.
		 *
		 * @return {@literal null} if no match found.
		 */
		@Nullable
		@Override
		T firstValue();

		/**
		 * Get all matching documents.
		 *
		 * @return never {@literal null}.
		 */
		@Override
		List<T> all();

		/**
		 * Stream all matching elements.
		 *
		 * @return a {@link Stream} of results. Never {@literal null}.
		 */
		@Override
		Stream<T> stream();

		/**
		 * Get the number of matching elements.
		 *
		 * @return total number of matching elements.
		 */
		@Override
		long count();

		/**
		 * Check for the presence of matching elements.
		 *
		 * @return {@literal true} if at least one matching element exists.
		 */
		@Override
		boolean exists();

	}

	/**
	 * Fluent methods to specify the query
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByQueryWithQuery<T> extends TerminatingFindByQuery<T>, WithQuery<T> {

		/**
		 * Set the filter for the query to be used.
		 *
		 * @param query must not be {@literal null}.
		 * @throws IllegalArgumentException if query is {@literal null}.
		 */
		@Override
		TerminatingFindByQuery<T> matching(Query query);

		/**
		 * Set the filter {@link QueryCriteriaDefinition criteria} to be used.
		 *
		 * @param criteria must not be {@literal null}.
		 * @return new instance of {@link ExecutableFindByQuery}.
		 * @throws IllegalArgumentException if criteria is {@literal null}.
		 */
		@Override
		default TerminatingFindByQuery<T> matching(QueryCriteriaDefinition criteria) {
			return matching(Query.query(criteria));
		}

	}

	/**
	 * Fluent method to specify options.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByQueryWithOptions<T> extends FindByQueryWithQuery<T>, WithQueryOptions<T> {
		/**
		 * Fluent method to specify options to use for execution
		 *
		 * @param options to use for execution
		 */
		@Override
		TerminatingFindByQuery<T> withOptions(QueryOptions options);
	}

	/**
	 * Fluent method to specify the collection.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByQueryInCollection<T> extends FindByQueryWithOptions<T>, InCollection<T> {
		/**
		 * With a different collection
		 *
		 * @param collection the collection to use.
		 */
		@Override
		FindByQueryWithOptions<T> inCollection(String collection);
	}

	/**
	 * Fluent method to specify the scope.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByQueryInScope<T> extends FindByQueryInCollection<T>, InScope<T> {
		/**
		 * With a different scope
		 *
		 * @param scope the scope to use.
		 */
		@Override
		FindByQueryInCollection<T> inScope(String scope);
	}

	/**
	 * To be removed at the next major release. use WithConsistency instead
	 * 
	 * @param <T> the entity type to use for the results.
	 */
	@Deprecated
	interface FindByQueryConsistentWith<T> extends FindByQueryInScope<T> {

		/**
		 * Allows to override the default scan consistency.
		 *
		 * @param scanConsistency the custom scan consistency to use for this query.
		 */
		@Deprecated
		FindByQueryInScope<T> consistentWith(QueryScanConsistency scanConsistency);
	}

	/**
	 * Fluent method to specify scan consistency.  Scan consistency may also come from an annotation.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByQueryWithConsistency<T> extends FindByQueryConsistentWith<T>, WithConsistency<T> {

		/**
		 * Allows to override the default scan consistency.
		 *
		 * @param scanConsistency the custom scan consistency to use for this query.
		 */
		@Override
		FindByQueryConsistentWith<T> withConsistency(QueryScanConsistency scanConsistency);
	}

	/**
	 * Fluent method to specify a return type different than the the entity type to use for the results.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByQueryWithProjection<T> extends FindByQueryWithConsistency<T> {

		/**
		 * Define the target type fields should be mapped to. <br />
		 * Skip this step if you are only interested in the original the entity type to use for the results.
		 *
		 * @param returnType must not be {@literal null}.
		 * @return new instance of {@link FindByQueryWithProjection}.
		 * @throws IllegalArgumentException if returnType is {@literal null}.
		 */
		<R> FindByQueryWithConsistency<R> as(Class<R> returnType);
	}

	/**
	 * Fluent method to specify DISTINCT fields
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByQueryWithDistinct<T> extends FindByQueryWithProjection<T>, WithDistinct<T> {

		/**
		 * Finds the distinct values for a specified {@literal field} across a single collection
		 *
		 * @param distinctFields name of the field. Must not be {@literal null}.
		 * @return new instance of {@link ExecutableFindByQuery}.
		 * @throws IllegalArgumentException if field is {@literal null}.
		 */
		@Override
		FindByQueryWithProjection<T> distinct(String[] distinctFields);
	}

	/**
	 * Provides methods for constructing query operations in a fluent way.
	 *
	 * @param <T> the entity type to use for the results
	 */
	interface ExecutableFindByQuery<T> extends FindByQueryWithDistinct<T> {}

}
