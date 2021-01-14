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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteriaDefinition;
import org.springframework.data.couchbase.core.support.OneAndAllReactive;
import org.springframework.data.couchbase.core.support.WithCollection;
import org.springframework.data.couchbase.core.support.WithConsistency;
import org.springframework.data.couchbase.core.support.WithDistinct;
import org.springframework.data.couchbase.core.support.WithProjection;
import org.springframework.data.couchbase.core.support.WithQuery;

import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * ReactiveFindByQueryOperation
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
public interface ReactiveFindByQueryOperation {

	/**
	 * Queries the N1QL service.
	 *
	 * @param domainType the entity type to use for the results.
	 */
	<T> ReactiveFindByQuery<T> findByQuery(Class<T> domainType);

	/**
	 * Compose find execution by calling one of the terminating methods.
	 */
	interface TerminatingFindByQuery<T> extends OneAndAllReactive<T> {

		/**
		 * Get exactly zero or one result.
		 *
		 * @return a mono with the match if found (an empty one otherwise).
		 * @throws IncorrectResultSizeDataAccessException if more than one match found.
		 */
		Mono<T> one();

		/**
		 * Get the first or no result.
		 *
		 * @return the first or an empty mono if none found.
		 */
		Mono<T> first();

		/**
		 * Get all matching elements.
		 *
		 * @return never {@literal null}.
		 */
		Flux<T> all();

		/**
		 * Get the number of matching elements.
		 *
		 * @return total number of matching elements.
		 */
		Mono<Long> count();

		/**
		 * Check for the presence of matching elements.
		 *
		 * @return {@literal true} if at least one matching element exists.
		 */
		Mono<Boolean> exists();

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
		 * @return new instance of {@link ExecutableFindByQueryOperation.ExecutableFindByQuery}.
		 * @throws IllegalArgumentException if criteria is {@literal null}.
		 */
		default TerminatingFindByQuery<T> matching(QueryCriteriaDefinition criteria) {
			return matching(Query.query(criteria));
		}

	}

	/**
	 * Collection override (optional).
	 */
	interface FindByQueryInCollection<T> extends FindByQueryWithQuery<T>, WithCollection<T> {

		/**
		 * Explicitly set the name of the collection to perform the query on. <br />
		 * Skip this step to use the default collection derived from the domain type.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return new instance of {@link FindByQueryWithProjection}.
		 * @throws IllegalArgumentException if collection is {@literal null}.
		 */
		FindByQueryWithQuery<T> inCollection(String collection);
	}

	/**
	 * @deprecated
	 * @see FindByQueryWithConsistency
	 */
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
	 * Result type override (optional).
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
	 *
	 * @author Michael Reiche
	 */
	interface FindByQueryWithDistinct<T> extends FindByQueryWithProjection<T>, WithDistinct<T> {

		/**
		 * Finds the distinct values for a specified {@literal field} across a single {@link } or view.
		 *
		 * @param distinctFields name of the field. Must not be {@literal null}.
		 * @return new instance of {@link ReactiveFindByQuery}.
		 * @throws IllegalArgumentException if field is {@literal null}.
		 */
		FindByQueryWithProjection<T> distinct(String[] distinctFields);
	}

	interface ReactiveFindByQuery<T> extends FindByQueryWithDistinct<T> {}

}
