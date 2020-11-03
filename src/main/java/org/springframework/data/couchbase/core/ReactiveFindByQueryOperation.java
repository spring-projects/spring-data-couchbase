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
	interface TerminatingFindByQuery<T> {

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
	interface FindByQueryWithQuery<T> extends TerminatingFindByQuery<T> {

		/**
		 * Set the filter for the query to be used.
		 *
		 * @param query must not be {@literal null}.
		 * @throws IllegalArgumentException if query is {@literal null}.
		 */
		TerminatingFindByQuery<T> matching(Query query);

	}

	interface FindByQueryConsistentWith<T> extends FindByQueryWithQuery<T> {

		/**
		 * Allows to override the default scan consistency.
		 *
		 * @param scanConsistency the custom scan consistency to use for this query.
		 */
		FindByQueryConsistentWith<T> consistentWith(QueryScanConsistency scanConsistency);

	}

	/**
	 * Collection override (optional).
	 */
	interface FindInCollection<T> extends FindByQueryWithQuery<T> {

		/**
		 * Explicitly set the name of the collection to perform the query on. <br />
		 * Skip this step to use the default collection derived from the domain type.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return new instance of {@link FindWithProjection}.
		 * @throws IllegalArgumentException if collection is {@literal null}.
		 */
		FindInCollection<T> inCollection(String collection);
	}

	/**
	 * Result type override (optional).
	 */
	interface FindWithProjection<T> extends FindInCollection<T>, FindDistinct {

		/**
		 * Define the target type fields should be mapped to. <br />
		 * Skip this step if you are anyway only interested in the original domain type.
		 *
		 * @param resultType must not be {@literal null}.
		 * @param <R> result type.
		 * @return new instance of {@link FindWithProjection}.
		 * @throws IllegalArgumentException if resultType is {@literal null}.
		 */
		<R> FindByQueryWithQuery<R> as(Class<R> resultType);
	}

	/**
	 * Distinct Find support.
	 *
 	 * @author Michael Reiche
	 */
	interface FindDistinct {

		/**
		 * Finds the distinct values for a specified {@literal field} across a single {@link } or view.
		 *
		 * @param field name of the field. Must not be {@literal null}.
		 * @return new instance of {@link TerminatingDistinct}.
		 * @throws IllegalArgumentException if field is {@literal null}.
		 */
		TerminatingDistinct<Object> distinct(String field);
	}

	/**
	 * Result type override. Optional.
	 *
 	 * @author Michael Reiche
	 */
	interface DistinctWithProjection {

		/**
		 * Define the target type the result should be mapped to. <br />
		 * Skip this step if you are anyway fine with the default conversion.
		 * <dl>
		 * <dt>{@link Object} (the default)</dt>
		 * <dd>Result is mapped according to the {@link } converting eg. {@link } into plain {@link String}, {@link } to
		 * {@link Long}, etc. always picking the most concrete type with respect to the domain types property.<br />
		 * Any {@link } is run through the {@link org.springframework.data.convert.EntityReader} to obtain the domain type.
		 * <br />
		 * Using {@link Object} also works for non strictly typed fields. Eg. a mixture different types like fields using
		 * {@link String} in one {@link } while {@link Long} in another.</dd>
		 * <dt>Any Simple type like {@link String}, {@link Long}, ...</dt>
		 * <dd>The result is mapped directly by the Couchbase Java driver and the {@link } in place. This works only for
		 * results where all documents considered for the operation use the very same type for the field.</dd>
		 * <dt>Any Domain type</dt>
		 * <dd>Domain types can only be mapped if the if the result of the actual {@code distinct()} operation returns
		 * {@link }.</dd>
		 * <dt>{@link }</dt>
		 * <dd>Using {@link } allows retrieval of the raw driver specific format, which returns eg. {@link }.</dd>
		 * </dl>
		 *
		 * @param resultType must not be {@literal null}.
		 * @param <R> result type.
		 * @return new instance of {@link TerminatingDistinct}.
		 * @throws IllegalArgumentException if resultType is {@literal null}.
		 */
		<R> TerminatingDistinct<R> as(Class<R> resultType);
	}

	/**
	 * Result restrictions. Optional.
	 *
 	 * @author Michael Reiche
	 */
	interface DistinctWithQuery<T> extends DistinctWithProjection {

		/**
		 * Set the filter {@link Query criteria} to be used.
		 *
		 * @param query must not be {@literal null}.
		 * @return new instance of {@link TerminatingDistinct}.
		 * @throws IllegalArgumentException if criteria is {@literal null}.
		 */
		TerminatingDistinct<T> matching(Query query);
	}

	/**
	 * Terminating distinct find operations.
	 *
 	 * @author Michael Reiche
	 */
	interface TerminatingDistinct<T> extends DistinctWithQuery<T> {

		/**
		 * Get all matching distinct field values.
		 *
		 * @return empty {@link Flux} if not match found. Never {@literal null}.
		 */
		Flux<T> all();
	}

	interface ReactiveFindByQuery<T> extends FindByQueryConsistentWith<T>, FindInCollection<T>, FindDistinct {}

}
