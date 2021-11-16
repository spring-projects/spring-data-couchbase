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

import org.springframework.data.couchbase.transaction.CouchbaseStuffHandle;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;

import org.springframework.data.couchbase.core.support.InCollection;
import org.springframework.data.couchbase.core.support.InScope;
import org.springframework.data.couchbase.core.support.OneAndAllIdReactive;
import org.springframework.data.couchbase.core.support.WithExpiry;
import org.springframework.data.couchbase.core.support.WithGetOptions;
import org.springframework.data.couchbase.core.support.WithProjectionId;
import org.springframework.data.couchbase.core.support.WithTransaction;

import com.couchbase.client.java.kv.GetOptions;

/**
 * Get Operations - method/interface chaining is from the bottom up.
 *
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveFindByIdOperation {

	/**
	 * Loads a document from a bucket.
	 *
	 * @param domainType the entity type to use for the results.
	 */
	<T> ReactiveFindById<T> findById(Class<T> domainType);

	/**
	 * Terminating operations invoking the actual execution.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface TerminatingFindById<T> extends OneAndAllIdReactive<T> {

		/**
		 * Finds one document based on the given ID.
		 *
		 * @param id the document ID.
		 * @return the entity if found.
		 */
		Mono<T> one(String id);

		/**
		 * Finds a list of documents based on the given IDs.
		 *
		 * @param ids the document ID ids.
		 * @return the list of found entities.
		 */
		Flux<? extends T> all(Collection<String> ids);
	}

	/**
	 * Provide transaction
	 *
	 * @param <T> the entity type to use for the results
	 */
	interface FindByIdWithTransaction<T> extends TerminatingFindById<T>, WithTransaction<T> {
		/**
		 * Provide transaction
		 *
		 * @param txCtx
		 * @return
		 */
		TerminatingFindById<T> transaction(CouchbaseStuffHandle txCtx);
	}

	/**
	 * Fluent method to specify options.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByIdWithOptions<T> extends TerminatingFindById<T>, WithGetOptions<T> {
		/**
		 * Fluent method to specify options to use for execution
		 *
		 * @param options options to use for execution
		 */
		@Override
		TerminatingFindById<T> withOptions(GetOptions options);
	}

	interface FindByIdWithProjection<T> extends FindByIdWithOptions<T>, WithProjectionId<T> {
		/**
		 * Load only certain fields for the document.
		 *
		 * @param fields the projected fields to load.
		 */
		FindByIdWithOptions<T> project(String... fields);
	}

	interface FindByIdWithExpiry<T> extends FindByIdWithProjection<T>, WithExpiry<T> {
		/**
		 * Load only certain fields for the document.
		 *
		 * @param expiry the projected fields to load.
		 */
		@Override
		FindByIdWithProjection<T> withExpiry(Duration expiry);
	}

	/**
	 * Interface to that can produce either transactional or non-transactional operations.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByIdTxOrNot<T> extends FindByIdWithTransaction<T>, FindByIdWithExpiry<T> {}

	/**
	 * Fluent method to specify the collection.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByIdInCollection<T> extends FindByIdTxOrNot<T>, InCollection<T> {
		/**
		 * With a different collection
		 *
		 * @param collection the collection to use.
		 */
		@Override
		FindByIdTxOrNot<T> inCollection(String collection);
	}

	/**
	 * Fluent method to specify the scope.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByIdInScope<T> extends FindByIdInCollection<T>, InScope<T> {
		/**
		 * With a different scope
		 *
		 * @param scope the scope to use.
		 */
		@Override
		FindByIdInCollection<T> inScope(String scope);
	}

	/**
	 * Provides methods for constructing query operations in a fluent way.
	 *
	 * @param <T> the entity type.
	 */
	interface ReactiveFindById<T> extends FindByIdInScope<T> {};

}
