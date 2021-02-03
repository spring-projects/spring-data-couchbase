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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;

import org.springframework.data.couchbase.core.support.InCollection;
import org.springframework.data.couchbase.core.support.InScope;
import org.springframework.data.couchbase.core.support.OneAndAllEntityReactive;
import org.springframework.data.couchbase.core.support.WithUpsertOptions;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.UpsertOptions;

/**
 * Insert Operations
 *
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveUpsertByIdOperation {


	/**
	 * Upsert using the KV service.
	 *
	 * @param domainType the entity type to upsert.
	 */
	<T> ReactiveUpsertById<T> upsertById(Class<T> domainType);

	/**
	 * Terminating operations invoking the actual execution.
	 */
	interface TerminatingUpsertById<T> extends OneAndAllEntityReactive<T> {

		/**
		 * Upsert one entity.
		 *
		 * @return Upserted entity.
		 */
		@Override
		Mono<T> one(T object);

		/**
		 * Insert a collection of entities.
		 *
		 * @return Inserted entities
		 */
		@Override
		Flux<? extends T> all(Collection<? extends T> objects);

	}

	/**
	 * Fluent method to specify options.
	 *
	 * @param <T> the entity type to use.
	 */
	interface UpsertByIdWithOptions<T> extends TerminatingUpsertById<T>, WithUpsertOptions<T> {
		/**
		 * Fluent method to specify options to use for execution
		 *
		 * @param options to use for execution
		 */
		@Override
		TerminatingUpsertById<T> withOptions(UpsertOptions options);
	}

	/**
	 * Fluent method to specify the collection.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface UpsertByIdInCollection<T> extends UpsertByIdWithOptions<T>, InCollection<Object> {
		/**
		 * With a different collection
		 *
		 * @param collection the collection to use.
		 */
		@Override
		UpsertByIdWithOptions<T> inCollection(String collection);
	}

	/**
	 * Fluent method to specify the scope.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface UpsertByIdInScope<T> extends UpsertByIdInCollection<T>, InScope<Object> {
		/**
		 * With a different scope
		 *
		 * @param scope the scope to use.
		 */
		@Override
		UpsertByIdInCollection<T> inScope(String scope);
	}

	interface UpsertByIdWithDurability<T> extends UpsertByIdInScope<T>, WithDurability<T> {
		@Override
		UpsertByIdInCollection<T> withDurability(DurabilityLevel durabilityLevel);
		@Override
		UpsertByIdInCollection<T> withDurability(PersistTo persistTo, ReplicateTo replicateTo);

	}

	interface UpsertByIdWithExpiry<T> extends UpsertByIdWithDurability<T>, WithExpiry<T> {
		@Override
		UpsertByIdWithDurability<T> withExpiry(Duration expiry);
	}

	/**
	 * Provides methods for constructing KV operations in a fluent way.
	 *
	 * @param <T> the entity type to upsert
	 */
	interface ReactiveUpsertById<T> extends UpsertByIdWithExpiry<T> {}

}
