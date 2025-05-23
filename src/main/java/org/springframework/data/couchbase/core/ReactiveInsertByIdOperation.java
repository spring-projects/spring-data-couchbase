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
package org.springframework.data.couchbase.core;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;

import org.springframework.data.couchbase.core.support.InCollection;
import org.springframework.data.couchbase.core.support.InScope;
import org.springframework.data.couchbase.core.support.OneAndAllEntityReactive;
import org.springframework.data.couchbase.core.support.WithDurability;
import org.springframework.data.couchbase.core.support.WithExpiry;
import org.springframework.data.couchbase.core.support.WithInsertOptions;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

/**
 * Insert Operations
 *
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveInsertByIdOperation {

	/**
	 * Insert using the KV service.
	 *
	 * @param domainType the entity type to insert.
	 */
	<T> ReactiveInsertById<T> insertById(Class<T> domainType);

	/**
	 * Terminating operations invoking the actual execution.
	 */
	interface TerminatingInsertById<T> extends OneAndAllEntityReactive<T> {

		/**
		 * Insert one entity.
		 *
		 * @return Inserted entity.
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
	 */
	interface InsertByIdWithOptions<T> extends TerminatingInsertById<T>, WithInsertOptions<T> {
		/**
		 * Fluent method to specify options to use for execution.
		 *
		 * @param options to use for execution
		 */
		@Override
		TerminatingInsertById<T> withOptions(InsertOptions options);
	}

	/**
	 * Fluent method to specify the collection.
	 */
	interface InsertByIdInCollection<T> extends InsertByIdWithOptions<T>, InCollection<T> {
		/**
		 * With a different collection
		 *
		 * @param collection the collection to use.
		 */
		@Override
		InsertByIdWithOptions<T> inCollection(String collection);
	}

	/**
	 * Fluent method to specify the scope.
	 */
	interface InsertByIdInScope<T> extends InsertByIdInCollection<T>, InScope<T> {
		/**
		 * With a different scope
		 *
		 * @param scope the scope to use.
		 */
		@Override
		InsertByIdInCollection<T> inScope(String scope);
	}

	interface InsertByIdWithDurability<T> extends InsertByIdInScope<T>, WithDurability<T> {

		@Override
		InsertByIdInScope<T> withDurability(DurabilityLevel durabilityLevel);

		@Override
		InsertByIdInScope<T> withDurability(PersistTo persistTo, ReplicateTo replicateTo);

	}

	interface InsertByIdWithExpiry<T> extends InsertByIdWithDurability<T>, WithExpiry<T> {

		@Override
		InsertByIdWithDurability<T> withExpiry(Duration expiry);
	}

	/**
	 * Provides methods for constructing KV insert operations in a fluent way.
	 *
	 * @param <T> the entity type to insert
	 */
	interface ReactiveInsertById<T> extends InsertByIdWithExpiry<T> {}

}
