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

import java.util.Collection;

import org.springframework.data.couchbase.core.support.InCollection;
import org.springframework.data.couchbase.core.support.InScope;
import org.springframework.data.couchbase.core.support.OneAndAllIdReactive;
import org.springframework.data.couchbase.core.support.WithDurability;
import org.springframework.data.couchbase.core.support.WithRemoveOptions;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplicateTo;

/**
 * Remove Operations on KV service.
 *
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveRemoveByIdOperation {
	/**
	 * Removes a document.
	 */
	@Deprecated
	ReactiveRemoveById removeById();

	/**
	 * Removes a document.
	 */
	ReactiveRemoveById removeById(Class<?> domainType);

	/**
	 * Terminating operations invoking the actual execution.
	 */
	interface TerminatingRemoveById extends OneAndAllIdReactive<RemoveResult> {

		/**
		 * Remove one document based on the given ID.
		 *
		 * @param id the document ID.
		 * @return result of the remove
		 */
		@Override
		Mono<RemoveResult> one(String id);

		/**
		 * Remove the documents in the collection.
		 *
		 * @param ids the document IDs.
		 * @return result of the removes.
		 */
		@Override
		Flux<RemoveResult> all(Collection<String> ids);

	}

	/**
	 * Fluent method to specify options.
	 */
	interface RemoveByIdWithOptions extends TerminatingRemoveById, WithRemoveOptions<RemoveResult> {
		/**
		 * Fluent method to specify options to use for execution
		 *
		 * @param options options to use for execution
		 */
		TerminatingRemoveById withOptions(RemoveOptions options);
	}

	/**
	 * Fluent method to specify the collection.
	 */
	interface RemoveByIdInCollection extends RemoveByIdWithOptions, InCollection<Object> {
		/**
		 * With a different collection
		 *
		 * @param collection the collection to use.
		 */
		RemoveByIdWithOptions inCollection(String collection);
	}

	/**
	 * Fluent method to specify the scope.
	 */
	interface RemoveByIdInScope extends RemoveByIdInCollection, InScope<Object> {
		/**
		 * With a different scope
		 *
		 * @param scope the scope to use.
		 */
		RemoveByIdInCollection inScope(String scope);
	}

	interface RemoveByIdWithDurability extends RemoveByIdInScope, WithDurability<RemoveResult> {
		@Override
		RemoveByIdInCollection withDurability(DurabilityLevel durabilityLevel);

		@Override
		RemoveByIdInCollection withDurability(PersistTo persistTo, ReplicateTo replicateTo);

	}

	interface RemoveByIdWithCas extends RemoveByIdWithDurability {

		RemoveByIdWithDurability withCas(Long cas);
	}

	/**
	 * Provides methods for constructing remove operations in a fluent way.
	 */
	interface ReactiveRemoveById extends RemoveByIdWithCas {}

}
