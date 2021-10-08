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
package org.springframework.data.couchbase.core;

import java.util.Collection;
import java.util.List;

import org.springframework.data.couchbase.core.support.InCollection;
import org.springframework.data.couchbase.core.support.InScope;
import org.springframework.data.couchbase.core.support.OneAndAllId;
import org.springframework.data.couchbase.core.support.WithCas;
import org.springframework.data.couchbase.core.support.WithDurability;
import org.springframework.data.couchbase.core.support.WithRemoveOptions;
import org.springframework.data.couchbase.core.support.WithTransaction;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.transactions.AttemptContextReactive;

/**
 * Remove Operations on KV service.
 *
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ExecutableRemoveByIdOperation {
	/**
	 * Removes a document.
	 */
	ExecutableRemoveById removeById(Class<?> domainType);

	/**
	 * Removes a document.
	 */
	@Deprecated
	ExecutableRemoveById removeById();

	/**
	 * Terminating operations invoking the actual execution.
	 */
	interface TerminatingRemoveById extends OneAndAllId<RemoveResult> {

		/**
		 * Remove one document based on the given ID.
		 *
		 * @param id the document ID.
		 * @return result of the remove
		 */
		@Override
		RemoveResult one(String id);

		/**
		 * Remove the documents in the collection.
		 *
		 * @param ids the document IDs.
		 * @return result of the removes.
		 */
		@Override
		List<RemoveResult> all(Collection<String> ids);

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
		@Override
		TerminatingRemoveById withOptions(RemoveOptions options);
	}

	interface RemoveByIdWithDurability extends RemoveByIdWithOptions, WithDurability<RemoveResult> {

		@Override
		RemoveByIdWithOptions withDurability(DurabilityLevel durabilityLevel);

		@Override
		RemoveByIdWithOptions withDurability(PersistTo persistTo, ReplicateTo replicateTo);

	}

	interface RemoveByIdWithCas extends RemoveByIdWithDurability, WithCas<RemoveResult> {
		@Override
		RemoveByIdWithDurability withCas(Long cas);
	}

	interface RemoveByIdWithTransaction extends TerminatingRemoveById, WithTransaction<RemoveResult> {
		@Override
		TerminatingRemoveById transaction(AttemptContextReactive txCtx);
	}

	interface RemoveByIdTxOrNot extends RemoveByIdWithCas, RemoveByIdWithTransaction {}

	/**
	 * Fluent method to specify the collection.
	 */
	interface RemoveByIdInCollection extends RemoveByIdTxOrNot, InCollection<Object> {
		/**
		 * With a different collection
		 *
		 * @param collection the collection to use.
		 */
		@Override
		RemoveByIdTxOrNot inCollection(String collection);
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
		@Override
		RemoveByIdInCollection inScope(String scope);
	}

	/**
	 * Provides methods for constructing remove operations in a fluent way.
	 */
	interface ExecutableRemoveById extends RemoveByIdInScope {}

}
