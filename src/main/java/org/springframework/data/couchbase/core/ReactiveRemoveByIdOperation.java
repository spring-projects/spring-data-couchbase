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

import java.util.Collection;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

public interface ReactiveRemoveByIdOperation {

	ReactiveRemoveById removeById();

	interface TerminatingRemoveById {

		Mono<RemoveResult> one(String id);

		Flux<RemoveResult> all(Collection<String> ids);

	}

	interface RemoveByIdWithCollection extends TerminatingRemoveById, InCollection {

		TerminatingRemoveById inCollection(String collection);
	}

	interface RemoveByIdWithDurability extends RemoveByIdWithCollection, WithDurability {

		RemoveByIdWithCollection withDurability(DurabilityLevel durabilityLevel);

		RemoveByIdWithCollection withDurability(PersistTo persistTo, ReplicateTo replicateTo);

	}

	interface ReactiveRemoveById extends RemoveByIdWithDurability {}

}
