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

import org.springframework.data.couchbase.core.support.AnyIdReactive;
import org.springframework.data.couchbase.core.support.WithCollection;

public interface ReactiveFindFromReplicasByIdOperation {

	<T> ReactiveFindFromReplicasById<T> findFromReplicasById(Class<T> domainType);

	interface TerminatingFindFromReplicasById<T> extends AnyIdReactive<T> {

		Mono<T> any(String id);

		Flux<? extends T> any(Collection<String> ids);

	}

	interface FindFromReplicasByIdWithCollection<T> extends TerminatingFindFromReplicasById<T>, WithCollection<T> {

		TerminatingFindFromReplicasById<T> inCollection(String collection);
	}

	interface ReactiveFindFromReplicasById<T> extends FindFromReplicasByIdWithCollection<T> {}

}
