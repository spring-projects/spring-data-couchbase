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

import java.util.Collection;

import org.springframework.data.couchbase.core.support.AnyId;
import org.springframework.data.couchbase.core.support.WithCollection;

public interface ExecutableFindFromReplicasByIdOperation {

	<T,I> ExecutableFindFromReplicasById<T,I> findFromReplicasById(Class<T> domainType);

	interface TerminatingFindFromReplicasById<T,I> extends AnyId<T,I> {

		T any(I id);

		Collection<? extends T> any(Collection<I> ids);

	}

	interface FindFromReplicasByIdWithCollection<T,I> extends TerminatingFindFromReplicasById<T,I>, WithCollection<T> {

		TerminatingFindFromReplicasById<T,I> inCollection(String collection);
	}

	interface ExecutableFindFromReplicasById<T,I> extends FindFromReplicasByIdWithCollection<T,I> {}

}
