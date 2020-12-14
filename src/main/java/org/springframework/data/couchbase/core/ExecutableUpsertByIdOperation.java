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

import java.time.Duration;
import java.util.Collection;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

public interface ExecutableUpsertByIdOperation {

	<T> ExecutableUpsertById<T> upsertById(Class<T> domainType);

	interface TerminatingUpsertById<T> extends OneAndAll<T>{

		@Override
		T one(T object);

		@Override
		Collection<? extends T> all(Collection<? extends T> objects);

	}

	interface UpsertByIdWithCollection<T> extends TerminatingUpsertById<T>, InCollection<T> {

		TerminatingUpsertById<T> inCollection(String collection);
	}

	interface UpsertByIdWithDurability<T> extends UpsertByIdWithCollection<T>, WithDurability<T> {

		UpsertByIdWithCollection<T> withDurability(DurabilityLevel durabilityLevel);

		UpsertByIdWithCollection<T> withDurability(PersistTo persistTo, ReplicateTo replicateTo);

	}

	interface UpsertByIdWithExpiry<T> extends UpsertByIdWithDurability<T>, WithExpiry<T> {

		@Override
		UpsertByIdWithDurability<T> withExpiry(Duration expiry);
	}

	interface ExecutableUpsertById<T> extends UpsertByIdWithExpiry<T> {}

}
