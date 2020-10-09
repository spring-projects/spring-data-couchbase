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

import org.springframework.data.couchbase.core.mapping.Document;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;

import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.util.Assert;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.UpsertOptions;

public class ReactiveUpsertByIdOperationSupport implements ReactiveUpsertByIdOperation {

	private final ReactiveCouchbaseTemplate template;

	public ReactiveUpsertByIdOperationSupport(final ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveUpsertById<T> upsertById(final Class<T> domainType) {
		Assert.notNull(domainType, "DomainType must not be null!");
		return new ReactiveUpsertByIdSupport<>(template, domainType, null, PersistTo.NONE, ReplicateTo.NONE,
				DurabilityLevel.NONE, Duration.ofSeconds(0));
	}

	static class ReactiveUpsertByIdSupport<T> implements ReactiveUpsertById<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<T> domainType;
		private final String collection;
		private final PersistTo persistTo;
		private final ReplicateTo replicateTo;
		private final DurabilityLevel durabilityLevel;
		private final Duration expiry;

		ReactiveUpsertByIdSupport(final ReactiveCouchbaseTemplate template, final Class<T> domainType,
				final String collection, final PersistTo persistTo, final ReplicateTo replicateTo,
				final DurabilityLevel durabilityLevel, final Duration expiry) {
			this.template = template;
			this.domainType = domainType;
			this.collection = collection;
			this.persistTo = persistTo;
			this.replicateTo = replicateTo;
			this.durabilityLevel = durabilityLevel;
			this.expiry = expiry;
		}

		@Override
		public Mono<T> one(T object) {
			return Mono.just(object).flatMap(o -> {
				CouchbaseDocument converted = template.support().encodeEntity(o);
				return template.getCollection(collection).reactive()
						.upsert(converted.getId(), converted.export(), buildUpsertOptions()).map(result -> {
							template.support().applyUpdatedCas(object, result.cas());
							return o;
						});
			}).onErrorMap(throwable -> {
				if (throwable instanceof RuntimeException) {
					return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
				} else {
					return throwable;
				}
			});
		}

		@Override
		public Flux<? extends T> all(Collection<? extends T> objects) {
			return Flux.fromIterable(objects).flatMap(this::one);
		}

		private UpsertOptions buildUpsertOptions() {
			final UpsertOptions options = UpsertOptions.upsertOptions();
			if (persistTo != PersistTo.NONE || replicateTo != ReplicateTo.NONE) {
				options.durability(persistTo, replicateTo);
			} else if (durabilityLevel != DurabilityLevel.NONE) {
				options.durability(durabilityLevel);
			}
			if (expiry != null && !expiry.isZero()) {
				options.expiry(expiry);
			} else if (domainType.isAnnotationPresent(Document.class)) {
				Document documentAnn = domainType.getAnnotation(Document.class);
				long durationSeconds = documentAnn.expiryUnit().toSeconds(documentAnn.expiry());
				options.expiry(Duration.ofSeconds(durationSeconds));
			}
			return options;
		}

		@Override
		public TerminatingUpsertById<T> inCollection(final String collection) {
			Assert.hasText(collection, "Collection must not be null nor empty.");
			return new ReactiveUpsertByIdSupport<>(template, domainType, collection, persistTo, replicateTo, durabilityLevel,
					expiry);
		}

		@Override
		public UpsertByIdWithCollection<T> withDurability(final DurabilityLevel durabilityLevel) {
			Assert.notNull(durabilityLevel, "Durability Level must not be null.");
			return new ReactiveUpsertByIdSupport<>(template, domainType, collection, persistTo, replicateTo, durabilityLevel,
					expiry);
		}

		@Override
		public UpsertByIdWithCollection<T> withDurability(final PersistTo persistTo, final ReplicateTo replicateTo) {
			Assert.notNull(persistTo, "PersistTo must not be null.");
			Assert.notNull(replicateTo, "ReplicateTo must not be null.");
			return new ReactiveUpsertByIdSupport<>(template, domainType, collection, persistTo, replicateTo, durabilityLevel,
					expiry);
		}

		@Override
		public UpsertByIdWithDurability<T> withExpiry(final Duration expiry) {
			Assert.notNull(expiry, "expiry must not be null.");
			return new ReactiveUpsertByIdSupport<>(template, domainType, collection, persistTo, replicateTo, durabilityLevel,
					expiry);
		}
	}

}
