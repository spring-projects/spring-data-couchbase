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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.util.Assert;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

public class ReactiveInsertByIdOperationSupport implements ReactiveInsertByIdOperation {

	private static final Logger LOG = LoggerFactory.getLogger(ReactiveInsertByIdOperationSupport.class);
	private final ReactiveCouchbaseTemplate template;

	public ReactiveInsertByIdOperationSupport(final ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveInsertById<T> insertById(final Class<T> domainType) {
		Assert.notNull(domainType, "DomainType must not be null!");
		return new ReactiveInsertByIdSupport<>(template, domainType, null, null, null, PersistTo.NONE, ReplicateTo.NONE,
				DurabilityLevel.NONE, null, template.support());
	}

	static class ReactiveInsertByIdSupport<T> implements ReactiveInsertById<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<T> domainType;
		private final String scope;
		private final String collection;
		private final InsertOptions options;
		private final PersistTo persistTo;
		private final ReplicateTo replicateTo;
		private final DurabilityLevel durabilityLevel;
		private final Duration expiry;
		private final ReactiveTemplateSupport support;

		ReactiveInsertByIdSupport(final ReactiveCouchbaseTemplate template, final Class<T> domainType, final String scope,
				final String collection, final InsertOptions options, final PersistTo persistTo, final ReplicateTo replicateTo,
				final DurabilityLevel durabilityLevel, Duration expiry, ReactiveTemplateSupport support) {
			this.template = template;
			this.domainType = domainType;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.persistTo = persistTo;
			this.replicateTo = replicateTo;
			this.durabilityLevel = durabilityLevel;
			this.expiry = expiry;
			this.support = support;
		}

		@Override
		public Mono<T> one(T object) {
            PseudoArgs<InsertOptions> pArgs = new PseudoArgs(template, scope, collection,
                                                             options != null ? options : InsertOptions.insertOptions());
            LOG.trace("statement: {} scope: {} collection: {} options: {}", "insertById", pArgs.getScope(),
                      pArgs.getCollection(), pArgs.getOptions());
			return Mono.just(object).flatMap(support::encodeEntity)
            .flatMap(converted -> template.getCouchbaseClientFactory().withScope(pArgs.getScope())
                     .getCollection(pArgs.getCollection()).reactive()
                     .insert(converted.getId(), converted.export(), buildOptions(pArgs.getOptions(), converted))
							.flatMap(result -> support.applyUpdatedId(object, converted.getId())
									.flatMap(updatedObject -> support.applyUpdatedCas(updatedObject, converted, result.cas()))))
					.onErrorMap(throwable -> {
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

		@Override
		public InsertOptions buildOptions(InsertOptions options, CouchbaseDocument doc) { // CouchbaseDocument converted
			options = options != null ? options : InsertOptions.insertOptions();
			if (persistTo != PersistTo.NONE || replicateTo != ReplicateTo.NONE) {
				options.durability(persistTo, replicateTo);
			} else if (durabilityLevel != DurabilityLevel.NONE) {
				options.durability(durabilityLevel);
			}
			if (expiry != null) {
				options.expiry(expiry);
			} else if (doc.getExpiration() != 0) {
				options.expiry(Duration.ofSeconds(doc.getExpiration()));
			}
			return options;
		}

		@Override
		public TerminatingInsertById<T> withOptions(final InsertOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ReactiveInsertByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, support);
		}

		@Override
		public InsertByIdInCollection<T> inScope(final String scope) {
			Assert.hasText(scope, "Scope must not be null nor empty.");
			return new ReactiveInsertByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, support);
		}

		@Override
		public InsertByIdWithOptions<T> inCollection(final String collection) {
			Assert.hasText(collection, "Collection must not be null nor empty.");
			return new ReactiveInsertByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, support);
		}

		@Override
		public InsertByIdInCollection<T> withDurability(final DurabilityLevel durabilityLevel) {
			Assert.notNull(durabilityLevel, "Durability Level must not be null.");
			return new ReactiveInsertByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, support);
		}

		@Override
		public InsertByIdInCollection<T> withDurability(final PersistTo persistTo, final ReplicateTo replicateTo) {
			Assert.notNull(persistTo, "PersistTo must not be null.");
			Assert.notNull(replicateTo, "ReplicateTo must not be null.");
			return new ReactiveInsertByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, support);
		}

		@Override
		public InsertByIdWithDurability<T> withExpiry(final Duration expiry) {
			Assert.notNull(expiry, "expiry must not be null.");
			return new ReactiveInsertByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, support);
		}
	}

}
