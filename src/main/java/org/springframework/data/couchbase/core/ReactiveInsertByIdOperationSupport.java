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

import static com.couchbase.client.core.cnc.TracingIdentifiers.TRANSACTION_OP_INSERT;
import static com.couchbase.client.java.transactions.internal.ConverterUtil.makeCollectionIdentifier;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.util.Assert;

import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

/**
 * {@link ReactiveInsertByIdOperation} implementations for Couchbase.
 *
 * @author Michael Reiche
 * @author Tigran Babloyan
 */
public class ReactiveInsertByIdOperationSupport implements ReactiveInsertByIdOperation {

	private final ReactiveCouchbaseTemplate template;
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveInsertByIdOperationSupport.class);

	public ReactiveInsertByIdOperationSupport(final ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveInsertById<T> insertById(final Class<T> domainType) {
		Assert.notNull(domainType, "DomainType must not be null!");
		return new ReactiveInsertByIdSupport<>(template, domainType, OptionsBuilder.getScopeFrom(domainType),
				OptionsBuilder.getCollectionFrom(domainType), null, OptionsBuilder.getPersistTo(domainType),
				OptionsBuilder.getReplicateTo(domainType), OptionsBuilder.getDurabilityLevel(domainType, template.getConverter()),
				null, template.support());
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
			PseudoArgs<InsertOptions> pArgs = new PseudoArgs(template, scope, collection, options, domainType);
			if (LOG.isDebugEnabled()) {
				LOG.debug("insertById object={} {}", object, pArgs);
			}
			return Mono
					.just(template.getCouchbaseClientFactory().withScope(pArgs.getScope()).getCollection(pArgs.getCollection()))
					.flatMap(collection -> support.encodeEntity(object)
							.flatMap(converted -> TransactionalSupport.checkForTransactionInThreadLocalStorage().flatMap(ctxOpt -> {
								if (!ctxOpt.isPresent()) {
									return collection.reactive()
											.insert(converted.getId().toString(), converted.export(),
													buildOptions(pArgs.getOptions(), converted))
											.flatMap(result -> this.support.applyResult(object, converted, converted.getId(), result.cas(),
													null, null));
								} else {
									rejectInvalidTransactionalOptions();
									CoreTransactionAttemptContext internal = ctxOpt.get().getCore();
									RequestSpan span = CbTracing.newSpan(internal.core().context(), TRANSACTION_OP_INSERT,
											internal.span());
									span.attribute(TracingIdentifiers.ATTR_OPERATION, TRANSACTION_OP_INSERT);
									return internal
											.insert(makeCollectionIdentifier(collection.async()), converted.getId().toString(),
													template.getCouchbaseClientFactory().getCluster().environment().transcoder()
															.encode(converted.export()).encoded(),
													new SpanWrapper(span))
											.flatMap(result -> this.support.applyResult(object, converted, converted.getId(), result.cas(),
													null, null));
								}
							})).onErrorMap(throwable -> {
								if (throwable instanceof RuntimeException) {
									return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
								} else {
									return throwable;
								}
							}));
		}

		private void rejectInvalidTransactionalOptions() {
			if ((this.persistTo != null && this.persistTo != PersistTo.NONE)
					|| (this.replicateTo != null && this.replicateTo != ReplicateTo.NONE)) {
				throw new IllegalArgumentException(
						"withDurability PersistTo and ReplicateTo overload is not supported in a transaction");
			}
			if (this.expiry != null) {
				throw new IllegalArgumentException("withExpiry is not supported in a transaction");
			}
			if (this.durabilityLevel != null && this.durabilityLevel != DurabilityLevel.NONE) {
				throw new IllegalArgumentException("withDurability is not supported in a transaction");
			}
			if (this.options != null) {
				throw new IllegalArgumentException("withOptions is not supported in a transaction");
			}
		}

		@Override
		public Flux<? extends T> all(Collection<? extends T> objects) {
			return Flux.fromIterable(objects).flatMap(this::one);
		}

		public InsertOptions buildOptions(InsertOptions options, CouchbaseDocument doc) { // CouchbaseDocument converted
			return OptionsBuilder.buildInsertOptions(options, persistTo, replicateTo, durabilityLevel, expiry, doc);
		}

		@Override
		public TerminatingInsertById<T> withOptions(final InsertOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ReactiveInsertByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, support);
		}

		@Override
		public InsertByIdInCollection<T> inScope(final String scope) {
			return new ReactiveInsertByIdSupport<>(template, domainType, scope != null ? scope : this.scope, collection,
					options, persistTo, replicateTo, durabilityLevel, expiry, support);
		}

		@Override
		public InsertByIdWithOptions<T> inCollection(final String collection) {
			return new ReactiveInsertByIdSupport<>(template, domainType, scope,
					collection != null ? collection : this.collection, options, persistTo, replicateTo, durabilityLevel, expiry,
					support);
		}

		@Override
		public InsertByIdInScope<T> withDurability(final DurabilityLevel durabilityLevel) {
			Assert.notNull(durabilityLevel, "Durability Level must not be null.");
			return new ReactiveInsertByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, support);
		}

		@Override
		public InsertByIdInScope<T> withDurability(final PersistTo persistTo, final ReplicateTo replicateTo) {
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
