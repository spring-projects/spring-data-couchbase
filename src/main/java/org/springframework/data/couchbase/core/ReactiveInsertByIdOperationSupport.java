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

import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.data.couchbase.repository.support.TransactionResultHolder;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionalOperator;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.util.Assert;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

import static com.couchbase.client.java.transactions.internal.ConverterUtil.makeCollectionIdentifier;

public class ReactiveInsertByIdOperationSupport implements ReactiveInsertByIdOperation {

	private final ReactiveCouchbaseTemplate template;
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveInsertByIdOperationSupport.class);

	public ReactiveInsertByIdOperationSupport(final ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveInsertById<T> insertById(final Class<T> domainType) {
		Assert.notNull(domainType, "DomainType must not be null!");
		return new ReactiveInsertByIdSupport<>(template, domainType, null, null, null, PersistTo.NONE, ReplicateTo.NONE,
				DurabilityLevel.NONE, null, (TransactionalOperator) null, template.support());
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
		private final CouchbaseTransactionalOperator txCtx;
		private final TransactionalOperator txOp;
		private final ReactiveTemplateSupport support;

		ReactiveInsertByIdSupport(final ReactiveCouchbaseTemplate template, final Class<T> domainType, final String scope,
								  final String collection, final InsertOptions options, final PersistTo persistTo, final ReplicateTo replicateTo,
								  final DurabilityLevel durabilityLevel, Duration expiry, CouchbaseTransactionalOperator txCtx,
								  ReactiveTemplateSupport support) {
			this.template = template;
			this.domainType = domainType;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.persistTo = persistTo;
			this.replicateTo = replicateTo;
			this.durabilityLevel = durabilityLevel;
			this.expiry = expiry;
			this.txCtx = txCtx;
			this.txOp = null;
			this.support = support;
		}

		ReactiveInsertByIdSupport(final ReactiveCouchbaseTemplate template, final Class<T> domainType, final String scope,
								  final String collection, final InsertOptions options, final PersistTo persistTo, final ReplicateTo replicateTo,
								  final DurabilityLevel durabilityLevel, Duration expiry, TransactionalOperator txOp,
								  ReactiveTemplateSupport support) {
			this.template = template;
			this.domainType = domainType;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.persistTo = persistTo;
			this.replicateTo = replicateTo;
			this.durabilityLevel = durabilityLevel;
			this.expiry = expiry;
			this.txCtx = null;
			this.txOp = txOp;
			this.support = support;
		}

		@Override
		public Mono<T> one(T object) {
			PseudoArgs<InsertOptions> pArgs = new PseudoArgs(template, scope, collection, options, txCtx, domainType);
			LOG.trace("insertById {}", pArgs);

			return template.doGetTemplate().getCouchbaseClientFactory().withScope(pArgs.getScope())
					.getCollectionMono(pArgs.getCollection()).flatMap(collection -> support.encodeEntity(object)
							.flatMap(converted -> TransactionalSupport.checkForTransactionInThreadLocalStorage(txCtx).flatMap(ctxOpt -> {
								if (!ctxOpt.isPresent()) {
									System.err.println("insert non-tx");
									return collection.reactive()
											.insert(converted.getId(), converted.export(), buildOptions(pArgs.getOptions(), converted))
											.flatMap(
													result -> this.support.applyResult(object, converted, converted.getId(), result.cas(), null));
								} else {
									rejectInvalidTransactionalOptions();
									System.err.println("insert tx");
									return ctxOpt.get().getCore()
											.insert(makeCollectionIdentifier(collection.async()), converted.getId(),
													template.getCouchbaseClientFactory().getCluster().environment().transcoder()
															.encode(converted.export()).encoded())
											.flatMap(result -> this.support.applyResult(object, converted, converted.getId(), getCas(result),
													new TransactionResultHolder(result), null));
								}
							})).onErrorMap(throwable -> {
								if (throwable instanceof RuntimeException) {
									return template.doGetTemplate().potentiallyConvertRuntimeException((RuntimeException) throwable);
								} else {
									return throwable;
								}
							}));
		}

		private void rejectInvalidTransactionalOptions() {
			if ((this.persistTo != null && this.persistTo != PersistTo.NONE) || (this.replicateTo != null && this.replicateTo != ReplicateTo.NONE)) {
				throw new IllegalArgumentException("withDurability PersistTo and ReplicateTo overload is not supported in a transaction");
			}
			if (this.expiry != null) {
				throw new IllegalArgumentException("withExpiry is not supported in a transaction");
			}
			if (this.options != null) {
				throw new IllegalArgumentException("withOptions is not supported in a transaction");
			}
		}

		private Long getCas(CoreTransactionGetResult getResult) {
			CoreTransactionGetResult internal;
			try {
				// Method method = CoreTransactionGetResult.class.getDeclaredMethod("internal");
				// method.setAccessible(true);
				// internal = (CoreTransactionGetResult) method.invoke(getResult);
			} catch (Throwable err) {
				throw new RuntimeException(err);
			}
			return getResult.cas();
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
					durabilityLevel, expiry, txCtx, support);
		}

		@Override
		public InsertByIdInCollection<T> inScope(final String scope) {
			return new ReactiveInsertByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

		@Override
		public InsertByIdTxOrNot<T> inCollection(final String collection) {
			return new ReactiveInsertByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

		@Override
		public InsertByIdInScope<T> withDurability(final DurabilityLevel durabilityLevel) {
			Assert.notNull(durabilityLevel, "Durability Level must not be null.");
			return new ReactiveInsertByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

		@Override
		public InsertByIdInScope<T> withDurability(final PersistTo persistTo, final ReplicateTo replicateTo) {
			Assert.notNull(persistTo, "PersistTo must not be null.");
			Assert.notNull(replicateTo, "ReplicateTo must not be null.");
			return new ReactiveInsertByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

		@Override
		public InsertByIdWithDurability<T> withExpiry(final Duration expiry) {
			Assert.notNull(expiry, "expiry must not be null.");
			return new ReactiveInsertByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

		@Override
		public InsertByIdWithExpiry<T> transaction(final CouchbaseTransactionalOperator txCtx) {
			Assert.notNull(txCtx, "txCtx must not be null.");
			return new ReactiveInsertByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

	}

}
