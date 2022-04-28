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

import org.springframework.data.couchbase.repository.support.TransactionResultHolder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.data.couchbase.transaction.CouchbaseStuffHandle;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.util.Assert;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION;
import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE;

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
		private final CouchbaseStuffHandle txCtx;
		private final TransactionalOperator txOp;
		private final ReactiveTemplateSupport support;

		ReactiveInsertByIdSupport(final ReactiveCouchbaseTemplate template, final Class<T> domainType, final String scope,
				final String collection, final InsertOptions options, final PersistTo persistTo, final ReplicateTo replicateTo,
				final DurabilityLevel durabilityLevel, Duration expiry, CouchbaseStuffHandle txCtx,
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
			// ReactiveCouchbaseResourceHolder resourceHolder = (ReactiveCouchbaseResourceHolder) synchronizationManager
			// .getResource(getRequiredDatabaseFactory());

			// ((ReactiveCouchbaseResourceHolder)
			// TransactionSynchronizationManager.forCurrentTransaction().flatMap((synchronizationManager) -> {
			// return Mono.just(synchronizationManager.getResource( template.getCouchbaseClientFactory()));
			// }).block()).getSession().getAttemptContextReactive() /
			// if (TransactionSynchronizationManager.hasResource(template.getCouchbaseClientFactory())){
			//
			// }
			// the template should have the session(???)
			PseudoArgs<InsertOptions> pArgs = new PseudoArgs(template, scope, collection, options, txCtx, domainType);
			LOG.trace("insertById {}", pArgs);

			Mono<ReactiveCouchbaseTemplate> tmpl = template.doGetTemplate();
			//ClientSession session = CouchbaseTransactionalTemplate.getSession(template);
			Mono<T> reactiveEntity = support.encodeEntity(object)
					.flatMap(converted -> tmpl.flatMap(tp -> tp.getCouchbaseClientFactory().getSession(null).flatMap(s -> {
						if (s == null || s.getReactiveTransactionAttemptContext() == null) {
							return template.getCouchbaseClientFactory().withScope(pArgs.getScope())
									.getCollection(pArgs.getCollection())
									.flatMap(collection -> collection.reactive()
											.insert(converted.getId(), converted.export(), buildOptions(pArgs.getOptions(), converted))
											.flatMap(
													result -> support.applyResult(object, converted, converted.getId(), result.cas(), null)));
						} else {
							return s.getReactiveTransactionAttemptContext()
									.insert(
											tp.doGetDatabase().block().bucket(tp.getBucketName()).reactive()
													.scope(pArgs.getScope() != null ? pArgs.getScope() : DEFAULT_SCOPE)
													.collection(pArgs.getCollection() != null ? pArgs.getCollection() : DEFAULT_COLLECTION),
											converted.getId(), converted.getContent())
									// todo gp don't have result.cas() anymore - needed?
									.flatMap(result -> support.applyResult(object, converted, converted.getId(), 0L, new TransactionResultHolder(result), s));
						}
					})));
			// .flatMap(converted ->/* rc */tmpl.flatMap(tp -> tp.getCouchbaseClientFactory().getCluster().flatMap( cl ->
			// cl.bucket("my_bucket").reactive()
			// .defaultCollection()
			// .insert(converted.getId(), converted.export(), buildOptions(pArgs.getOptions(), converted))
			// .flatMap(result -> support.applyResult(object, converted, converted.getId(), result.cas(), null)))));
			/*
			} else {
				reactiveEntity = support.encodeEntity(object).flatMap(converted -> pArgs.getTxOp().getAttemptContextReactive() // transactional()
																																																												// needs
																																																												// to
																																																												// have
																																																												// initted
																																																												// acr
						.insert(template.doGetDatabase().block().bucket("my_bucket").reactive().defaultCollection(),
								converted.getId(), converted.getContent(), buildTxOptions(pArgs.getOptions(), converted))
						.flatMap(result -> support.applyResult(object, converted, converted.getId(), result.cas(),
								pArgs.getTxOp().transactionResultHolder(result))));
			}
			*/

			return reactiveEntity.onErrorMap(throwable -> {
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

		// todo gp need to figure out how to handle options re transactions.  E.g. many non-transactional insert options, like this, aren't supported
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
		public InsertByIdWithExpiry<T> transaction(final CouchbaseStuffHandle txCtx) {
			Assert.notNull(txCtx, "txCtx must not be null.");
			return new ReactiveInsertByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

	}

}
