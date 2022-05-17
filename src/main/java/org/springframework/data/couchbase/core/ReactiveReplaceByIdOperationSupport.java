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

import com.couchbase.client.core.error.transaction.RetryTransactionException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.transactions.TransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionGetResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.data.couchbase.repository.support.TransactionResultHolder;
import org.springframework.data.couchbase.transaction.CouchbaseStuffHandle;
import org.springframework.util.Assert;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.ReplicateTo;

public class ReactiveReplaceByIdOperationSupport implements ReactiveReplaceByIdOperation {

	private final ReactiveCouchbaseTemplate template;
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveReplaceByIdOperationSupport.class);

	public ReactiveReplaceByIdOperationSupport(final ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveReplaceById<T> replaceById(final Class<T> domainType) {
		Assert.notNull(domainType, "DomainType must not be null!");
		return new ReactiveReplaceByIdSupport<>(template, domainType, null, null, null, PersistTo.NONE, ReplicateTo.NONE,
				DurabilityLevel.NONE, null, null, template.support());
	}

	static class ReactiveReplaceByIdSupport<T> implements ReactiveReplaceById<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<T> domainType;
		private final String scope;
		private final String collection;
		private final ReplaceOptions options;
		private final PersistTo persistTo;
		private final ReplicateTo replicateTo;
		private final DurabilityLevel durabilityLevel;
		private final Duration expiry;
		private final CouchbaseStuffHandle txCtx;
		private final ReactiveTemplateSupport support;

		ReactiveReplaceByIdSupport(final ReactiveCouchbaseTemplate template, final Class<T> domainType, final String scope,
				final String collection, final ReplaceOptions options, final PersistTo persistTo, final ReplicateTo replicateTo,
				final DurabilityLevel durabilityLevel, final Duration expiry, final CouchbaseStuffHandle txCtx,
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
			this.support = support;
		}

		/*
				@Override
				public Mono<T> one(T object) {
					PseudoArgs<ReplaceOptions> pArgs = new PseudoArgs(template, scope, collection, options, null, domainType);
					LOG.trace("upsertById {}", pArgs);
					Mono<ReactiveCouchbaseTemplate> tmpl = template.doGetTemplate();
					Mono<T> reactiveEntity = support.encodeEntity(object)
							.flatMap(converted -> tmpl.flatMap(tp -> tp.getCouchbaseClientFactory().getSession(null).flatMap(s -> {
								if (s == null || s.getAttemptContextReactive() == null) {
									return tp.getCouchbaseClientFactory().withScope(pArgs.getScope()).getCollection(pArgs.getCollection())
											.flatMap(collection -> collection.reactive()
													.replace(converted.getId(), converted.export(),
															buildReplaceOptions(pArgs.getOptions(), object, converted))
													.flatMap(
															result -> support.applyResult(object, converted, converted.getId(), result.cas(), null)));
								} else {
									return Mono.error(new CouchbaseException("No upsert in a transaction. Use insert or replace"));
								}
							})));
		
					return reactiveEntity.onErrorMap(throwable -> {
						if (throwable instanceof RuntimeException) {
							return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
						} else {
							return throwable;
						}
					});
				}
		*/
		@Override
		public Mono<T> one(T object) {
			PseudoArgs<ReplaceOptions> pArgs = new PseudoArgs<>(template, scope, collection, options, txCtx, domainType);
			LOG.trace("replaceById {}", pArgs);

			return GenericSupport.one(template, scope, collection, support, object,
					(GenericSupportHelper support) -> {
						CouchbaseDocument converted = support.converted;

						return support.collection.reactive()
								.replace(converted.getId(), converted.export(),
										buildReplaceOptions(pArgs.getOptions(), object, converted))
								.flatMap(result -> this.support.applyResult(object, converted, converted.getId(), result.cas(), null));
					},
					(GenericSupportHelper support) -> {
						CouchbaseDocument converted = support.converted;

						// todo gpx replace is a nightmare...
						// Where to put and how to pass the TransactionGetResult
						// - Idea: TransactionSynchronizationManager.bindResource
						// - Idea: use @Version as an index into Map<Long, TransactionGetResult>
						// - As below, one idea is not to store it at all.
						// Person could have been fetched outside of @Transactional block.  Need to flat out prevent. Right??
						// - Maybe not. Could have the replaceById do a ctx.get(), and check the CAS matches the Person (will mandate @Version on Person).
						// - Could always do that in fact. Then no need to hold onto TransactionGetResult anywhere - but slower too (could optimise later).
						// - And if had get-less replaces, could pass in the CAS.
						// - Note: if Person was fetched outside the transaction, the transaction will inevitably expire (continuous CAS mismatch).
						// -- Will have to doc that the user generally wants to do the read inside the txn.
						// -- Can we detect this scenario and reject at runtime?  That would also probably need storing something in Person.

						// todo gpx also, what about exception contract?  transactions raises TransactionOperationFailed

						// todo gpx can give different inScope on .findById() than on subsequent .replaceById()...

						return support.ctx.get(support.toCollectionIdentifier(), converted.getId())
								.flatMap(gr -> {

									// todo gpx if we need this of course needs to be exposed nicely
									CoreTransactionGetResult internal;
									try {
										Method method = TransactionGetResult.class.getDeclaredMethod("internal");
										method.setAccessible(true);
										internal = (CoreTransactionGetResult) method.invoke(gr);
									} catch (Throwable err) {
										throw new RuntimeException(err);
									}

									if (internal.cas() != support.converted.version) {
										// todo gpx really want to set internal state and raise a TransactionOperationFailed
										throw new RetryTransactionException();
									}

									return support.ctx.replace(gr, JsonObject.from(converted.getContent()).toBytes())
											.flatMap(replaced -> {
												// todo gpx no CAS
												return this.support.applyResult(object, converted, converted.getId(), 0L, null, null);
											});
								});
					});

//		Mono<ReactiveCouchbaseTemplate> tmpl = template.doGetTemplate();
//			Mono<T> reactiveEntity;
//
//			Optional<TransactionAttemptContext> ctxr = Optional.ofNullable((TransactionAttemptContext)
//					org.springframework.transaction.support.TransactionSynchronizationManager.getResource(TransactionAttemptContext.class));
//
//			CouchbaseDocument converted = support.encodeEntity(object).block();
//			reactiveEntity = tmpl.flatMap(tp -> tp.getCouchbaseClientFactory().getSession(null).flatMap(s -> {
//				if (s == null || s.getReactiveTransactionAttemptContext() == null) {
//					System.err.println("ReactiveReplaceById: not");
//					Mono<com.couchbase.client.java.Collection> op = template.getCouchbaseClientFactory()
//							.withScope(pArgs.getScope()).getCollection(pArgs.getCollection());
//					return op.flatMap(collection -> collection.reactive()
//							.replace(converted.getId(), converted.export(),
//									buildReplaceOptions(pArgs.getOptions(), object, converted))
//							.flatMap(result -> support.applyResult(object, converted, converted.getId(), result.cas(), null)));
//				} else {
//					System.err.println("ReactiveReplaceById: transaction");
//					return s.getReactiveTransactionAttemptContext()
//							.replace(s.transactionResultHolder(getTransactionHolder(object)).transactionGetResult(),
//									converted.getContent())
//							// todo gp no CAS
//							.flatMap(result -> support.applyResult(object, converted, converted.getId(), 0L,
//									new TransactionResultHolder(result), s));
//				}
//			}));
//
//			return reactiveEntity.onErrorMap(throwable -> {
//				if (throwable instanceof RuntimeException) {
//					return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
//				} else {
//					return throwable;
//				}
//			});
		}

		private <T> Integer getTransactionHolder(T object) {
			Integer transactionResultHolder;
			System.err.println("GET: " + System.identityHashCode(object) + " " + object);
			if (1 == 1) {
				return System.identityHashCode(object);
			}
			transactionResultHolder = template.support().getTxResultHolder(object);
			if (transactionResultHolder == null) {
				throw new CouchbaseException(
						"TransactionResult from entity is null - was the entity obtained in a transaction?");
			}
			return transactionResultHolder;
		}

		@Override
		public Flux<? extends T> all(Collection<? extends T> objects) {
			return Flux.fromIterable(objects).flatMap(this::one);
		}

		private ReplaceOptions buildReplaceOptions(ReplaceOptions options, T object, CouchbaseDocument doc) {
			return OptionsBuilder.buildReplaceOptions(options, persistTo, replicateTo, durabilityLevel, expiry,
					support.getCas(object), doc);
		}

		@Override
		public TerminatingReplaceById<T> withOptions(final ReplaceOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ReactiveReplaceByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

		@Override
		public ReplaceByIdTxOrNot<T> inCollection(final String collection) {
			return new ReactiveReplaceByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

		@Override
		public ReplaceByIdInCollection<T> inScope(final String scope) {
			return new ReactiveReplaceByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

		@Override
		public ReplaceByIdInScope<T> withDurability(final DurabilityLevel durabilityLevel) {
			Assert.notNull(durabilityLevel, "Durability Level must not be null.");
			return new ReactiveReplaceByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

		@Override
		public ReplaceByIdInScope<T> withDurability(final PersistTo persistTo, final ReplicateTo replicateTo) {
			Assert.notNull(persistTo, "PersistTo must not be null.");
			Assert.notNull(replicateTo, "ReplicateTo must not be null.");
			return new ReactiveReplaceByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

		@Override
		public ReplaceByIdWithDurability<T> withExpiry(final Duration expiry) {
			Assert.notNull(expiry, "expiry must not be null.");
			return new ReactiveReplaceByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

		@Override
		public ReplaceByIdWithExpiry<T> transaction(final CouchbaseStuffHandle txCtx) {
			Assert.notNull(txCtx, "txCtx must not be null.");
			return new ReactiveReplaceByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

	}

}
