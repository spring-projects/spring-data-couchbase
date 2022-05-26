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
import org.springframework.data.couchbase.transaction.CouchbaseTransactionalOperator;
import org.springframework.util.Assert;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.ReplicateTo;

import static com.couchbase.client.java.transactions.internal.ConverterUtil.makeCollectionIdentifier;

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
		private final CouchbaseTransactionalOperator txCtx;
		private final ReactiveTemplateSupport support;

		ReactiveReplaceByIdSupport(final ReactiveCouchbaseTemplate template, final Class<T> domainType, final String scope,
								   final String collection, final ReplaceOptions options, final PersistTo persistTo, final ReplicateTo replicateTo,
								   final DurabilityLevel durabilityLevel, final Duration expiry, final CouchbaseTransactionalOperator txCtx,
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

		@Override
		public Mono<T> one(T object) {
			PseudoArgs<ReplaceOptions> pArgs = new PseudoArgs<>(template, scope, collection, options, txCtx, domainType);
			LOG.trace("replaceById {}", pArgs);
			Mono<ReactiveCouchbaseTemplate> tmpl = template.doGetTemplate();

			return TransactionalSupport.one(tmpl, pArgs.getTxOp(), pArgs.getScope(), pArgs.getCollection(), support, object,
					(TransactionalSupportHelper support) -> {
						CouchbaseDocument converted = support.converted;

						return support.collection
								.replace(converted.getId(), converted.export(),
										buildReplaceOptions(pArgs.getOptions(), object, converted))
								.flatMap(result -> this.support.applyResult(object, converted, converted.getId(), result.cas(), null));
					}, (TransactionalSupportHelper support) -> {
						rejectInvalidTransactionalOptions();

						CouchbaseDocument converted = support.converted;
						if ( support.cas == null || support.cas == 0 ){
							throw new IllegalArgumentException("cas must be supplied in object for tx replace. object="+object);
						}
						// todo gpx replace is a nightmare...
						// Where to put and how to pass the TransactionGetResult
						// - Idea: TransactionSynchronizationManager.bindResource
						// - Idea: use @Version as an index into Map<Long, TransactionGetResult>
						// - As below, one idea is not to store it at all.
						// Person could have been fetched outside of @Transactional block. Need to flat out prevent. Right??
						// - Maybe not. Could have the replaceById do a ctx.get(), and check the CAS matches the Person (will
						// mandate @Version on Person).
						// - Could always do that in fact. Then no need to hold onto TransactionGetResult anywhere - but slower too
						// (could optimise later).
						// - And if had get-less replaces, could pass in the CAS.
						// - Note: if Person was fetched outside the transaction, the transaction will inevitably expire (continuous
						// CAS mismatch).
						// -- Will have to doc that the user generally wants to do the read inside the txn.
						// -- Can we detect this scenario and reject at runtime? That would also probably need storing something in
						// Person.

						// TransactionGetResult gr = (TransactionGetResult)
						// org.springframework.transaction.support.TransactionSynchronizationManager.getResource(object);
						Mono<CoreTransactionGetResult> gr = support.ctx.get(makeCollectionIdentifier(support.collection.async()), converted.getId());

						return gr.flatMap(getResult -> {
							if (getResult.cas() !=  support.cas) {
								System.err.println("internal: "+getResult.cas()+" object.cas: "+ support.cas+" "+converted);
								// todo gp really want to set internal state and raise a TransactionOperationFailed
								throw new RetryTransactionException();
							}
							return support.ctx.replace(getResult, 	template.getCouchbaseClientFactory().getCluster().block().environment().transcoder()
									.encode(support.converted.export()).encoded());
						}).flatMap(result -> this.support.applyResult(object, converted, converted.getId(), 0L, null, null));
					});

		}

		private void rejectInvalidTransactionalOptions() {
			if (this.persistTo != null || this.replicateTo != null) {
				throw new IllegalArgumentException("withDurability PersistTo and ReplicateTo overload is not supported in a transaction");
			}
			if (this.expiry != null) {
				throw new IllegalArgumentException("withExpiry is not supported in a transaction");
			}
			if (this.options != null) {
				throw new IllegalArgumentException("withOptions is not supported in a transaction");
			}
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
		public ReplaceByIdWithExpiry<T> transaction(final CouchbaseTransactionalOperator txCtx) {
			Assert.notNull(txCtx, "txCtx must not be null.");
			return new ReactiveReplaceByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

	}

}
