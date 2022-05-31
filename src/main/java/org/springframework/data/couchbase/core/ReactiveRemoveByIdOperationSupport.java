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
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.util.Assert;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplicateTo;

import static com.couchbase.client.java.transactions.internal.ConverterUtil.makeCollectionIdentifier;

public class ReactiveRemoveByIdOperationSupport implements ReactiveRemoveByIdOperation {

	private final ReactiveCouchbaseTemplate template;
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveRemoveByIdOperationSupport.class);

	public ReactiveRemoveByIdOperationSupport(final ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	@Deprecated
	public ReactiveRemoveById removeById() {
		return removeById(null);
	}

	@Override
	public ReactiveRemoveById removeById(Class<?> domainType) {
		return new ReactiveRemoveByIdSupport(template, domainType, null, null, null, PersistTo.NONE, ReplicateTo.NONE,
				DurabilityLevel.NONE, null, null);
	}

	static class ReactiveRemoveByIdSupport implements ReactiveRemoveById {

		private final ReactiveCouchbaseTemplate template;
		private final Class<?> domainType;
		private final String scope;
		private final String collection;
		private final RemoveOptions options;
		private final PersistTo persistTo;
		private final ReplicateTo replicateTo;
		private final DurabilityLevel durabilityLevel;
		private final Long cas;
		private final CouchbaseTransactionalOperator txCtx;

		ReactiveRemoveByIdSupport(final ReactiveCouchbaseTemplate template, final Class<?> domainType, final String scope,
								  final String collection, final RemoveOptions options, final PersistTo persistTo, final ReplicateTo replicateTo,
								  final DurabilityLevel durabilityLevel, Long cas, CouchbaseTransactionalOperator txCtx) {
			this.template = template;
			this.domainType = domainType;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.persistTo = persistTo;
			this.replicateTo = replicateTo;
			this.durabilityLevel = durabilityLevel;
			this.cas = cas;
			this.txCtx = txCtx;
		}

		@Override
		public Mono<RemoveResult> one(final String id) {
			PseudoArgs<RemoveOptions> pArgs = new PseudoArgs<>(template, scope, collection, options, txCtx, domainType);
			LOG.trace("removeById {}", pArgs);
			ReactiveCouchbaseClientFactory clientFactory = template.getCouchbaseClientFactory();
			ReactiveCollection rc = clientFactory.withScope(pArgs.getScope()).getCollection(pArgs.getCollection())
					.reactive();
			Mono<ReactiveCouchbaseTemplate> tmpl = template.doGetTemplate();

			// todo gpx convert to TransactionalSupport
			Mono<RemoveResult> allResult = tmpl.map(tp -> tp.getCouchbaseClientFactory().getResources()).flatMap(s -> {
				if (s.getCore() == null) {
					System.err.println("non-tx remove");
					return rc.remove(id, buildRemoveOptions(pArgs.getOptions())).map(r -> RemoveResult.from(id, r));
				} else {
					rejectInvalidTransactionalOptions();

					System.err.println("tx remove");
					if ( cas == null || cas == 0 ){
						throw new IllegalArgumentException("cas must be supplied for tx remove");
					}
					Mono<CoreTransactionGetResult> gr = s.getCore().get(makeCollectionIdentifier(rc.async()), id);

					return gr.flatMap(getResult -> {
						if (getResult.cas() != cas) {
							return Mono.error(TransactionalSupport.retryTransactionOnCasMismatch(s.getCore(), getResult.cas(), cas));
						}
						return s.getCore().remove(getResult)
								.map(r -> new RemoveResult(id, 0, null));
					});

				}}).onErrorMap(throwable -> {
				if (throwable instanceof RuntimeException) {
					return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
				} else {
					return throwable;
				}
			});
			return allResult;
		}

		private void rejectInvalidTransactionalOptions() {
			if ((this.persistTo != null && this.persistTo != PersistTo.NONE) || (this.replicateTo != null && this.replicateTo != ReplicateTo.NONE)) {
				throw new IllegalArgumentException("withDurability PersistTo and ReplicateTo overload is not supported in a transaction");
			}
			if (this.options != null) {
				throw new IllegalArgumentException("withOptions is not supported in a transaction");
			}
		}

		@Override
		public Mono<RemoveResult> oneEntity(Object entity) {
			ReactiveRemoveByIdSupport op = new ReactiveRemoveByIdSupport(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, template.support().getCas(entity), txCtx);
			return op.one(template.support().getId(entity).toString());
		}

		@Override
		public Flux<RemoveResult> all(final Collection<String> ids) {
			return Flux.fromIterable(ids).flatMap(this::one);
		}

		@Override
		public Flux<RemoveResult> allEntities(Collection<Object> entities) {
			return Flux.fromIterable(entities).flatMap(this::oneEntity);
		}

		private RemoveOptions buildRemoveOptions(RemoveOptions options) {
			return OptionsBuilder.buildRemoveOptions(options, persistTo, replicateTo, durabilityLevel, cas);
		}

		@Override
		public RemoveByIdInScope withDurability(final DurabilityLevel durabilityLevel) {
			Assert.notNull(durabilityLevel, "Durability Level must not be null.");
			return new ReactiveRemoveByIdSupport(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, cas, txCtx);
		}

		@Override
		public RemoveByIdInScope withDurability(final PersistTo persistTo, final ReplicateTo replicateTo) {
			Assert.notNull(persistTo, "PersistTo must not be null.");
			Assert.notNull(replicateTo, "ReplicateTo must not be null.");
			return new ReactiveRemoveByIdSupport(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, cas, txCtx);
		}

		@Override
		public RemoveByIdTxOrNot inCollection(final String collection) {
			return new ReactiveRemoveByIdSupport(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, cas, txCtx);
		}

		@Override
		public RemoveByIdInCollection inScope(final String scope) {
			return new ReactiveRemoveByIdSupport(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, cas, txCtx);
		}

		@Override
		public TerminatingRemoveById withOptions(final RemoveOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ReactiveRemoveByIdSupport(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, cas, txCtx);
		}

		@Override
		public RemoveByIdWithDurability withCas(Long cas) {
			return new ReactiveRemoveByIdSupport(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, cas, txCtx);
		}

		@Override
		public RemoveByIdWithCas transaction(CouchbaseTransactionalOperator txCtx) {
			return new ReactiveRemoveByIdSupport(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, cas, txCtx);
		}

	}

}
