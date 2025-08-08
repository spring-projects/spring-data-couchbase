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

import static com.couchbase.client.core.cnc.TracingIdentifiers.TRANSACTION_OP_REMOVE;
import static com.couchbase.client.java.transactions.internal.ConverterUtil.makeCollectionIdentifier;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.util.Assert;

import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplicateTo;

/**
 * {@link ReactiveRemoveByIdOperation} implementations for Couchbase.
 *
 * @author Michael Reiche
 * @author Tigran Babloyan
 */
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
		return new ReactiveRemoveByIdSupport(template, domainType, OptionsBuilder.getScopeFrom(domainType),
				OptionsBuilder.getCollectionFrom(domainType), null, OptionsBuilder.getPersistTo(domainType),
				OptionsBuilder.getReplicateTo(domainType), OptionsBuilder.getDurabilityLevel(domainType, template.getConverter()),
				null);
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

		ReactiveRemoveByIdSupport(final ReactiveCouchbaseTemplate template, final Class<?> domainType, final String scope,
				final String collection, final RemoveOptions options, final PersistTo persistTo, final ReplicateTo replicateTo,
				final DurabilityLevel durabilityLevel, Long cas) {
			this.template = template;
			this.domainType = domainType;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.persistTo = persistTo;
			this.replicateTo = replicateTo;
			this.durabilityLevel = durabilityLevel;
			this.cas = cas;
		}

		@Override
		public Mono<RemoveResult> one(final Object id) {
			PseudoArgs<RemoveOptions> pArgs = new PseudoArgs<>(template, scope, collection, options, domainType);
			if (LOG.isDebugEnabled()) {
				LOG.debug("removeById key={} {}", id, pArgs);
			}
			CouchbaseClientFactory clientFactory = template.getCouchbaseClientFactory();
			ReactiveCollection rc = clientFactory.withScope(pArgs.getScope()).getCollection(pArgs.getCollection()).reactive();

			return TransactionalSupport.checkForTransactionInThreadLocalStorage().flatMap(s -> {
				if (!s.isPresent()) {
					return rc.remove(id.toString(), buildRemoveOptions(pArgs.getOptions()))
							.map(r -> RemoveResult.from(id.toString(), r));
				} else {
					rejectInvalidTransactionalOptions();

					if (cas == null || cas == 0) {
						throw new IllegalArgumentException("cas must be supplied for tx remove");
					}
					CoreTransactionAttemptContext ctx = s.get().getCore();
					Mono<CoreTransactionGetResult> gr = ctx.get(makeCollectionIdentifier(rc.async()), id.toString());

					return gr.flatMap(getResult -> {
						if (getResult.cas() != cas) {
							return Mono.error(TransactionalSupport.retryTransactionOnCasMismatch(ctx, getResult.cas(), cas));
						}
						CoreTransactionAttemptContext internal = ctx;
						RequestSpan span = CbTracing.newSpan(internal.core().context(), TRANSACTION_OP_REMOVE, internal.span());
						span.attribute(TracingIdentifiers.ATTR_OPERATION, TRANSACTION_OP_REMOVE);
						return ctx.remove(getResult, new SpanWrapper(span)).map(r -> new RemoveResult(id.toString(), 0, null));
					});

				}
			}).onErrorMap(throwable -> {
				if (throwable instanceof RuntimeException) {
					return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
				} else {
					return throwable;
				}
			});
		}

		private void rejectInvalidTransactionalOptions() {
			if ((this.persistTo != null && this.persistTo != PersistTo.NONE)
					|| (this.replicateTo != null && this.replicateTo != ReplicateTo.NONE)) {
				throw new IllegalArgumentException(
						"withDurability PersistTo and ReplicateTo overload is not supported in a transaction");
			}
			if (this.durabilityLevel != null && this.durabilityLevel != DurabilityLevel.NONE) {
				throw new IllegalArgumentException("withDurability is not supported in a transaction");
			}
			if (this.options != null) {
				throw new IllegalArgumentException("withOptions is not supported in a transaction");
			}
		}

		@Override
		public Mono<RemoveResult> oneEntity(Object entity) {
			ReactiveRemoveByIdSupport op = new ReactiveRemoveByIdSupport(template, domainType, scope, collection, options,
					persistTo, replicateTo, durabilityLevel, template.support().getCas(entity));
			return op.withCas(template.support().getCas(entity)).one(template.support().getId(entity).toString());
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
					durabilityLevel, cas);
		}

		@Override
		public RemoveByIdInScope withDurability(final PersistTo persistTo, final ReplicateTo replicateTo) {
			Assert.notNull(persistTo, "PersistTo must not be null.");
			Assert.notNull(replicateTo, "ReplicateTo must not be null.");
			return new ReactiveRemoveByIdSupport(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, cas);
		}

		@Override
		public RemoveByIdWithDurability inCollection(final String collection) {
			return new ReactiveRemoveByIdSupport(template, domainType, scope,
					collection != null ? collection : this.collection, options, persistTo, replicateTo, durabilityLevel, cas);
		}

		@Override
		public RemoveByIdInCollection inScope(final String scope) {
			return new ReactiveRemoveByIdSupport(template, domainType, scope != null ? scope : this.scope, collection,
					options, persistTo, replicateTo, durabilityLevel, cas);
		}

		@Override
		public TerminatingRemoveById withOptions(final RemoveOptions options) {
			return new ReactiveRemoveByIdSupport(
					template, domainType, scope, collection,
					options != null ? options : this.options, persistTo, replicateTo,
					durabilityLevel, cas);
		}

		@Override
		public RemoveByIdWithDurability withCas(Long cas) {
			return new ReactiveRemoveByIdSupport(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, cas);
		}

	}

}
