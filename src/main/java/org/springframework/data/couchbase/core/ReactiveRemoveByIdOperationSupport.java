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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.util.Assert;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.transactions.AttemptContextReactive;
import com.couchbase.transactions.TransactionGetResult;
import com.couchbase.transactions.components.TransactionLinks;

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
		private final AttemptContextReactive txCtx;

		private final TransactionLinks tl = new TransactionLinks(Optional.empty(), Optional.empty(), Optional.empty(),
				Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
				Optional.empty(), Optional.empty(), false, Optional.empty(), Optional.empty());

		ReactiveRemoveByIdSupport(final ReactiveCouchbaseTemplate template, final Class<?> domainType, final String scope,
				final String collection, final RemoveOptions options, final PersistTo persistTo, final ReplicateTo replicateTo,
				final DurabilityLevel durabilityLevel, Long cas, AttemptContextReactive txCtx) {
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
			ReactiveCollection rc = template.getCouchbaseClientFactory().withScope(pArgs.getScope())
					.getCollection(pArgs.getCollection()).reactive();
			Mono<RemoveResult> removeResult;
			if (pArgs.getCtx() == null) {
				removeResult = rc.remove(id, buildRemoveOptions(pArgs.getOptions())).map(r -> RemoveResult.from(id, r));
			} else {
				Transcoder transcoder = template.getCouchbaseClientFactory().getCluster().environment().transcoder();
				TransactionGetResult doc = new TransactionGetResult(id, null, 0, rc, tl, null, Optional.empty(), transcoder,
						null);
				removeResult = pArgs.getCtx().remove(doc).map(r -> new RemoveResult(id, 0, null));
			}
			return removeResult.onErrorMap(throwable -> {
				if (throwable instanceof RuntimeException) {
					return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
				} else {
					return throwable;
				}
			});
		}

		@Override
		public Flux<RemoveResult> all(final Collection<String> ids) {
			return Flux.fromIterable(ids).flatMap(this::one);
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
		public RemoveByIdWithCas transaction(AttemptContextReactive txCtx) {
			return new ReactiveRemoveByIdSupport(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, cas, txCtx);
		}

	}

}
