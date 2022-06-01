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

import static com.couchbase.client.java.kv.GetAndTouchOptions.getAndTouchOptions;
import static com.couchbase.client.java.transactions.internal.ConverterUtil.makeCollectionIdentifier;

import org.springframework.data.couchbase.repository.support.TransactionResultHolder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionalOperator;
import org.springframework.util.Assert;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.kv.GetAndTouchOptions;
import com.couchbase.client.java.kv.GetOptions;

public class ReactiveFindByIdOperationSupport implements ReactiveFindByIdOperation {

	private final ReactiveCouchbaseTemplate template;
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveFindByIdOperationSupport.class);

	ReactiveFindByIdOperationSupport(ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveFindById<T> findById(Class<T> domainType) {
		return new ReactiveFindByIdSupport<>(template, domainType, null, null, null, null, null, null, template.support());
	}

	static class ReactiveFindByIdSupport<T> implements ReactiveFindById<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<T> domainType;
		private final String scope;
		private final String collection;
		private final CommonOptions<?> options;
		private final List<String> fields;
		private final CouchbaseTransactionalOperator txCtx;
		private final ReactiveTemplateSupport support;
		private final Duration expiry;

		ReactiveFindByIdSupport(ReactiveCouchbaseTemplate template, Class<T> domainType, String scope, String collection,
				CommonOptions<?> options, List<String> fields, Duration expiry, CouchbaseTransactionalOperator txCtx,
				ReactiveTemplateSupport support) {
			this.template = template;
			this.domainType = domainType;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.fields = fields;
			this.expiry = expiry;
			this.txCtx = txCtx;
			this.support = support;
		}

		@Override
		public Mono<T> one(final String id) {

			CommonOptions<?> gOptions = initGetOptions();
			PseudoArgs<?> pArgs = new PseudoArgs(template, scope, collection, gOptions, txCtx, domainType);
			LOG.trace("findById {}", pArgs);

			ReactiveCollection rc = template.getCouchbaseClientFactory().withScope(pArgs.getScope())
					.getCollection(pArgs.getCollection()).reactive();

			Mono<T> reactiveEntity = TransactionalSupport.checkForTransactionInThreadLocalStorage(txCtx).flatMap(ctxOpt -> {
				if (!ctxOpt.isPresent()) {
					if (pArgs.getOptions() instanceof GetAndTouchOptions) {
						return rc.getAndTouch(id, expiryToUse(), (GetAndTouchOptions) pArgs.getOptions())
								.flatMap(result -> support.decodeEntity(id, result.contentAs(String.class), result.cas(), domainType,
										pArgs.getScope(), pArgs.getCollection(), null));
					} else {
						return rc.get(id, (GetOptions) pArgs.getOptions())
								.flatMap(result -> support.decodeEntity(id, result.contentAs(String.class), result.cas(), domainType,
										pArgs.getScope(), pArgs.getCollection(), null));
					}
				} else {
					return ctxOpt.get().getCore().get(makeCollectionIdentifier(rc.async()), id)
							.flatMap(result -> support.decodeEntity(id, new String(result.contentAsBytes(), StandardCharsets.UTF_8),
									result.cas(), domainType, pArgs.getScope(), pArgs.getCollection(),
									new TransactionResultHolder(result), null));
				}
			});

			return reactiveEntity.onErrorResume(throwable -> {
				if (throwable instanceof DocumentNotFoundException) {
					return Mono.empty();
				}
				return Mono.error(throwable);
			}).onErrorMap(throwable -> {
				if (throwable instanceof RuntimeException) {
					return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
				} else {
					return throwable;
				}
			});

		}

		@Override
		public Flux<? extends T> all(final Collection<String> ids) {
			return Flux.fromIterable(ids).flatMap(this::one);
		}

		@Override
		public FindByIdInScope<T> withOptions(final GetOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ReactiveFindByIdSupport<>(template, domainType, scope, collection, options, fields, expiry, txCtx,
					support);
		}

		@Override
		public FindByIdInCollection<T> inCollection(final String collection) {
			return new ReactiveFindByIdSupport<>(template, domainType, scope, collection, options, fields, expiry, txCtx,
					support);
		}

		@Override
		public FindByIdInCollection<T> inScope(final String scope) {
			return new ReactiveFindByIdSupport<>(template, domainType, scope, collection, options, fields, expiry, txCtx,
					support);
		}

		@Override
		public FindByIdWithOptions<T> project(String... fields) {
			Assert.notNull(fields, "Fields must not be null");
			return new ReactiveFindByIdSupport<>(template, domainType, scope, collection, options, Arrays.asList(fields),
					expiry, txCtx, support);
		}

		@Override
		public FindByIdWithProjection<T> withExpiry(final Duration expiry) {
			return new ReactiveFindByIdSupport<>(template, domainType, scope, collection, options, fields, expiry, txCtx,
					support);
		}

		@Override
		public FindByIdWithProjection<T> transaction(CouchbaseTransactionalOperator txCtx) {
			Assert.notNull(txCtx, "txCtx must not be null");
			return new ReactiveFindByIdSupport<>(template, domainType, scope, collection, options, fields, expiry, txCtx,
					support);
		}

		private CommonOptions<?> initGetOptions() {
			CommonOptions<?> getOptions;
			if (expiry != null || options instanceof GetAndTouchOptions) {
				GetAndTouchOptions gOptions = options != null ? (GetAndTouchOptions) options : getAndTouchOptions();
				if (gOptions.build().transcoder() == null) {
					gOptions.transcoder(RawJsonTranscoder.INSTANCE);
				}
				getOptions = gOptions;
			} else {
				GetOptions gOptions = options != null ? (GetOptions) options : GetOptions.getOptions();
				if (gOptions.build().transcoder() == null) {
					gOptions.transcoder(RawJsonTranscoder.INSTANCE);
				}
				if (fields != null && !fields.isEmpty()) {
					gOptions.project(fields);
				}
				getOptions = gOptions;
			}
			return getOptions;
		}

		private Duration expiryToUse() {
			Duration expiryToUse = expiry;
			if (expiryToUse != null || options instanceof GetAndTouchOptions) {
				if (expiryToUse == null) { // GetAndTouchOptions without specifying expiry -> get expiry from annoation
					final CouchbasePersistentEntity<?> entity = template.getConverter().getMappingContext()
							.getRequiredPersistentEntity(domainType);
					expiryToUse = entity.getExpiryDuration();
				}
			}
			return expiryToUse;
		}

	}

}
