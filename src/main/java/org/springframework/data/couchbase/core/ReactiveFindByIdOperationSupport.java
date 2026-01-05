/*
 * Copyright 2012-present the original author or authors
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

import static com.couchbase.client.java.kv.GetAndLockOptions.getAndLockOptions;
import static com.couchbase.client.java.kv.GetAndTouchOptions.getAndTouchOptions;
import static com.couchbase.client.java.transactions.internal.ConverterUtil.makeCollectionIdentifier;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.util.Assert;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.GetAndLockOptions;
import com.couchbase.client.java.kv.GetAndTouchOptions;
import com.couchbase.client.java.kv.GetOptions;

/**
 * {@link ReactiveFindByIdOperation} implementations for Couchbase.
 *
 * @author Michael Reiche
 * @author Tigran Babloyan
 */
public class ReactiveFindByIdOperationSupport implements ReactiveFindByIdOperation {

	private final ReactiveCouchbaseTemplate template;
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveFindByIdOperationSupport.class);

	ReactiveFindByIdOperationSupport(ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveFindById<T> findById(Class<T> domainType) {
		return new ReactiveFindByIdSupport<>(template, domainType, OptionsBuilder.getScopeFrom(domainType),
				OptionsBuilder.getCollectionFrom(domainType), null, null, null, null, template.support());
	}

	static class ReactiveFindByIdSupport<T> implements ReactiveFindById<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<T> domainType;
		private final String scope;
		private final String collection;
		private final CommonOptions<?> options;
		private final List<String> fields;
		private final ReactiveTemplateSupport support;
		private final Duration expiry;
		private final Duration lockDuration;

		private Duration expiryToUse;

		ReactiveFindByIdSupport(ReactiveCouchbaseTemplate template, Class<T> domainType, String scope, String collection,
				CommonOptions<?> options, List<String> fields, Duration expiry, Duration lockDuration, ReactiveTemplateSupport support) {
			this.template = template;
			this.domainType = domainType;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.fields = fields;
			this.expiry = expiry;
			this.support = support;
			this.lockDuration = lockDuration;
		}

		@Override
		public Mono<T> one(final Object id) {

			PseudoArgs<?> testPargs = new PseudoArgs(template, scope, collection, null, domainType);
			CommonOptions<?> gOptions = testPargs.getOptions() != null ? (CommonOptions<?>) testPargs.getOptions()
					: initGetOptions();
			PseudoArgs<?> pArgs = new PseudoArgs(template, testPargs.getScope(), testPargs.getCollection(), gOptions,
					domainType);
			if (LOG.isDebugEnabled()) {
				LOG.debug("findById key={} {}", id, pArgs);
			}
			ReactiveCollection rc = template.getCouchbaseClientFactory().withScope(pArgs.getScope())
					.getCollection(pArgs.getCollection()).reactive();

			Mono<T> reactiveEntity = TransactionalSupport.checkForTransactionInThreadLocalStorage().flatMap(ctxOpt -> {
				if (!ctxOpt.isPresent()) {
					if (pArgs.getOptions() instanceof GetAndTouchOptions options) {
						return rc
								.getAndTouch(id.toString(), expiryToUse,
										buildOptions((GetAndTouchOptions) pArgs.getOptions()))
								.flatMap(result -> support.decodeEntity(id, result.contentAs(String.class),
										result.cas(), result.expiryTime().orElse(null), domainType, pArgs.getScope(),
										pArgs.getCollection(), null, null));
					} else if (pArgs.getOptions() instanceof GetAndLockOptions options) {
						return rc
								.getAndLock(id.toString(), Optional.of(lockDuration).orElse(Duration.ZERO),
										buildOptions((GetAndLockOptions) pArgs.getOptions()))
								.flatMap(result -> support.decodeEntity(id, result.contentAs(String.class),
										result.cas(), result.expiryTime().orElse(null), domainType, pArgs.getScope(),
										pArgs.getCollection(), null, null));
					} else {
						return rc.get(id.toString(), buildOptions((GetOptions) pArgs.getOptions()))
								.flatMap(result -> support.decodeEntity(id, result.contentAs(String.class),
										result.cas(), result.expiryTime().orElse(null), domainType,
										pArgs.getScope(), pArgs.getCollection(), null, null));
					}
				} else {
					rejectInvalidTransactionalOptions();
					return ctxOpt.get().getCore().get(makeCollectionIdentifier(rc.async()), id.toString())
							.flatMap(result -> support.decodeEntity(id, new String(result.contentAsBytes(), StandardCharsets.UTF_8),
									result.cas(), null, domainType, pArgs.getScope(), pArgs.getCollection(), null,
									null));
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

		private void rejectInvalidTransactionalOptions() {
			if (this.lockDuration != null) {
				throw new IllegalArgumentException("withLock is not supported in a transaction");
			}
			if (this.expiry != null) {
				throw new IllegalArgumentException("withExpiry is not supported in a transaction");
			}
			if (this.options != null) {
				throw new IllegalArgumentException("withOptions is not supported in a transaction");
			}
			if (this.fields != null) {
				throw new IllegalArgumentException("project is not supported in a transaction");
			}
		}

		@Override
		public Flux<? extends T> all(final Collection<String> ids) {
			return Flux.fromIterable(ids).flatMap(this::one);
		}

		public GetOptions buildOptions(GetOptions options) {
			return OptionsBuilder.buildGetOptions(options);
		}

		public GetAndTouchOptions buildOptions(GetAndTouchOptions options) {
			return OptionsBuilder.buildGetAndTouchOptions(options);
		}

		public GetAndLockOptions buildOptions(GetAndLockOptions options) {
			return OptionsBuilder.buildGetAndLockOptions(options);
		}

		@Override
		public FindByIdInScope<T> withOptions(final GetOptions options) {
			return new ReactiveFindByIdSupport<>(template, domainType, scope, collection,
					options != null ? options : this.options, fields, expiry,
					lockDuration, support);
		}

		@Override
		public FindByIdWithOptions<T> inCollection(final String collection) {
			return new ReactiveFindByIdSupport<>(template, domainType, scope,
					collection != null ? collection : this.collection, options, fields, expiry, lockDuration, support);
		}

		@Override
		public FindByIdInCollection<T> inScope(final String scope) {
			return new ReactiveFindByIdSupport<>(template, domainType, scope != null ? scope : this.scope, collection,
					options, fields, expiry, lockDuration, support);
		}

		@Override
		public FindByIdInCollection<T> project(String... fields) {
			Assert.notNull(fields, "Fields must not be null");
			return new ReactiveFindByIdSupport<>(template, domainType, scope, collection, options, Arrays.asList(fields),
					expiry, lockDuration, support);
		}

		@Override
		public FindByIdWithProjection<T> withExpiry(final Duration expiry) {
			return new ReactiveFindByIdSupport<>(template, domainType, scope, collection, options, fields, expiry, 
					lockDuration, support);
		}

		@Override
		public FindByIdWithExpiry<T> withLock(final Duration lockDuration) {
			return new ReactiveFindByIdSupport<>(template, domainType, scope, collection, options, fields, expiry, 
					lockDuration, support);
		}

		private CommonOptions<?> initGetOptions() {
			CommonOptions<?> getOptions;
			final CouchbasePersistentEntity<?> entity = template.getConverter().getMappingContext()
					.getRequiredPersistentEntity(domainType);
			Boolean isTouchOnRead = entity.isTouchOnRead();
			if(lockDuration != null || options instanceof GetAndLockOptions) {
				getOptions = options != null ? (GetAndLockOptions) options : getAndLockOptions();
			} else if (expiry != null || isTouchOnRead	|| options instanceof GetAndTouchOptions) {
				if (expiry != null) {
					expiryToUse = expiry;
				} else if (isTouchOnRead) {
					expiryToUse = entity.getExpiryDuration();
				} else {
					expiryToUse = Duration.ZERO;
				}
				getOptions = options != null ? (GetAndTouchOptions) options : getAndTouchOptions();
			} else {
				GetOptions gOptions = options != null ? (GetOptions) options : GetOptions.getOptions();
				if (fields != null && !fields.isEmpty()) {
					gOptions.project(fields);
				}
				getOptions = gOptions;
			}
			return getOptions;
		}

	}

}
