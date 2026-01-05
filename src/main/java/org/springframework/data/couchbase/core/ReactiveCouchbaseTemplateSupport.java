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

import reactor.core.publisher.Mono;

import java.time.Instant;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.couchbase.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.couchbase.core.mapping.event.ReactiveAfterConvertCallback;
import org.springframework.data.couchbase.core.mapping.event.ReactiveBeforeConvertCallback;
import org.springframework.data.couchbase.transaction.CouchbaseResourceHolder;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;
import org.springframework.util.Assert;

/**
 * Internal encode/decode support for {@link ReactiveCouchbaseTemplate}.
 *
 * @author Carlos Espinaco
 * @author Michael Reiche
 * @since 4.2
 */
class ReactiveCouchbaseTemplateSupport extends AbstractTemplateSupport
		implements ApplicationContextAware, ReactiveTemplateSupport {

	private final ReactiveCouchbaseTemplate template;
	private ReactiveEntityCallbacks reactiveEntityCallbacks;

	public ReactiveCouchbaseTemplateSupport(final ReactiveCouchbaseTemplate template, final CouchbaseConverter converter,
			final TranslationService translationService) {
		super(template, converter, translationService);
		this.template = template;
	}

	@Override
	public Mono<CouchbaseDocument> encodeEntity(final Object entityToEncode) {
		return Mono.just(entityToEncode).doOnNext(entity -> maybeEmitEvent(new BeforeConvertEvent<>(entity)))
				.flatMap(entity -> maybeCallBeforeConvert(entity, "")).map(maybeNewEntity -> {
					final CouchbaseDocument converted = new CouchbaseDocument();
					converter.write(maybeNewEntity, converted);
					return converted;
				}).flatMap(converted -> maybeCallAfterConvert(entityToEncode, converted, "").thenReturn(converted))
				.doOnNext(converted -> maybeEmitEvent(new BeforeSaveEvent<>(entityToEncode, converted)));
	}

	@Override
	ReactiveCouchbaseTemplate getReactiveTemplate() {
		return template;
	}

	@Override
	public <T> Mono<T> decodeEntity(Object id, String source, Long cas, Instant expiryTime, Class<T> entityClass,
			String scope, String collection, Object txResultHolder, CouchbaseResourceHolder holder) {
		return Mono
				.fromSupplier(() -> decodeEntityBase(id, source, cas, expiryTime, entityClass, scope, collection,
						txResultHolder, holder));
	}

	@Override
	public <T> Mono<T> applyResult(T entity, CouchbaseDocument converted, Object id, Long cas,
			Object txResultHolder, CouchbaseResourceHolder holder) {
		return Mono.fromSupplier(() -> applyResultBase(entity, converted, id, cas, txResultHolder, holder));
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
		if (reactiveEntityCallbacks == null) {
			setReactiveEntityCallbacks(ReactiveEntityCallbacks.create(applicationContext));
		}
	}

	/**
	 * Set the {@link ReactiveEntityCallbacks} instance to use when invoking
	 * {@link org.springframework.data.mapping.callback.ReactiveEntityCallbacks callbacks} like the
	 * {@link ReactiveBeforeConvertCallback}.
	 * <p>
	 * Overrides potentially existing {@link EntityCallbacks}.
	 *
	 * @param reactiveEntityCallbacks must not be {@literal null}.
	 * @throws IllegalArgumentException if the given instance is {@literal null}.
	 */
	public void setReactiveEntityCallbacks(ReactiveEntityCallbacks reactiveEntityCallbacks) {
		Assert.notNull(reactiveEntityCallbacks, "EntityCallbacks must not be null!");
		this.reactiveEntityCallbacks = reactiveEntityCallbacks;
	}

	protected <T> Mono<T> maybeCallBeforeConvert(T object, String collection) {
		if (reactiveEntityCallbacks != null) {
			return reactiveEntityCallbacks.callback(ReactiveBeforeConvertCallback.class, object, collection);
		} else {
			LOG.info("maybeCallBeforeConvert called, but ReactiveCouchbaseTemplate not initialized with applicationContext");
		}
		return Mono.just(object);
	}

	protected <T> Mono<T> maybeCallAfterConvert(T object, CouchbaseDocument document, String collection) {
		if (null != reactiveEntityCallbacks) {
			return reactiveEntityCallbacks.callback(ReactiveAfterConvertCallback.class, object, document, collection);
		} else {
			LOG.info("maybeCallAfterConvert called, but ReactiveCouchbaseTemplate not initialized with applicationContext");
		}
		return Mono.just(object);
	}

}
