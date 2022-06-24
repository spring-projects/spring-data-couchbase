/*
 * Copyright 2021-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.core;

import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.transaction.CouchbaseResourceHolder;
import reactor.core.publisher.Mono;

import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.repository.support.TransactionResultHolder;

/**
 * Wrapper of {@link TemplateSupport} methods to adapt them to {@link ReactiveTemplateSupport}.
 *
 * @author Carlos Espinaco
 * @since 4.2
 */
public class NonReactiveSupportWrapper implements ReactiveTemplateSupport {

	private final TemplateSupport support;

	public NonReactiveSupportWrapper(TemplateSupport support) {
		this.support = support;
	}

	@Override
	public Mono<CouchbaseDocument> encodeEntity(Object entityToEncode) {
		return Mono.fromSupplier(() -> support.encodeEntity(entityToEncode));
	}

	@Override
	public <T> Mono<T> decodeEntity(String id, String source, long cas, Class<T> entityClass, String scope, String collection,
									TransactionResultHolder txResultHolder) {
		return decodeEntity(id, source, cas, entityClass, scope, collection, txResultHolder, null);
	}

	@Override
	public <T> Mono<T> decodeEntity(String id, String source, long cas, Class<T> entityClass, String scope, String collection,
									TransactionResultHolder txResultHolder, CouchbaseResourceHolder holder) {
		return Mono.fromSupplier(() -> support.decodeEntity(id, source, cas, entityClass, scope, collection, txResultHolder, holder));
	}

	@Override
	public <T> Mono<T> applyResult(T entity, CouchbaseDocument converted, Object id, Long cas,
								   TransactionResultHolder txResultHolder) {
		return Mono.fromSupplier(() -> support.applyResult(entity, converted, id, cas, txResultHolder));
	}

	@Override
	public <T> Mono<T> applyResult(T entity, CouchbaseDocument converted, Object id, Long cas,
								   TransactionResultHolder txResultHolder, CouchbaseResourceHolder holder) {
		return Mono.fromSupplier(() -> support.applyResult(entity, converted, id, cas, txResultHolder, holder));
	}


	@Override
	public Long getCas(Object entity) {
		return support.getCas(entity);
	}

	@Override
	public Object getId(Object entity) {
		return support.getId(entity);
	}

	@Override
	public String getJavaNameForEntity(Class<?> clazz) {
		return support.getJavaNameForEntity(clazz);
	}

	@Override
	public TranslationService getTranslationService() {
		return support.getTranslationService();
	}
}
