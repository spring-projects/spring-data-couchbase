/*
 * Copyright 2021-2023 the original author or authors.
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

import reactor.core.publisher.Mono;

import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.event.CouchbaseMappingEvent;

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
	public <T> Mono<T> decodeEntity(Object id, String source, Long cas, Class<T> entityClass, String scope,
			String collection) {
		return Mono.fromSupplier(() -> support.decodeEntity(id, source, cas, entityClass, scope, collection));
	}

	@Override
	public Mono<Object> applyUpdatedCas(Object entity, CouchbaseDocument converted, long cas) {
		return Mono.fromSupplier(() -> support.applyUpdatedCas(entity, converted, cas));
	}

	@Override
	public Mono<Object> applyUpdatedId(Object entity, Object id) {
		return Mono.fromSupplier(() -> support.applyUpdatedId(entity, id));
	}

	@Override
	public Long getCas(Object entity) {
		return support.getCas(entity);
	}

	@Override
	public String getJavaNameForEntity(Class<?> clazz) {
		return support.getJavaNameForEntity(clazz);
	}

	@Override
	public void maybeEmitEvent(CouchbaseMappingEvent<?> event) {
		support.maybeEmitEvent(event);
	}
}
