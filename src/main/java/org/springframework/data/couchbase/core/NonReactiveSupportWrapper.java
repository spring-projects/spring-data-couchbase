/*
 * Copyright 2021 the original author or authors.
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

import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;

import reactor.core.publisher.Mono;

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
	public <T> Mono<T> decodeEntity(String id, String source, long cas, Class<T> entityClass) {
		return Mono.fromSupplier(() -> support.decodeEntity(id, source, cas, entityClass));
	}

	@Override
	public Mono<Object> applyUpdatedCas(Object entity, long cas) {
		return Mono.fromSupplier(() -> support.applyUpdatedCas(entity, cas));
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
}
