/*
 * Copyright 2021-2022 the original author or authors
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

import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.event.CouchbaseMappingEvent;

/**
 *
 * @author Michael Reiche
 */
public interface TemplateSupport {

	CouchbaseDocument encodeEntity(Object entityToEncode);

	<T> T decodeEntity(String id, String source, long cas, Class<T> entityClass, String scope, String collection);

	<T> T applyUpdatedCas(T entity, CouchbaseDocument converted, long cas);

	<T> T applyUpdatedId(T entity, Object id);

	long getCas(Object entity);

	String getJavaNameForEntity(Class<?> clazz);

	void maybeEmitEvent(CouchbaseMappingEvent<?> event);
}
