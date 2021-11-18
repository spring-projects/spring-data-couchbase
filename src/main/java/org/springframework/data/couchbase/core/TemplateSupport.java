/*
 * Copyright 2021 the original author or authors
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
import org.springframework.data.couchbase.transactions.TransactionResultMap;

import com.couchbase.transactions.TransactionGetResult;

public interface TemplateSupport {

	CouchbaseDocument encodeEntity(Object entityToEncode);

	<T> T decodeEntity(String id, String source, long cas, Class<T> entityClass);

	<T> T decodeEntity(String id, String source, long cas, Class<T> entityClass, TransactionGetResult txResult,
			TransactionResultMap map);

	<T> T applyResult(T entity, CouchbaseDocument converted, Object id, Long cas, TransactionGetResult txResult,
			TransactionResultMap map);

	long getCas(Object entity);

	Object getId(Object entity);

	String getJavaNameForEntity(Class<?> clazz);

	void maybeEmitEvent(CouchbaseMappingEvent<?> event);

	<T> Integer getTxResultKey(Object source);

}
