/*
 * Copyright 2020-2025 the original author or authors.
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
package org.springframework.data.couchbase.core.mapping.event;

import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.mapping.callback.EntityCallback;

/**
 * Callback being invoked after a domain object is materialized from a {@link CouchbaseDocument} when reading results.
 *
 * @author Michael Reiche
 * @see org.springframework.data.mapping.callback.EntityCallbacks
 * @since 4.2
 */
@FunctionalInterface
public interface AfterConvertCallback<T> extends EntityCallback<T> {

	/**
	 * Entity callback method invoked after a domain object is materialized from a {@link CouchbaseDocument}. Can return
	 * either the same or a modified instance of the domain object.
	 *
	 * @param entity the domain object (the result of the conversion).
	 * @param document must not be {@literal null}.
	 * @param collection name of the collection.
	 * @return the domain object that is the result of reading it from the {@link CouchbaseDocument}.
	 */
	T onAfterConvert(T entity, CouchbaseDocument document, String collection);
}
