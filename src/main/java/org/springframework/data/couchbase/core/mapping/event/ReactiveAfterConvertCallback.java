/*
 * Copyright 2021-2024 the original author or authors.
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

import org.reactivestreams.Publisher;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.mapping.callback.EntityCallback;

/**
 * Callback being invoked after a domain object is materialized from a document when reading results.
 *
 * @author Jorge Rodríguez Martín
 * @author Mico Piira
 * @see org.springframework.data.mapping.callback.EntityCallbacks
 * @since 4.2
 */
@FunctionalInterface
public interface ReactiveAfterConvertCallback<T> extends EntityCallback<T> {

	/**
	 * Entity callback method invoked after a domain object is materialized from a document.
	 *
	 * @param entity the domain object (the result of the conversion).
	 * @param document must not be null.
	 * @param collection name of the document.
	 * @return the domain object that is the result of reading it from the document.
	 */
	Publisher<T> onAfterConvert(T entity, CouchbaseDocument document, String collection);
}
