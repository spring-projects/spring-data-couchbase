/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.mapping;

import org.springframework.data.mapping.PersistentEntity;

/**
 * Represents an entity that can be persisted which contains 0 or more properties.
 *
 * @author Michael Nitschinger
 */
public interface CouchbasePersistentEntity<T> extends
  PersistentEntity<T, CouchbasePersistentProperty> {

  /**
   * Returns the expiry time for the document.
   *
   * @return the expiration time.
   */
	int getExpiry();

    /**
     * Flag for using getAndTouch operations for reads
     * @return
     */
    boolean isTouchOnRead();

}
