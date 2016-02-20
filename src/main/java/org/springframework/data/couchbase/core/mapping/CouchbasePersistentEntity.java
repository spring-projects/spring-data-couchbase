/*
 * Copyright 2012-2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
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
   * The threshold (inclusive) after which expiry should be expressed as a Unix UTC time.
   */
  long TTL_IN_SECONDS_INCLUSIVE_END = 30 * 24 * 60 * 60;

  /**
   * Returns the expiration time of the entity.
   * <p/>
   * The Couchbase format for expiration time is:
   *  - for TTL < 31 days (<= 30 * 24 * 60 * 60): expressed as a TTL in seconds
   *  - for TTL > 30 days: expressed as Unix UTC time of expiry (number of SECONDS since the Epoch)
   *
   * @return the expiration time in correct Couchbase format.
   */
  int getExpiry();

  /**
   * Flag for using getAndTouch operations for reads
   * @return
   */
   boolean isTouchOnRead();

}
