/*
 * Copyright 2013-2015 the original author or authors.
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

package org.springframework.data.couchbase.repository.support;

import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.repository.core.support.PersistentEntityInformation;

import java.io.Serializable;

/**
 * Entity Information container.
 *
 * @author Michael Nitschinger
 * @author Oliver Gierke
 */
public class MappingCouchbaseEntityInformation<T, ID  extends Serializable>
  extends PersistentEntityInformation<T, ID>
  implements CouchbaseEntityInformation<T, ID> {

  /**
   * Create a new Infomration container.
   *
   * @param entity the entity of the container.
   */
  public MappingCouchbaseEntityInformation(final CouchbasePersistentEntity<T> entity) {
    super(entity);
  }
}
