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

package org.springframework.data.couchbase.repository.support;

import java.io.Serializable;

import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.mapping.model.BeanWrapper;
import org.springframework.data.repository.core.support.AbstractEntityInformation;

/**
 * Entity Information container.
 *
 * @author Michael Nitschinger
 */
public class MappingCouchbaseEntityInformation<T, ID  extends Serializable>
  extends AbstractEntityInformation<T, ID>
  implements CouchbaseEntityInformation<T, ID> {

  /**
   * Contains the entity metadata.
   */
  private final CouchbasePersistentEntity<T> entityMetadata;

  /**
   * Create a new Infomration container.
   *
   * @param entity the entity of the container.
   */
  public MappingCouchbaseEntityInformation(final CouchbasePersistentEntity<T> entity) {
    super(entity.getType());
    entityMetadata = entity;
  }

  /**
   * Returns the ID of the entity.
   *
   * @param entity the entity from where to extract the ID from.
   * @return the id of the entity.
   */
  @Override
  public ID getId(T entity) {
    CouchbasePersistentProperty idProperty = entityMetadata.getIdProperty();

    if (idProperty == null) {
      return null;
    }

    try {
      return (ID) BeanWrapper.create(entity, null).getProperty(idProperty);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the ID type.
   *
   * @return the ID type.
   */
  @Override
  public Class<ID> getIdType() {
    return (Class<ID>) entityMetadata.getIdProperty().getType();
  }
}
